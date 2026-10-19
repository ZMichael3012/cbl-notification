// main starts the HTTP and gRPC service of the CBL notification platform.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/cbl/cbl-kit/grpcmw"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-notification/internal/domain"
	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/transport/grpcserver"
	"github.com/cbl/cbl-notification/internal/usecase"
)

// main starts the gRPC server of the service, the administrative HTTP server and the metrics server.
func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	httpAddr := getenv("HTTP_ADDR", ":8090")
	grpcAddr := getenv("GRPC_ADDR", ":9090")
	metricsAddr := getenv("METRICS_ADDR", ":9290")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := openDatabase(ctx, log)
	if err != nil {
		log.Error("database startup failed", zap.Error(err))
		os.Exit(1)
	}
	secretKey, err := loadSecretKey()
	if err != nil {
		log.Error("secret key startup failed", zap.Error(err))
		os.Exit(1)
	}

	endpoints := storage.NewEndpointRepository(db)
	register := usecase.NewRegisterService(endpoints, secretKey)

	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", func(c *gin.Context) {
		pingCtx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if err := pingDatabase(pingCtx, db); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	httpSrv := &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsHandler(), ReadHeaderTimeout: 5 * time.Second}

	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(grpcmw.UnaryServer(log)),
	)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	reflection.Register(grpcSrv)
	grpcserver.NewServer(register, log).Register(grpcSrv)

	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error("grpc listen failed", zap.String("addr", grpcAddr), zap.Error(err))
		os.Exit(1)
	}

	go func() {
		log.Info("http server started", zap.String("addr", httpAddr))
		if serveErr := httpSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("metrics server started", zap.String("addr", metricsAddr))
		if serveErr := metricsSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("metrics server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("grpc server started", zap.String("addr", grpcAddr))
		if serveErr := grpcSrv.Serve(grpcListener); serveErr != nil {
			log.Error("grpc server failed", zap.Error(serveErr))
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown started")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("http shutdown failed", zap.Error(shutdownErr))
	}
	if shutdownErr := metricsSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("metrics shutdown failed", zap.Error(shutdownErr))
	}
	grpcSrv.GracefulStop()
	if sqlDB, sqlErr := db.DB(); sqlErr == nil {
		_ = sqlDB.Close()
	}
	log.Info("shutdown finished")
}

// openDatabase opens the PostgreSQL connection and applies the migrations.
func openDatabase(ctx context.Context, log *zap.Logger) (*gorm.DB, error) {
	dsn := getenv("PG_DSN", "")
	if dsn == "" {
		return nil, errors.New("PG_DSN is required")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	migrationsDir := getenv("MIGRATIONS_DIR", "./migrations")
	if err := storagex.Migrate(ctx, db, migrationsDir); err != nil {
		return nil, fmt.Errorf("apply migrations: %w", err)
	}
	log.Info("database ready", zap.String("migrations_dir", migrationsDir))
	return db, nil
}

// loadSecretKey reads the AES-256 secret encryption key (64 hex characters);
// the decoder error is not wrapped so no key material reaches the log.
func loadSecretKey() ([]byte, error) {
	raw := getenv("NOTIFICATION_SECRET_KEY", "")
	if raw == "" {
		return nil, errors.New("NOTIFICATION_SECRET_KEY is required")
	}
	key, err := hex.DecodeString(raw)
	if err != nil || len(key) != domain.EncryptionKeyLen {
		return nil, errors.New("NOTIFICATION_SECRET_KEY must be 64 hex characters")
	}
	return key, nil
}

// pingDatabase checks the database readiness.
func pingDatabase(ctx context.Context, db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// metricsHandler serves a metrics stub until task 0.7.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# metrics endpoint placeholder (task 0.7)")
	})
	return mux
}

// getenv returns the environment value or the fallback.
func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
