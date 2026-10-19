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
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/cbl/cbl-kit/grpcmw"
	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
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
	defer closeDatabase(db, log)
	secretKey, err := loadSecretKey()
	if err != nil {
		log.Error("secret key startup failed", zap.Error(err))
		os.Exit(1)
	}

	endpoints := storage.NewEndpointRepository(db)
	register := usecase.NewRegisterService(endpoints, secretKey)
	admin := usecase.NewAdminService(storage.NewDeliveryRepository(db))

	closePipeline, err := startDeliveryPipeline(ctx, log, db, endpoints, secretKey)
	if err != nil {
		log.Error("delivery pipeline startup failed", zap.Error(err))
		os.Exit(1)
	}
	defer closePipeline()

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
	grpcserver.NewServer(register, admin, log).Register(grpcSrv)

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

// webhookStore combines the endpoint and delivery repositories into the
// delivery pipeline persistence port.
type webhookStore struct {
	*storage.EndpointRepository
	*storage.DeliveryRepository
}

// EndpointByID resolves the delivery target (the endpoint repository method).
func (s webhookStore) EndpointByID(ctx context.Context, id string) (storage.EndpointRecord, error) {
	return s.ByID(ctx, id)
}

// startDeliveryPipeline wires the Kafka consumers and the dispatcher; without
// KAFKA_BROKERS the pipeline stays disabled (registration only), FR-6. The
// returned closer stops the consumers and the publisher before the caller
// closes the database.
func startDeliveryPipeline(ctx context.Context, log *zap.Logger, db *gorm.DB, endpoints *storage.EndpointRepository, secretKey []byte) (func(), error) {
	brokers := splitCSV(getenv("KAFKA_BROKERS", ""))
	if len(brokers) == 0 {
		log.Warn("kafka is not configured: the delivery pipeline is disabled")
		return func() {}, nil
	}
	publisher, err := kafkapub.New(brokers)
	if err != nil {
		return nil, fmt.Errorf("kafka publisher: %w", err)
	}
	runners := make([]*kafkaconsumer.Runner, 0, 3)
	closeAll := func() {
		for _, runner := range runners {
			runner.Close()
		}
		publisher.Close()
	}
	store := webhookStore{EndpointRepository: endpoints, DeliveryRepository: storage.NewDeliveryRepository(db)}
	schedule, err := usecase.ParseRetrySchedule(os.Getenv("NOTIFICATION_RETRY_SCHEDULE"))
	if err != nil {
		return nil, fmt.Errorf("retry schedule: %w", err)
	}
	dispatcher := usecase.NewDispatcher(store, secretKey, log,
		usecase.WithDispatcherInterval(parseDuration(getenv("NOTIFICATION_DISPATCH_INTERVAL", "1s"), time.Second)),
		usecase.WithDispatcherAttemptTimeout(parseDuration(getenv("NOTIFICATION_ATTEMPT_TIMEOUT", "10s"), 10*time.Second)),
		usecase.WithDispatcherSchedule(schedule))
	go func() {
		if runErr := dispatcher.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
			log.Error("delivery dispatcher stopped", zap.Error(runErr))
		}
	}()

	consumers := []struct {
		kind  usecase.HandlerKind
		topic string
		group string
	}{
		{usecase.HandlerPayments, "payment.events", getenv("NOTIFICATION_PAYMENTS_GROUP", "cbl-notification-payments")},
		{usecase.HandlerPayouts, "payout.events", getenv("NOTIFICATION_PAYOUTS_GROUP", "cbl-notification-payouts")},
		{usecase.HandlerRecon, "recon.alerts", getenv("NOTIFICATION_RECON_GROUP", "cbl-notification-recon")},
	}
	for _, consumer := range consumers {
		handler := usecase.NewEventHandler(consumer.kind, store, log)
		processor := inbox.NewProcessor(consumer.group, db, inbox.NewGuard(), handler,
			publisher, inbox.NewRouter(consumer.topic, 3), log)
		runner, runErr := kafkaconsumer.New(brokers, consumer.group, topicWithRetries(consumer.topic), processor, log,
			kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
		if runErr != nil {
			closeAll()
			return nil, fmt.Errorf("consumer %s: %w", consumer.topic, runErr)
		}
		runners = append(runners, runner)
		go func(topic string, runner *kafkaconsumer.Runner) {
			if runErr := runner.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				log.Error("delivery consumer stopped", zap.String("topic", topic), zap.Error(runErr))
			}
		}(consumer.topic, runner)
	}
	return closeAll, nil
}

// closeDatabase closes the SQL pool at shutdown.
func closeDatabase(db *gorm.DB, log *zap.Logger) {
	if sqlDB, err := db.DB(); err == nil {
		if closeErr := sqlDB.Close(); closeErr != nil {
			log.Error("database close failed", zap.Error(closeErr))
		}
	}
}

// topicWithRetries returns the topic and its inbox retry topics.
func topicWithRetries(main string) []string {
	return []string{
		main,
		main + inbox.RetrySuffix5s,
		main + inbox.RetrySuffix1m,
		main + inbox.RetrySuffix10m,
	}
}

// splitCSV splits a comma-separated list skipping empty elements.
func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// parseDuration parses a duration with the fallback.
func parseDuration(raw string, fallback time.Duration) time.Duration {
	parsed, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
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
