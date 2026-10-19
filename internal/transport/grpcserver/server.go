// Package grpcserver contains the gRPC transport layer of the notification
// service: validation of untrusted input and mapping of domain errors.
package grpcserver

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	notificationv1 "github.com/cbl/cbl-protos/gen/go/billing/notification/v1"

	"github.com/cbl/cbl-kit/grpcmw"

	"github.com/cbl/cbl-notification/internal/domain"
	"github.com/cbl/cbl-notification/internal/usecase"
)

// Server implements the NotificationService gRPC service (CBL-8.1).
type Server struct {
	notificationv1.UnimplementedNotificationServiceServer
	register *usecase.RegisterService
	log      *zap.Logger
}

// NewServer creates the NotificationService server.
func NewServer(register *usecase.RegisterService, log *zap.Logger) *Server {
	if log == nil {
		log = zap.NewNop()
	}
	return &Server{register: register, log: log}
}

// Register registers the service on the gRPC server.
func (s *Server) Register(server *grpc.Server) {
	notificationv1.RegisterNotificationServiceServer(server, s)
}

// RegisterWebhookEndpoint validates the untrusted request and registers the
// endpoint; the response carries the signing secret (shown once). The raw
// failure is logged with the correlation context, the response stays
// sanitized (spec 3.5, NFR 9).
func (s *Server) RegisterWebhookEndpoint(ctx context.Context, req *notificationv1.RegisterWebhookEndpointRequest) (*notificationv1.RegisterWebhookEndpointResponse, error) {
	request := domain.RegisterRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		MerchantID:     req.GetMerchantId(),
		URL:            req.GetUrl(),
		Events:         req.GetEvents(),
	}
	if err := validateMerchantMetadata(ctx, request.MerchantID); err != nil {
		return nil, grpcmw.MapError(err)
	}
	result, err := s.register.Register(ctx, request)
	if err != nil {
		s.log.Error("webhook endpoint registration failed",
			zap.String("request_id", grpcmw.RequestID(ctx)),
			zap.String("merchant_id", request.MerchantID),
			zap.Error(err))
		return nil, grpcmw.MapError(err)
	}
	s.log.Info("webhook endpoint registered",
		zap.String("request_id", grpcmw.RequestID(ctx)),
		zap.String("merchant_id", request.MerchantID),
		zap.String("endpoint_id", result.EndpointID),
		zap.Bool("replayed", result.Replayed))
	return &notificationv1.RegisterWebhookEndpointResponse{
		EndpointId:    result.EndpointID,
		Url:           result.URL,
		Events:        result.Events,
		SigningSecret: result.SigningSecret,
	}, nil
}

// validateMerchantMetadata rejects a request whose x-merchant-id metadata
// differs from the payload (defense in depth; the mTLS perimeter and the
// operator authZ are 0.5/0.6, KI-notification-02). An absent metadata element
// is tolerated on the stand.
func validateMerchantMetadata(ctx context.Context, merchantID string) error {
	metadataMerchant, err := grpcmw.MerchantID(ctx)
	if err != nil {
		return nil
	}
	if metadataMerchant != merchantID {
		return grpcmw.E(grpcmw.KindUnauthenticated, "merchant_mismatch",
			"the merchant identity does not match the request", nil)
	}
	return nil
}
