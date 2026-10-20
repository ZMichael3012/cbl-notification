// Package grpcserver contains the gRPC transport layer of the notification
// service: validation of untrusted input and mapping of domain errors.
package grpcserver

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	notificationv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/notification/v1"

	"github.com/ZMichael3012/cbl-kit/grpcmw"

	"github.com/cbl/cbl-notification/internal/domain"
	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/usecase"
)

// AdminPort reads and resets the delivery journal (CBL-8.3).
type AdminPort interface {
	// List returns one merchant journal page.
	List(ctx context.Context, query usecase.ListQuery) (usecase.DeliveryPage, error)
	// Stats returns the merchant journal aggregates.
	Stats(ctx context.Context, merchantID string, from, to *time.Time) (usecase.DeliveryStats, error)
	// Resend resets a failed delivery for a new schedule round.
	Resend(ctx context.Context, deliveryID, idempotencyKey string) (usecase.ResendResult, error)
}

// Server implements the NotificationService gRPC service (CBL-8.1, CBL-8.3).
type Server struct {
	notificationv1.UnimplementedNotificationServiceServer
	register *usecase.RegisterService
	admin    AdminPort
	log      *zap.Logger
}

// NewServer creates the NotificationService server.
func NewServer(register *usecase.RegisterService, admin AdminPort, log *zap.Logger) *Server {
	if log == nil {
		log = zap.NewNop()
	}
	return &Server{register: register, admin: admin, log: log}
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

// ListWebhookDeliveries returns one merchant journal page (CBL-8.3).
func (s *Server) ListWebhookDeliveries(ctx context.Context, req *notificationv1.ListWebhookDeliveriesRequest) (*notificationv1.ListWebhookDeliveriesResponse, error) {
	if s.admin == nil {
		return nil, status.Error(codes.Unimplemented, "the delivery reads are not configured")
	}
	from, err := protoTime(req.GetFrom())
	if err != nil {
		return nil, grpcmw.MapError(err)
	}
	to, err := protoTime(req.GetTo())
	if err != nil {
		return nil, grpcmw.MapError(err)
	}
	query := usecase.ListQuery{
		MerchantID: req.GetMerchantId(),
		EndpointID: req.GetEndpointId(),
		Status:     req.GetStatus(),
		EventType:  req.GetEventType(),
		From:       from,
		To:         to,
		PageSize:   int(req.GetPageSize()),
		PageToken:  req.GetPageToken(),
	}
	if err := validateMerchantMetadata(ctx, query.MerchantID); err != nil {
		return nil, grpcmw.MapError(err)
	}
	page, err := s.admin.List(ctx, query)
	if err != nil {
		s.log.Error("delivery listing failed",
			zap.String("request_id", grpcmw.RequestID(ctx)),
			zap.String("merchant_id", query.MerchantID),
			zap.Error(err))
		return nil, grpcmw.MapError(err)
	}
	response := &notificationv1.ListWebhookDeliveriesResponse{NextPageToken: page.NextToken}
	for _, row := range page.Deliveries {
		response.Deliveries = append(response.Deliveries, deliveryMessage(row))
	}
	return response, nil
}

// GetDeliveryStats returns the merchant journal aggregates (CBL-8.3).
func (s *Server) GetDeliveryStats(ctx context.Context, req *notificationv1.GetDeliveryStatsRequest) (*notificationv1.GetDeliveryStatsResponse, error) {
	if s.admin == nil {
		return nil, status.Error(codes.Unimplemented, "the delivery reads are not configured")
	}
	from, err := protoTime(req.GetFrom())
	if err != nil {
		return nil, grpcmw.MapError(err)
	}
	to, err := protoTime(req.GetTo())
	if err != nil {
		return nil, grpcmw.MapError(err)
	}
	if err := validateMerchantMetadata(ctx, req.GetMerchantId()); err != nil {
		return nil, grpcmw.MapError(err)
	}
	stats, err := s.admin.Stats(ctx, req.GetMerchantId(), from, to)
	if err != nil {
		s.log.Error("delivery statistics failed",
			zap.String("request_id", grpcmw.RequestID(ctx)),
			zap.String("merchant_id", req.GetMerchantId()),
			zap.Error(err))
		return nil, grpcmw.MapError(err)
	}
	return &notificationv1.GetDeliveryStatsResponse{
		Total:       stats.Total,
		Pending:     stats.Pending,
		Delivered:   stats.Delivered,
		Failed:      stats.Failed,
		SuccessRate: stats.SuccessRate,
	}, nil
}

// ResendWebhookDelivery resets a failed delivery for a new schedule round.
func (s *Server) ResendWebhookDelivery(ctx context.Context, req *notificationv1.ResendWebhookDeliveryRequest) (*notificationv1.ResendWebhookDeliveryResponse, error) {
	if s.admin == nil {
		return nil, status.Error(codes.Unimplemented, "the delivery resend is not configured")
	}
	result, err := s.admin.Resend(ctx, req.GetDeliveryId(), req.GetIdempotencyKey())
	if err != nil {
		s.log.Error("delivery resend failed",
			zap.String("request_id", grpcmw.RequestID(ctx)),
			zap.String("delivery_id", req.GetDeliveryId()),
			zap.Error(err))
		return nil, grpcmw.MapError(err)
	}
	s.log.Info("delivery resend accepted",
		zap.String("request_id", grpcmw.RequestID(ctx)),
		zap.String("delivery_id", result.Delivery.ID),
		zap.String("status", result.Delivery.Status))
	return &notificationv1.ResendWebhookDeliveryResponse{
		DeliveryId:    result.Delivery.ID,
		Status:        result.Delivery.Status,
		Attempts:      int32(result.Delivery.Attempts),
		NextAttemptAt: optionalTime(result.Delivery.NextAttemptAt),
	}, nil
}

// deliveryMessage maps a journal row; the signed body is represented by its
// digest only (T-NOTIFY-21).
func deliveryMessage(row storage.DeliveryRecord) *notificationv1.WebhookDelivery {
	return &notificationv1.WebhookDelivery{
		DeliveryId:    row.ID,
		EventId:       row.EventID,
		EndpointId:    row.EndpointID,
		EventType:     row.EventType,
		Status:        row.Status,
		Attempts:      int32(row.Attempts),
		NextAttemptAt: optionalTime(row.NextAttemptAt),
		LastError:     row.LastError,
		CreatedAt:     optionalTime(&row.CreatedAt),
		DeliveredAt:   optionalTime(row.DeliveredAt),
		BodySha256:    usecase.BodySHA256(row.Body),
	}
}

// protoTime validates an optional timestamp (untrusted input).
func protoTime(ts *timestamppb.Timestamp) (*time.Time, error) {
	if ts == nil {
		return nil, nil
	}
	if err := ts.CheckValid(); err != nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_request", "the window bound is not a valid timestamp", nil)
	}
	at := ts.AsTime().UTC()
	return &at, nil
}

// optionalTime renders an optional timestamp.
func optionalTime(at *time.Time) *timestamppb.Timestamp {
	if at == nil || at.IsZero() {
		return nil
	}
	return timestamppb.New(at.UTC())
}
