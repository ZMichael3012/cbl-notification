package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ZMichael3012/cbl-kit/grpcmw"

	"github.com/cbl/cbl-notification/internal/domain"
	"github.com/cbl/cbl-notification/internal/storage"
)

// Journal window bounds of the operator reads (CBL-8.3).
const (
	// DefaultStatsWindow sets the default statistics window.
	DefaultStatsWindow = 24 * time.Hour
	// MaxJournalWindow bounds the list and statistics windows.
	MaxJournalWindow = 90 * 24 * time.Hour
	// DefaultPageSize sets the default listing page size.
	DefaultPageSize = 20
	// MaxPageSize bounds the listing page size.
	MaxPageSize = 100
)

// DeliveryStore reads and resets the delivery journal (CBL-8.3).
type DeliveryStore interface {
	// ListDeliveries returns the filtered journal rows.
	ListDeliveries(ctx context.Context, query storage.ListQuery) ([]storage.DeliveryRecord, error)
	// DeliveryByID returns one journal row.
	DeliveryByID(ctx context.Context, deliveryID string) (storage.DeliveryRecord, error)
	// DeliveryStats counts the journal rows by status in the window.
	DeliveryStats(ctx context.Context, merchantID string, from, to time.Time) (storage.DeliveryStats, error)
	// ResendFailed resets a failed delivery for a new schedule round.
	ResendFailed(ctx context.Context, deliveryID string, now time.Time) (storage.DeliveryRecord, error)
}

// AdminService exposes the delivery journal to the operator surface (CBL-8.3).
type AdminService struct {
	store DeliveryStore
	now   func() time.Time
}

// AdminOption configures the admin service.
type AdminOption func(*AdminService)

// WithAdminClock sets the time source.
func WithAdminClock(clock func() time.Time) AdminOption {
	return func(s *AdminService) {
		if clock != nil {
			s.now = clock
		}
	}
}

// NewAdminService creates the operator delivery service.
func NewAdminService(store DeliveryStore, opts ...AdminOption) *AdminService {
	service := &AdminService{store: store, now: time.Now}
	for _, opt := range opts {
		opt(service)
	}
	return service
}

// ListQuery filters the delivery journal listing.
type ListQuery struct {
	// MerchantID scopes the listing.
	MerchantID string
	// EndpointID filters by endpoint.
	EndpointID string
	// Status filters by pending, delivered or failed.
	Status string
	// EventType filters by the CF-3 type.
	EventType string
	// From filters created_at >= from.
	From *time.Time
	// To filters created_at < to.
	To *time.Time
	// PageSize sets 1..100, the default is 20.
	PageSize int
	// PageToken sets the opaque keyset cursor.
	PageToken string
}

// DeliveryPage is one listing page.
type DeliveryPage struct {
	// Deliveries sets the page rows.
	Deliveries []storage.DeliveryRecord
	// NextToken sets the cursor of the next page; empty at the end.
	NextToken string
}

// DeliveryStats is the journal aggregate of a window.
type DeliveryStats struct {
	// Total sets all deliveries in the window.
	Total int64
	// Pending sets the pending deliveries.
	Pending int64
	// Delivered sets the delivered deliveries.
	Delivered int64
	// Failed sets the failed deliveries.
	Failed int64
	// SuccessRate sets delivered / (delivered + failed), 0 without terminal rows.
	SuccessRate float64
}

// ResendResult is the delivery state after the reset.
type ResendResult struct {
	// Delivery sets the current journal row.
	Delivery storage.DeliveryRecord
}

// DeliveryCursor is the decoded keyset cursor.
type DeliveryCursor struct {
	// CreatedAt sets the creation time of the last row.
	CreatedAt time.Time
	// ID sets the delivery id of the last row.
	ID string
}

// EncodeDeliveryCursor renders the opaque page token of a row.
func EncodeDeliveryCursor(cursor DeliveryCursor) string {
	raw := cursor.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + cursor.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeDeliveryCursor parses an opaque page token.
func DecodeDeliveryCursor(token string) (DeliveryCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return DeliveryCursor{}, invalidRequest("the page token is not a valid cursor")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return DeliveryCursor{}, invalidRequest("the page token is not a valid cursor")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return DeliveryCursor{}, invalidRequest("the page token is not a valid cursor")
	}
	if _, err := uuid.Parse(parts[1]); err != nil {
		return DeliveryCursor{}, invalidRequest("the page token is not a valid cursor")
	}
	return DeliveryCursor{CreatedAt: createdAt, ID: parts[1]}, nil
}

// List returns one page of the merchant journal (created_at DESC).
func (s *AdminService) List(ctx context.Context, query ListQuery) (DeliveryPage, error) {
	if query.MerchantID == "" {
		return DeliveryPage{}, invalidRequest("merchant_id is required")
	}
	if query.Status != "" && query.Status != storage.DeliveryPending &&
		query.Status != storage.DeliveryDelivered && query.Status != storage.DeliveryFailed {
		return DeliveryPage{}, invalidRequest("status must be pending, delivered or failed")
	}
	if query.EventType != "" && !domain.KnownEvent(query.EventType) {
		return DeliveryPage{}, invalidRequest("event_type is not a delivery catalog type")
	}
	pageSize := query.PageSize
	if pageSize == 0 {
		pageSize = DefaultPageSize
	}
	if pageSize < 1 || pageSize > MaxPageSize {
		return DeliveryPage{}, invalidRequest("page_size must be within 1..100")
	}
	if err := validateWindow(query.From, query.To, false); err != nil {
		return DeliveryPage{}, err
	}
	listQuery := storage.ListQuery{
		MerchantID: query.MerchantID,
		EndpointID: query.EndpointID,
		Status:     query.Status,
		EventType:  query.EventType,
		From:       normalizeTime(query.From),
		To:         normalizeTime(query.To),
		Limit:      pageSize + 1,
	}
	if query.PageToken != "" {
		cursor, err := DecodeDeliveryCursor(query.PageToken)
		if err != nil {
			return DeliveryPage{}, err
		}
		listQuery.CursorCreatedAt = &cursor.CreatedAt
		listQuery.CursorID = cursor.ID
	}
	records, err := s.store.ListDeliveries(ctx, listQuery)
	if err != nil {
		return DeliveryPage{}, err
	}
	page := DeliveryPage{Deliveries: records}
	if len(records) > pageSize {
		page.Deliveries = records[:pageSize]
		last := page.Deliveries[len(page.Deliveries)-1]
		page.NextToken = EncodeDeliveryCursor(DeliveryCursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	return page, nil
}

// Stats returns the merchant journal aggregates of the window.
func (s *AdminService) Stats(ctx context.Context, merchantID string, from, to *time.Time) (DeliveryStats, error) {
	if merchantID == "" {
		return DeliveryStats{}, invalidRequest("merchant_id is required")
	}
	now := s.now().UTC()
	resolvedTo := now
	if to != nil {
		resolvedTo = to.UTC()
	}
	resolvedFrom := resolvedTo.Add(-DefaultStatsWindow)
	if from != nil {
		resolvedFrom = from.UTC()
	}
	if err := validateWindow(&resolvedFrom, &resolvedTo, true); err != nil {
		return DeliveryStats{}, err
	}
	raw, err := s.store.DeliveryStats(ctx, merchantID, resolvedFrom, resolvedTo)
	if err != nil {
		return DeliveryStats{}, err
	}
	stats := DeliveryStats{
		Total:     raw.Pending + raw.Delivered + raw.Failed,
		Pending:   raw.Pending,
		Delivered: raw.Delivered,
		Failed:    raw.Failed,
	}
	if terminal := raw.Delivered + raw.Failed; terminal > 0 {
		stats.SuccessRate = float64(raw.Delivered) / float64(terminal)
	}
	return stats, nil
}

// Resend resets a failed delivery for a new schedule round (CBL-8.3).
func (s *AdminService) Resend(ctx context.Context, deliveryID, idempotencyKey string) (ResendResult, error) {
	if deliveryID == "" {
		return ResendResult{}, invalidRequest("delivery_id is required")
	}
	if idempotencyKey == "" {
		return ResendResult{}, invalidRequest("idempotency_key is required")
	}
	record, err := s.store.DeliveryByID(ctx, deliveryID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return ResendResult{}, grpcmw.E(grpcmw.KindNotFound, "delivery_not_found",
				"the delivery does not exist", nil)
		}
		return ResendResult{}, err
	}
	switch record.Status {
	case storage.DeliveryFailed:
		reset, err := s.store.ResendFailed(ctx, deliveryID, s.now().UTC())
		if err == nil {
			return ResendResult{Delivery: reset}, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return ResendResult{}, err
		}
		// A concurrent resend won the conditional update: return its state.
		current, readErr := s.store.DeliveryByID(ctx, deliveryID)
		if readErr != nil {
			return ResendResult{}, readErr
		}
		if current.Status != storage.DeliveryPending {
			return ResendResult{}, grpcmw.E(grpcmw.KindFailedPrecondition, "delivery_not_resendable",
				"the delivery is not in the failed state", nil)
		}
		return ResendResult{Delivery: current}, nil
	case storage.DeliveryPending:
		return ResendResult{Delivery: record}, nil
	default:
		return ResendResult{}, grpcmw.E(grpcmw.KindFailedPrecondition, "delivery_not_resendable",
			"a delivered webhook is not resent automatically", nil)
	}
}

// ParseRetrySchedule parses NOTIFICATION_RETRY_SCHEDULE; an empty value keeps
// the contract schedule (stands and the e2e only, T-NOTIFY-20).
func ParseRetrySchedule(raw string) ([]time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return append([]time.Duration(nil), RetrySchedule...), nil
	}
	parts := strings.Split(raw, ",")
	if len(parts) == 0 || len(parts) > 20 {
		return nil, invalidRequest("the retry schedule must have 1..20 entries")
	}
	schedule := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		delay, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil || delay <= 0 {
			return nil, invalidRequest("the retry schedule entries must be positive durations")
		}
		if len(schedule) > 0 && delay <= schedule[len(schedule)-1] {
			return nil, invalidRequest("the retry schedule must be strictly ascending")
		}
		schedule = append(schedule, delay)
	}
	return schedule, nil
}

// BodySHA256 renders the hex digest of the signed body (the listing identity).
func BodySHA256(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// validateWindow checks the list and statistics window rules.
func validateWindow(from, to *time.Time, required bool) error {
	if from == nil || to == nil {
		if required {
			return invalidRequest("the window bounds are required")
		}
		return nil
	}
	if !from.Before(*to) {
		return invalidRequest("from must be before to")
	}
	if to.Sub(*from) > MaxJournalWindow {
		return invalidRequest("the window must not exceed 90 days")
	}
	return nil
}

// normalizeTime returns an UTC copy of the optional time.
func normalizeTime(at *time.Time) *time.Time {
	if at == nil {
		return nil
	}
	utc := at.UTC()
	return &utc
}

// invalidRequest builds the standard invalid argument error.
func invalidRequest(message string) error {
	return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_request", message, nil)
}
