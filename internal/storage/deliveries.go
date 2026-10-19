package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Delivery statuses of the merchant webhook journal.
const (
	// DeliveryPending means the delivery awaits (or retries) the send.
	DeliveryPending = "pending"
	// DeliveryDelivered means a 2xx answer was received.
	DeliveryDelivered = "delivered"
	// DeliveryFailed means the retry schedule is exhausted.
	DeliveryFailed = "failed"
)

// DeliveryRecord describes a merchant webhook delivery row.
type DeliveryRecord struct {
	// ID sets the delivery identifier (UUIDv7).
	ID string `gorm:"column:id;primaryKey"`
	// EventID sets the evt_<uuidv7> merchant event identifier.
	EventID string `gorm:"column:event_id"`
	// SourceEventID sets the platform event identifier the delivery was
	// derived from; (source_event_id, endpoint_id) deduplicates replays.
	SourceEventID string `gorm:"column:source_event_id"`
	// MerchantID sets the owning merchant.
	MerchantID string `gorm:"column:merchant_id"`
	// EndpointID sets the target endpoint.
	EndpointID string `gorm:"column:endpoint_id"`
	// EventType sets the CF-3 event type.
	EventType string `gorm:"column:event_type"`
	// Body sets the exact signed JSON body.
	Body []byte `gorm:"column:body"`
	// Status sets the delivery status (pending, delivered, failed).
	Status string `gorm:"column:status"`
	// Attempts sets the total performed attempts across resends.
	Attempts int `gorm:"column:attempts"`
	// RetryAttempts sets the position in the retry schedule round (reset by a
	// manual resend, CBL-8.3).
	RetryAttempts int `gorm:"column:retry_attempts"`
	// NextAttemptAt sets the due time of the next attempt.
	NextAttemptAt *time.Time `gorm:"column:next_attempt_at"`
	// LastError sets the bounded last failure text.
	LastError string `gorm:"column:last_error"`
	// CreatedAt sets the creation time.
	CreatedAt time.Time `gorm:"column:created_at"`
	// DeliveredAt sets the success time.
	DeliveredAt *time.Time `gorm:"column:delivered_at"`
}

// TableName sets the table of the record.
func (DeliveryRecord) TableName() string { return "webhook_deliveries" }

// AttemptRecord describes one delivery attempt.
type AttemptRecord struct {
	// ID sets the attempt identifier.
	ID int64 `gorm:"column:id;primaryKey"`
	// DeliveryID sets the delivery.
	DeliveryID string `gorm:"column:delivery_id"`
	// Attempt sets the attempt number (1-based).
	Attempt int `gorm:"column:attempt"`
	// SentAt sets the attempt time.
	SentAt time.Time `gorm:"column:sent_at"`
	// HTTPStatus sets the response code (0 for a transport error).
	HTTPStatus int `gorm:"column:http_status"`
	// Error sets the bounded failure text.
	Error string `gorm:"column:error"`
}

// TableName sets the table of the record.
func (AttemptRecord) TableName() string { return "webhook_delivery_attempts" }

// DeliveryRepository stores the merchant delivery journal.
type DeliveryRepository struct {
	db *gorm.DB
}

// NewDeliveryRepository creates the repository.
func NewDeliveryRepository(db *gorm.DB) *DeliveryRepository {
	return &DeliveryRepository{db: db}
}

// InsertDelivery stores a delivery row (in the caller transaction); a replay
// of the same source event for the same endpoint is skipped by the unique
// constraint (T-NOTIFY-16).
func (r *DeliveryRepository) InsertDelivery(ctx context.Context, tx *gorm.DB, record DeliveryRecord) error {
	target := tx
	if target == nil {
		target = r.db
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.Status == "" {
		record.Status = DeliveryPending
	}
	err := target.WithContext(ctx).
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "source_event_id"}, {Name: "endpoint_id"}}, DoNothing: true}).
		Create(&record).Error
	if err != nil {
		return fmt.Errorf("storage: insert delivery: %w", err)
	}
	return nil
}

// ClaimDue atomically claims the due pending deliveries for one dispatcher:
// the due rows are leased (next_attempt_at moves forward) so a restart or a
// parallel instance cannot claim them before the lease expires.
func (r *DeliveryRepository) ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]DeliveryRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	leaseUntil := now.Add(lease)
	var records []DeliveryRecord
	err := r.db.WithContext(ctx).Raw(`
		UPDATE webhook_deliveries SET next_attempt_at = ?
		WHERE id IN (
			SELECT id FROM webhook_deliveries
			WHERE status = 'pending' AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?
			ORDER BY next_attempt_at
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, event_id, merchant_id, endpoint_id, event_type, body, status,
			attempts, retry_attempts, next_attempt_at, last_error, created_at, delivered_at`,
		leaseUntil, now, limit,
	).Scan(&records).Error
	if err != nil {
		return nil, fmt.Errorf("storage: claim deliveries: %w", err)
	}
	return records, nil
}

// RecordAttempt journals an attempt; a duplicate attempt number (a crash
// between the send and the journal write) is ignored.
func (r *DeliveryRepository) RecordAttempt(ctx context.Context, attempt AttemptRecord) error {
	err := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "delivery_id"}, {Name: "attempt"}}, DoNothing: true}).
		Create(&attempt).Error
	if err != nil {
		return fmt.Errorf("storage: record attempt: %w", err)
	}
	return nil
}

// MarkDelivered marks the delivery delivered.
func (r *DeliveryRepository) MarkDelivered(ctx context.Context, deliveryID string, attempts, retryAttempts int) error {
	err := r.db.WithContext(ctx).Model(&DeliveryRecord{}).
		Where("id = ? AND status = ?", deliveryID, DeliveryPending).
		Updates(map[string]any{
			"status":         DeliveryDelivered,
			"attempts":       attempts,
			"retry_attempts": retryAttempts,
			"delivered_at":   time.Now().UTC(),
			"last_error":     "",
		}).Error
	if err != nil {
		return fmt.Errorf("storage: mark delivered: %w", err)
	}
	return nil
}

// ScheduleRetry schedules the next attempt.
func (r *DeliveryRepository) ScheduleRetry(ctx context.Context, deliveryID string, attempts, retryAttempts int, next time.Time, lastError string) error {
	err := r.db.WithContext(ctx).Model(&DeliveryRecord{}).
		Where("id = ? AND status = ?", deliveryID, DeliveryPending).
		Updates(map[string]any{
			"status":          DeliveryPending,
			"attempts":        attempts,
			"retry_attempts":  retryAttempts,
			"next_attempt_at": next.UTC(),
			"last_error":      lastError,
		}).Error
	if err != nil {
		return fmt.Errorf("storage: schedule retry: %w", err)
	}
	return nil
}

// MarkFailed marks the delivery failed after the exhausted schedule.
func (r *DeliveryRepository) MarkFailed(ctx context.Context, deliveryID string, attempts, retryAttempts int, lastError string) error {
	err := r.db.WithContext(ctx).Model(&DeliveryRecord{}).
		Where("id = ? AND status = ?", deliveryID, DeliveryPending).
		Updates(map[string]any{
			"status":         DeliveryFailed,
			"attempts":       attempts,
			"retry_attempts": retryAttempts,
			"last_error":     lastError,
		}).Error
	if err != nil {
		return fmt.Errorf("storage: mark failed: %w", err)
	}
	return nil
}

// ListQuery filters the delivery journal listing (CBL-8.3).
type ListQuery struct {
	// MerchantID scopes the listing (required by the service).
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
	// CursorCreatedAt sets the keyset cursor creation time.
	CursorCreatedAt *time.Time
	// CursorID sets the keyset cursor delivery id.
	CursorID string
	// Limit bounds the returned rows.
	Limit int
}

// DeliveryStats aggregates the journal statuses in a window.
type DeliveryStats struct {
	// Pending sets the pending count.
	Pending int64
	// Delivered sets the delivered count.
	Delivered int64
	// Failed sets the failed count.
	Failed int64
}

// ListDeliveries returns the merchant journal rows ordered created_at DESC, id
// DESC with the keyset cursor (CBL-8.3).
func (r *DeliveryRepository) ListDeliveries(ctx context.Context, query ListQuery) ([]DeliveryRecord, error) {
	db := r.db.WithContext(ctx).Model(&DeliveryRecord{}).
		Where("merchant_id = ?", query.MerchantID)
	if query.EndpointID != "" {
		db = db.Where("endpoint_id = ?", query.EndpointID)
	}
	if query.Status != "" {
		db = db.Where("status = ?", query.Status)
	}
	if query.EventType != "" {
		db = db.Where("event_type = ?", query.EventType)
	}
	if query.From != nil {
		db = db.Where("created_at >= ?", query.From.UTC())
	}
	if query.To != nil {
		db = db.Where("created_at < ?", query.To.UTC())
	}
	if query.CursorCreatedAt != nil && query.CursorID != "" {
		db = db.Where("(created_at, id) < (?, ?)", query.CursorCreatedAt.UTC(), query.CursorID)
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 20
	}
	var records []DeliveryRecord
	if err := db.Order("created_at DESC").Order("id DESC").Limit(limit).Find(&records).Error; err != nil {
		return nil, fmt.Errorf("storage: list deliveries: %w", err)
	}
	return records, nil
}

// DeliveryByID returns one delivery row.
func (r *DeliveryRepository) DeliveryByID(ctx context.Context, deliveryID string) (DeliveryRecord, error) {
	var record DeliveryRecord
	err := r.db.WithContext(ctx).Where("id = ?", deliveryID).First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return DeliveryRecord{}, ErrNotFound
	}
	if err != nil {
		return DeliveryRecord{}, fmt.Errorf("storage: delivery by id: %w", err)
	}
	return record, nil
}

// DeliveryStats counts the merchant journal rows by status in the window.
func (r *DeliveryRepository) DeliveryStats(ctx context.Context, merchantID string, from, to time.Time) (DeliveryStats, error) {
	var rows []struct {
		Status string
		Count  int64
	}
	err := r.db.WithContext(ctx).Raw(`
		SELECT status, count(*) AS count FROM webhook_deliveries
		WHERE merchant_id = ? AND created_at >= ? AND created_at < ?
		GROUP BY status`, merchantID, from.UTC(), to.UTC()).Scan(&rows).Error
	if err != nil {
		return DeliveryStats{}, fmt.Errorf("storage: delivery stats: %w", err)
	}
	var stats DeliveryStats
	for _, row := range rows {
		switch row.Status {
		case DeliveryPending:
			stats.Pending = row.Count
		case DeliveryDelivered:
			stats.Delivered = row.Count
		case DeliveryFailed:
			stats.Failed = row.Count
		}
	}
	return stats, nil
}

// ResendFailed resets a failed delivery for a new schedule round; a row that
// is not failed (or not found) yields ErrNotFound for the caller re-read.
func (r *DeliveryRepository) ResendFailed(ctx context.Context, deliveryID string, now time.Time) (DeliveryRecord, error) {
	var record DeliveryRecord
	err := r.db.WithContext(ctx).Raw(`
		UPDATE webhook_deliveries
		SET status = 'pending', retry_attempts = 0, next_attempt_at = ?, last_error = ''
		WHERE id = ? AND status = 'failed'
		RETURNING id, event_id, source_event_id, merchant_id, endpoint_id, event_type,
			body, status, attempts, retry_attempts, next_attempt_at, last_error,
			created_at, delivered_at`, now.UTC(), deliveryID).Scan(&record).Error
	if err != nil {
		return DeliveryRecord{}, fmt.Errorf("storage: resend delivery: %w", err)
	}
	if record.ID == "" {
		return DeliveryRecord{}, ErrNotFound
	}
	return record, nil
}
