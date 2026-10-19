package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// Storage errors.
var (
	// ErrNotFound reports a missing record.
	ErrNotFound = errors.New("storage: not found")
	// ErrConflict reports a uniqueness violation.
	ErrConflict = errors.New("storage: conflict")
)

// EndpointRecord describes a stored webhook endpoint row.
type EndpointRecord struct {
	// ID sets the endpoint identifier (UUIDv7).
	ID string `gorm:"column:id;primaryKey"`
	// MerchantID sets the owning merchant.
	MerchantID string `gorm:"column:merchant_id"`
	// URL sets the delivery address.
	URL string `gorm:"column:url"`
	// Events sets the subscribed CF-3 types.
	Events []string `gorm:"column:events;serializer:json"`
	// SecretCiphertext sets the encrypted signing secret.
	SecretCiphertext []byte `gorm:"column:secret_ciphertext"`
	// SecretNonce sets the AES-GCM nonce of the secret.
	SecretNonce []byte `gorm:"column:secret_nonce"`
	// SecretKeyVersion sets the encryption key version of the ciphertext
	// (reserved for the rotation path; 8.1 always writes 1).
	SecretKeyVersion int `gorm:"column:secret_key_version"`
	// IdempotencyKey sets the registration key.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RequestHash sets the order-insensitive request fingerprint.
	RequestHash string `gorm:"column:request_hash"`
	// CreatedAt sets the registration time.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName sets the table of the record.
func (EndpointRecord) TableName() string { return "webhook_endpoints" }

// EndpointRepository stores webhook endpoints.
type EndpointRepository struct {
	db *gorm.DB
}

// NewEndpointRepository creates the repository.
func NewEndpointRepository(db *gorm.DB) *EndpointRepository {
	return &EndpointRepository{db: db}
}

// Insert stores the record; a duplicate (merchant, key) or (merchant, url)
// answers ErrConflict.
func (r *EndpointRepository) Insert(ctx context.Context, record EndpointRecord) error {
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.SecretKeyVersion == 0 {
		record.SecretKeyVersion = 1
	}
	if err := r.db.WithContext(ctx).Create(&record).Error; err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return fmt.Errorf("storage: insert endpoint: %w", err)
	}
	return nil
}

// ByMerchantKey returns the endpoint of the merchant and idempotency key.
func (r *EndpointRepository) ByMerchantKey(ctx context.Context, merchantID, key string) (EndpointRecord, error) {
	var record EndpointRecord
	err := r.db.WithContext(ctx).
		Where("merchant_id = ? AND idempotency_key = ?", merchantID, key).
		Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return EndpointRecord{}, ErrNotFound
	}
	if err != nil {
		return EndpointRecord{}, fmt.Errorf("storage: load endpoint: %w", err)
	}
	return record, nil
}

// ByID returns the endpoint by its identifier.
func (r *EndpointRepository) ByID(ctx context.Context, id string) (EndpointRecord, error) {
	var record EndpointRecord
	err := r.db.WithContext(ctx).Where("id = ?", id).Take(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return EndpointRecord{}, ErrNotFound
	}
	if err != nil {
		return EndpointRecord{}, fmt.Errorf("storage: load endpoint: %w", err)
	}
	return record, nil
}

// ListEndpoints returns every endpoint of the merchant in a deterministic order.
func (r *EndpointRepository) ListEndpoints(ctx context.Context, merchantID string) ([]EndpointRecord, error) {
	records := make([]EndpointRecord, 0)
	err := r.db.WithContext(ctx).
		Where("merchant_id = ?", merchantID).
		Order("created_at, id").
		Find(&records).Error
	if err != nil {
		return nil, fmt.Errorf("storage: list endpoints: %w", err)
	}
	return records, nil
}

// isUniqueViolation reports the PostgreSQL unique violation (SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
