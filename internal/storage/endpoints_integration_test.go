//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/cbl/cbl-kit/grpcmw"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-notification/internal/domain"
	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/usecase"
)

// setupNotificationDB starts PostgreSQL, applies migrations and returns the DB.
func setupNotificationDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("notification"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))

	var version int64
	require.NoError(t, db.Raw("SELECT max(version_id) FROM goose_db_version").Scan(&version).Error)
	assert.Equal(t, int64(1), version, "the webhook_endpoints migration is applied")
	return db
}

// record builds a test endpoint record.
func record(merchantID, key, hash string) storage.EndpointRecord {
	return storage.EndpointRecord{
		ID:               "01a154bb-0000-7000-8000-000000000001",
		MerchantID:       merchantID,
		URL:              "https://example.com/hooks",
		Events:           []string{"invoice.completed", "payout.failed"},
		SecretCiphertext: []byte{1, 2, 3, 4},
		SecretNonce:      []byte{5, 6, 7, 8, 9, 10, 11, 12},
		IdempotencyKey:   key,
		RequestHash:      hash,
	}
}

func TestIntegrationRegisterThroughRepository(t *testing.T) {
	db := setupNotificationDB(t)
	repo := storage.NewEndpointRepository(db)
	service := usecase.NewRegisterService(repo, bytes.Repeat([]byte{3}, 32))
	ctx := context.Background()

	request := domain.RegisterRequest{
		IdempotencyKey: "key-12345",
		MerchantID:     "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d",
		URL:            "https://example.com/hooks",
		Events:         []string{"invoice.completed", "payout.failed"},
	}
	result, err := service.Register(ctx, request)
	require.NoError(t, err)

	// The stored secret is encrypted and decrypts back to the delivered value.
	stored, err := repo.ByMerchantKey(ctx, request.MerchantID, request.IdempotencyKey)
	require.NoError(t, err)
	assert.NotEqual(t, result.SigningSecret, string(stored.SecretCiphertext))
	assert.Equal(t, 1, stored.SecretKeyVersion)
	decrypted, err := domain.DecryptSecret(bytes.Repeat([]byte{3}, 32), stored.ID, stored.SecretCiphertext, stored.SecretNonce)
	require.NoError(t, err)
	assert.Equal(t, result.SigningSecret, decrypted)

	// The replay returns the same identity and secret.
	replay, err := service.Register(ctx, request)
	require.NoError(t, err)
	assert.True(t, replay.Replayed)
	assert.Equal(t, result.EndpointID, replay.EndpointID)
	assert.Equal(t, result.SigningSecret, replay.SigningSecret)

	// A changed request under the same key is the reuse conflict.
	changed := request
	changed.URL = "https://other.example.com/hooks"
	_, err = service.Register(ctx, changed)
	require.Error(t, err)
	assert.Equal(t, "idempotency_key_reuse", grpcmw.CodeOf(err))

	// A fresh key with the same url is the frozen url conflict.
	second := request
	second.IdempotencyKey = "key-99999"
	_, err = service.Register(ctx, second)
	require.Error(t, err)
	assert.Equal(t, "webhook_endpoint_exists", grpcmw.CodeOf(err))
}

func TestIntegrationEndpointRoundTrip(t *testing.T) {
	db := setupNotificationDB(t)
	repo := storage.NewEndpointRepository(db)
	ctx := context.Background()

	require.NoError(t, repo.Insert(ctx, record("0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", "key-12345", "hash-1")))

	stored, err := repo.ByMerchantKey(ctx, "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", "key-12345")
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/hooks", stored.URL)
	assert.Equal(t, []string{"invoice.completed", "payout.failed"}, stored.Events, "the event list round-trips")
	assert.Equal(t, []byte{1, 2, 3, 4}, stored.SecretCiphertext)
	assert.Equal(t, []byte{5, 6, 7, 8, 9, 10, 11, 12}, stored.SecretNonce)
	assert.Equal(t, "hash-1", stored.RequestHash)
	assert.False(t, stored.CreatedAt.IsZero())
}

func TestIntegrationEndpointConflictAndScope(t *testing.T) {
	db := setupNotificationDB(t)
	repo := storage.NewEndpointRepository(db)
	ctx := context.Background()

	require.NoError(t, repo.Insert(ctx, record("0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", "key-12345", "hash-1")))
	err := repo.Insert(ctx, record("0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", "key-12345", "hash-2"))
	require.ErrorIs(t, err, storage.ErrConflict)

	// Another merchant may reuse the key.
	other := record("01a154aa-0000-7000-8000-000000000001", "key-12345", "hash-3")
	other.ID = "01a154bb-0000-7000-8000-000000000002"
	require.NoError(t, repo.Insert(ctx, other))

	// The same url under a fresh key is the frozen url conflict.
	urlClash := record("0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", "key-77777", "hash-4")
	urlClash.ID = "01a154bb-0000-7000-8000-000000000003"
	err = repo.Insert(ctx, urlClash)
	require.ErrorIs(t, err, storage.ErrConflict)

	_, err = repo.ByMerchantKey(ctx, "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", "missing-key")
	require.ErrorIs(t, err, storage.ErrNotFound)
}
