package usecase_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/grpcmw"

	"github.com/cbl/cbl-notification/internal/domain"
	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/usecase"
)

// testMerchantID sets the canonical merchant UUID for the tests.
const testMerchantID = "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d"

// fakeStore is an in-memory EndpointStore.
type fakeStore struct {
	records []storage.EndpointRecord
}

// Insert appends the record, enforcing both unique identities.
func (s *fakeStore) Insert(_ context.Context, record storage.EndpointRecord) error {
	for _, existing := range s.records {
		if existing.MerchantID == record.MerchantID && existing.IdempotencyKey == record.IdempotencyKey {
			return storage.ErrConflict
		}
		if existing.MerchantID == record.MerchantID && existing.URL == record.URL {
			return storage.ErrConflict
		}
	}
	s.records = append(s.records, record)
	return nil
}

// ByMerchantKey returns the record of the merchant and key.
func (s *fakeStore) ByMerchantKey(_ context.Context, merchantID, key string) (storage.EndpointRecord, error) {
	for _, existing := range s.records {
		if existing.MerchantID == merchantID && existing.IdempotencyKey == key {
			return existing, nil
		}
	}
	return storage.EndpointRecord{}, storage.ErrNotFound
}

// newRegister builds the service with a fake store and a fixed key.
func newRegister(t *testing.T) (*usecase.RegisterService, *fakeStore) {
	t.Helper()
	store := &fakeStore{}
	return usecase.NewRegisterService(store, bytes.Repeat([]byte{9}, 32)), store
}

// validRequest builds a valid registration request.
func validRequest() domain.RegisterRequest {
	return domain.RegisterRequest{
		IdempotencyKey: "key-12345",
		MerchantID:     testMerchantID,
		URL:            "https://example.com/hooks",
		Events:         []string{"invoice.completed"},
	}
}

func TestRegisterCreatesEndpointWithEncryptedSecret(t *testing.T) {
	service, store := newRegister(t)
	result, err := service.Register(context.Background(), validRequest())
	require.NoError(t, err)
	assert.False(t, result.Replayed)
	assert.NotEmpty(t, result.EndpointID)
	assert.Equal(t, "https://example.com/hooks", result.URL)
	assert.Equal(t, []string{"invoice.completed"}, result.Events)
	assert.Len(t, result.SigningSecret, domain.SecretHexLen)

	require.Len(t, store.records, 1)
	record := store.records[0]
	assert.NotContains(t, string(record.SecretCiphertext), result.SigningSecret)
	assert.NotEqual(t, result.SigningSecret, string(record.SecretCiphertext))

	// The stored secret decrypts back to the delivered value.
	decrypted, err := domain.DecryptSecret(bytes.Repeat([]byte{9}, 32), record.ID, record.SecretCiphertext, record.SecretNonce)
	require.NoError(t, err)
	assert.Equal(t, result.SigningSecret, decrypted)
}

func TestRegisterReplayReturnsTheSameEndpoint(t *testing.T) {
	service, store := newRegister(t)
	first, err := service.Register(context.Background(), validRequest())
	require.NoError(t, err)

	replay, err := service.Register(context.Background(), validRequest())
	require.NoError(t, err)
	assert.True(t, replay.Replayed)
	assert.Equal(t, first.EndpointID, replay.EndpointID)
	assert.Equal(t, first.SigningSecret, replay.SigningSecret, "an idempotent replay returns the same response")
	assert.Len(t, store.records, 1)

	// The event order does not change the request identity (a distinct url
	// keeps the one-endpoint-per-url rule out of the way).
	multi := validRequest()
	multi.IdempotencyKey = "key-67890"
	multi.URL = "https://example.com/hooks-multi"
	multi.Events = []string{"payout.failed", "invoice.completed"}
	firstMulti, err := service.Register(context.Background(), multi)
	require.NoError(t, err)
	replayMulti := multi
	replayMulti.Events = []string{"invoice.completed", "payout.failed"}
	secondMulti, err := service.Register(context.Background(), replayMulti)
	require.NoError(t, err)
	assert.True(t, secondMulti.Replayed)
	assert.Equal(t, firstMulti.EndpointID, secondMulti.EndpointID)
}

func TestRegisterKeyReuseWithDifferentRequest(t *testing.T) {
	service, store := newRegister(t)
	_, err := service.Register(context.Background(), validRequest())
	require.NoError(t, err)

	changed := validRequest()
	changed.URL = "https://other.example.com/hooks"
	_, err = service.Register(context.Background(), changed)
	require.Error(t, err)
	assert.Equal(t, "idempotency_key_reuse", grpcmw.CodeOf(err))
	assert.Len(t, store.records, 1)

	changedEvents := validRequest()
	changedEvents.Events = []string{"payout.failed"}
	_, err = service.Register(context.Background(), changedEvents)
	require.Error(t, err)
	assert.Equal(t, "idempotency_key_reuse", grpcmw.CodeOf(err), "a different event set under the same key is a reuse")

	// A different merchant may reuse the same key.
	otherMerchant := validRequest()
	otherMerchant.MerchantID = "01a154aa-0000-7000-8000-000000000001"
	result, err := service.Register(context.Background(), otherMerchant)
	require.NoError(t, err)
	assert.False(t, result.Replayed)
	assert.Len(t, store.records, 2)
}

func TestRegisterURLConflict(t *testing.T) {
	service, store := newRegister(t)
	_, err := service.Register(context.Background(), validRequest())
	require.NoError(t, err)

	// The frozen REST rule: one active endpoint per merchant url (409
	// webhook_endpoint_exists), even under a fresh idempotency key.
	second := validRequest()
	second.IdempotencyKey = "key-99999"
	_, err = service.Register(context.Background(), second)
	require.Error(t, err)
	assert.Equal(t, "webhook_endpoint_exists", grpcmw.CodeOf(err))
	assert.Len(t, store.records, 1)
}

func TestRegisterDuplicateAndWhitespaceReplay(t *testing.T) {
	service, _ := newRegister(t)
	first := validRequest()
	first.Events = []string{" invoice.completed ", "invoice.completed", "payout.failed"}
	result, err := service.Register(context.Background(), first)
	require.NoError(t, err)
	assert.Equal(t, []string{"invoice.completed", "payout.failed"}, result.Events, "the stored list is normalized")

	// A canonical retry of the same logical request replays.
	replay := validRequest()
	replay.Events = []string{"invoice.completed", "payout.failed"}
	second, err := service.Register(context.Background(), replay)
	require.NoError(t, err)
	assert.True(t, second.Replayed)
	assert.Equal(t, result.EndpointID, second.EndpointID)
	assert.Equal(t, result.SigningSecret, second.SigningSecret)
}

func TestRegisterInvalidRequest(t *testing.T) {
	service, _ := newRegister(t)
	invalid := validRequest()
	invalid.URL = "http://example.com/hooks"
	_, err := service.Register(context.Background(), invalid)
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "https")
}

func TestRegisterConcurrentConflictResolvesToReplay(t *testing.T) {
	service, store := newRegister(t)
	first, err := service.Register(context.Background(), validRequest())
	require.NoError(t, err)

	// Simulate the racing insert: the pre-read misses the row, the insert
	// conflicts on the unique identity and the re-read resolves to the replay.
	racing := &racingStore{store: store}
	racingService := usecase.NewRegisterService(racing, bytes.Repeat([]byte{9}, 32))
	replay, err := racingService.Register(context.Background(), validRequest())
	require.NoError(t, err)
	assert.True(t, replay.Replayed)
	assert.Equal(t, first.EndpointID, replay.EndpointID)
	assert.Equal(t, first.SigningSecret, replay.SigningSecret)
	assert.Len(t, store.records, 1, "the race must not create a second row")
}

// racingStore hides the record on the first read to force the insert conflict.
type racingStore struct {
	store *fakeStore
	seen  bool
}

// Insert delegates to the fake store.
func (s *racingStore) Insert(ctx context.Context, record storage.EndpointRecord) error {
	return s.store.Insert(ctx, record)
}

// ByMerchantKey misses the first read, then delegates.
func (s *racingStore) ByMerchantKey(ctx context.Context, merchantID, key string) (storage.EndpointRecord, error) {
	if !s.seen {
		s.seen = true
		return storage.EndpointRecord{}, storage.ErrNotFound
	}
	return s.store.ByMerchantKey(ctx, merchantID, key)
}
