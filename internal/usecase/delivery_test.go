package usecase_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ZMichael3012/cbl-kit/hmac"
	"github.com/ZMichael3012/cbl-kit/inbox"
	"gorm.io/gorm"

	"github.com/cbl/cbl-notification/internal/domain"
	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/usecase"
)

// testSecret sets the delivered signing secret of the fixtures.
const testSecret = "a4029ad5874b8448ea9954bfce640266e7aae888480e8c136b19ff246c417c3a"

// fakeDeliveryStore implements the handler and dispatcher stores in memory.
type fakeDeliveryStore struct {
	mu         sync.Mutex
	endpoints  []storage.EndpointRecord
	deliveries []storage.DeliveryRecord
	attempts   []storage.AttemptRecord
	claimed    []storage.DeliveryRecord
	statuses   map[string]string
	next       map[string]time.Time
	errors     map[string]string
}

// newFakeDeliveryStore builds an empty store.
func newFakeDeliveryStore() *fakeDeliveryStore {
	return &fakeDeliveryStore{statuses: map[string]string{}, next: map[string]time.Time{}, errors: map[string]string{}}
}

// ListEndpoints returns the merchant endpoints.
func (s *fakeDeliveryStore) ListEndpoints(_ context.Context, merchantID string) ([]storage.EndpointRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []storage.EndpointRecord
	for _, endpoint := range s.endpoints {
		if endpoint.MerchantID == merchantID {
			out = append(out, endpoint)
		}
	}
	return out, nil
}

// InsertDelivery appends the delivery.
func (s *fakeDeliveryStore) InsertDelivery(_ context.Context, _ *gorm.DB, record storage.DeliveryRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries = append(s.deliveries, record)
	return nil
}

// ClaimDue returns the preconfigured claimed deliveries.
func (s *fakeDeliveryStore) ClaimDue(context.Context, time.Time, time.Duration, int) ([]storage.DeliveryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	claimed := s.claimed
	s.claimed = nil
	return claimed, nil
}

// EndpointByID returns the endpoint.
func (s *fakeDeliveryStore) EndpointByID(_ context.Context, id string) (storage.EndpointRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, endpoint := range s.endpoints {
		if endpoint.ID == id {
			return endpoint, nil
		}
	}
	return storage.EndpointRecord{}, storage.ErrNotFound
}

// RecordAttempt appends the attempt.
func (s *fakeDeliveryStore) RecordAttempt(_ context.Context, attempt storage.AttemptRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts = append(s.attempts, attempt)
	return nil
}

// MarkDelivered marks the delivery delivered.
func (s *fakeDeliveryStore) MarkDelivered(_ context.Context, deliveryID string, _ int, _ int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[deliveryID] = "delivered"
	return nil
}

// ScheduleRetry schedules the next attempt.
func (s *fakeDeliveryStore) ScheduleRetry(_ context.Context, deliveryID string, _ int, _ int, next time.Time, lastError string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[deliveryID] = "pending"
	s.next[deliveryID] = next
	s.errors[deliveryID] = lastError
	return nil
}

// MarkFailed marks the delivery failed.
func (s *fakeDeliveryStore) MarkFailed(_ context.Context, deliveryID string, _ int, _ int, lastError string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[deliveryID] = "failed"
	s.errors[deliveryID] = lastError
	return nil
}

// paymentMessage builds an inbox message of a payment event.
func paymentMessage(t *testing.T, eventType eventsv1.PaymentEventType) inbox.Message {
	t.Helper()
	value, err := proto.Marshal(&eventsv1.PaymentEvent{
		PaymentId:      "01a154e0-0000-7000-8000-000000000001",
		MerchantId:     testMerchantID,
		Type:           eventType,
		ExpectedAmount: &commonv1.Money{MinorUnits: 60_000000, Asset: "USDT", Decimals: 6},
		OccurredAt:     timestamppb.New(time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)),
	})
	require.NoError(t, err)
	return inbox.Message{Value: value, Headers: map[string]string{"event_id": "01a154e0-0000-7000-8000-000000000901"}}
}

// endpoint builds a registered endpoint.
func endpoint(id, merchantID string, events ...string) storage.EndpointRecord {
	return storage.EndpointRecord{
		ID:               id,
		MerchantID:       merchantID,
		URL:              "https://merchant.example.com/hooks",
		Events:           events,
		SecretCiphertext: []byte{1},
		SecretNonce:      []byte{1},
	}
}

func TestHandlerCreatesOneDeliveryPerMatchingEndpoint(t *testing.T) {
	store := newFakeDeliveryStore()
	store.endpoints = []storage.EndpointRecord{
		endpoint("01a154e0-0000-7000-8000-000000000101", testMerchantID, "invoice.completed", "payout.failed"),
		endpoint("01a154e0-0000-7000-8000-000000000102", testMerchantID, "invoice.detected"),
		endpoint("01a154e0-0000-7000-8000-000000000103", "01a154aa-0000-7000-8000-000000000009", "invoice.completed"),
	}
	handler := usecase.NewEventHandler(usecase.HandlerPayments, store, nil)

	require.NoError(t, handler.Handle(context.Background(), nil, paymentMessage(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED)))
	require.Len(t, store.deliveries, 1, "only the merchant endpoint subscribing to the type")
	delivery := store.deliveries[0]
	assert.Equal(t, "01a154e0-0000-7000-8000-000000000101", delivery.EndpointID)
	assert.Equal(t, "invoice.completed", delivery.EventType)
	assert.Equal(t, testMerchantID, delivery.MerchantID)
	assert.Equal(t, "pending", delivery.Status)
	assert.Regexp(t, `^evt_`, delivery.EventID)
	assert.Contains(t, string(delivery.Body), `"type":"invoice.completed"`)
	assert.Contains(t, string(delivery.Body), delivery.EventID)

	// A skipped type creates nothing.
	require.NoError(t, handler.Handle(context.Background(), nil, paymentMessage(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CREATED)))
	assert.Len(t, store.deliveries, 1)
}

func TestHandlerRejectsInvalidMerchant(t *testing.T) {
	store := newFakeDeliveryStore()
	handler := usecase.NewEventHandler(usecase.HandlerPayments, store, nil)
	value, err := proto.Marshal(&eventsv1.PaymentEvent{
		PaymentId:  "01a154e0-0000-7000-8000-000000000001",
		MerchantId: "not-a-uuid",
		Type:       eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED,
		OccurredAt: timestamppb.New(time.Now().UTC()),
	})
	require.NoError(t, err)
	require.Error(t, handler.Handle(context.Background(), nil, inbox.Message{Value: value}))
	assert.Empty(t, store.deliveries)
}

func TestDispatcherDeliversAndSigns(t *testing.T) {
	store := newFakeDeliveryStore()
	store.endpoints = []storage.EndpointRecord{endpoint("01a154e0-0000-7000-8000-000000000101", testMerchantID)}
	key := make([]byte, 32)
	ciphertext, nonce, err := domain.EncryptSecret(key, store.endpoints[0].ID, testSecret)
	require.NoError(t, err)
	store.endpoints[0].SecretCiphertext = ciphertext
	store.endpoints[0].SecretNonce = nonce

	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	var gotSignature, gotEventID string
	var gotBody []byte
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotSignature = r.Header.Get("X-Billing-Signature")
		gotEventID = r.Header.Get("X-Billing-Event-Id")
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()

	store.endpoints[0].URL = sink.URL
	store.claimed = []storage.DeliveryRecord{{
		ID: "01a154e0-0000-7000-8000-000000000201", EventID: "evt_01a154e0-0000-7000-8000-000000000202",
		MerchantID: testMerchantID, EndpointID: store.endpoints[0].ID, EventType: "invoice.completed",
		Body:   []byte(`{"id":"evt_01a154e0-0000-7000-8000-000000000202","type":"invoice.completed"}`),
		Status: "pending", Attempts: 0,
	}}
	dispatcher := usecase.NewDispatcher(store, key, nil,
		usecase.WithDispatcherClock(func() time.Time { return now }),
		usecase.WithDispatcherSender(usecase.NewHTTPSender(time.Second,
			usecase.WithHTTPSenderDial((&net.Dialer{Timeout: time.Second}).DialContext))))
	require.NoError(t, dispatcher.ProcessDue(context.Background()))

	assert.Equal(t, "delivered", store.statuses["01a154e0-0000-7000-8000-000000000201"])
	require.Len(t, store.attempts, 1)
	assert.Equal(t, http.StatusOK, store.attempts[0].HTTPStatus)
	assert.Equal(t, gotEventID, "evt_01a154e0-0000-7000-8000-000000000202")
	require.NoError(t, hmac.Verify([]byte(testSecret), gotSignature, gotBody, now, hmac.DefaultWindow))
}

func TestDispatcherRetriesAndFails(t *testing.T) {
	store := newFakeDeliveryStore()
	store.endpoints = []storage.EndpointRecord{endpoint("01a154e0-0000-7000-8000-000000000101", testMerchantID)}
	key := make([]byte, 32)
	ciphertext, nonce, err := domain.EncryptSecret(key, store.endpoints[0].ID, testSecret)
	require.NoError(t, err)
	store.endpoints[0].SecretCiphertext = ciphertext
	store.endpoints[0].SecretNonce = nonce
	store.endpoints[0].URL = "https://merchant.example.com/hooks"

	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	delivery := storage.DeliveryRecord{
		ID: "01a154e0-0000-7000-8000-000000000201", EventID: "evt_01a154e0-0000-7000-8000-000000000202",
		MerchantID: testMerchantID, EndpointID: store.endpoints[0].ID, EventType: "invoice.completed",
		Body: []byte(`{"id":"evt_01a154e0-0000-7000-8000-000000000202"}`), Status: "pending",
	}
	dispatcher := usecase.NewDispatcher(store, key, nil,
		usecase.WithDispatcherClock(func() time.Time { return now }),
		usecase.WithDispatcherSender(failingSender{}))

	store.claimed = []storage.DeliveryRecord{delivery}
	require.NoError(t, dispatcher.ProcessDue(context.Background()))
	assert.Equal(t, "pending", store.statuses[delivery.ID])
	assert.Equal(t, now.Add(time.Second), store.next[delivery.ID], "the first retry follows 1s")

	// The exhausted schedule marks the delivery failed.
	exhausted := delivery
	exhausted.Attempts = len(usecase.RetrySchedule)
	exhausted.RetryAttempts = len(usecase.RetrySchedule)
	store.claimed = []storage.DeliveryRecord{exhausted}
	require.NoError(t, dispatcher.ProcessDue(context.Background()))
	assert.Equal(t, "failed", store.statuses[delivery.ID])
}

// failingSender always answers a transport failure.
type failingSender struct{}

// Send returns a transport error.
func (failingSender) Send(context.Context, string, []byte, []byte, string, time.Time) (int, error) {
	return 0, assert.AnError
}
