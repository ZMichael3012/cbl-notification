package usecase_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ZMichael3012/cbl-kit/inbox"

	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/usecase"
)

func TestHTTPSenderOutcomes(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	body := []byte(`{"id":"evt_x"}`)
	secret := []byte("a4029ad5874b8448ea9954bfce640266")

	t.Run("2xx", func(t *testing.T) {
		sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.Equal(t, "evt_x", r.Header.Get("X-Billing-Event-Id"))
			assert.NotEmpty(t, r.Header.Get("X-Billing-Signature"))
			w.WriteHeader(http.StatusNoContent)
		}))
		defer sink.Close()
		status, err := plainSender().Send(context.Background(), sink.URL, body, secret, "evt_x", now)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, status)
	})

	t.Run("5xx", func(t *testing.T) {
		sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer sink.Close()
		status, err := plainSender().Send(context.Background(), sink.URL, body, secret, "evt_x", now)
		require.Error(t, err)
		assert.Equal(t, http.StatusInternalServerError, status)
	})

	t.Run("redirect is not followed", func(t *testing.T) {
		sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", "https://elsewhere.example.com")
			w.WriteHeader(http.StatusFound)
		}))
		defer sink.Close()
		status, err := plainSender().Send(context.Background(), sink.URL, body, secret, "evt_x", now)
		require.Error(t, err)
		assert.Equal(t, http.StatusFound, status)
	})

	t.Run("transport error", func(t *testing.T) {
		status, err := plainSender().Send(context.Background(), "https://127.0.0.1:1/hooks", body, secret, "evt_x", now)
		require.Error(t, err)
		assert.Zero(t, status)
		assert.NotContains(t, err.Error(), "127.0.0.1", "the address is not leaked")
	})

	t.Run("invalid url", func(t *testing.T) {
		_, err := plainSender().Send(context.Background(), "://bad", body, secret, "evt_x", now)
		require.Error(t, err)
	})
}

// plainSender builds a sender with the guard bypassed (the tests dial local).
func plainSender() *usecase.HTTPSender {
	return usecase.NewHTTPSender(time.Second,
		usecase.WithHTTPSenderDial((&net.Dialer{Timeout: time.Second}).DialContext))
}

// fakeResolver returns the configured addresses.
type fakeResolver struct {
	addresses []net.IPAddr
	err       error
}

// LookupIPAddr returns the configured addresses.
func (f fakeResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return f.addresses, f.err
}

func TestGuardedDial(t *testing.T) {
	ctx := context.Background()
	dialed := ""
	dial := func(_ context.Context, _ string, address string) (net.Conn, error) {
		dialed = address
		return nil, assert.AnError
	}

	// A host resolving to an internal address is rejected without a dial.
	for _, internal := range []string{"10.0.0.1", "127.0.0.1", "169.254.169.254", "100.64.0.1", "::1", "fd00::1"} {
		guarded := usecase.GuardedDial(dial, fakeResolver{addresses: []net.IPAddr{{IP: net.ParseIP(internal)}}})
		_, err := guarded(ctx, "tcp", "merchant.example.com:443")
		require.ErrorIs(t, err, usecase.ErrBlockedAddress, internal)
	}
	assert.Empty(t, dialed, "an internal target is never dialed")

	// A public host dials the pinned resolved address.
	guarded := usecase.GuardedDial(dial, fakeResolver{addresses: []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}})
	_, err := guarded(ctx, "tcp", "merchant.example.com:443")
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, "93.184.216.34:443", dialed)

	// An empty resolution fails closed.
	guarded = usecase.GuardedDial(dial, fakeResolver{})
	_, err = guarded(ctx, "tcp", "merchant.example.com:443")
	require.Error(t, err)
}

func TestHandlerPayoutAndRecon(t *testing.T) {
	store := newFakeDeliveryStore()
	store.endpoints = []storage.EndpointRecord{
		endpoint("01a154e0-0000-7000-8000-000000000101", testMerchantID, "refund.completed", "payout.failed"),
	}
	payoutHandler := usecase.NewEventHandler(usecase.HandlerPayouts, store, nil)
	require.NoError(t, payoutHandler.Handle(context.Background(), nil, payoutMessage(t, "refund.completed")))
	require.Len(t, store.deliveries, 1)
	assert.Equal(t, "refund.completed", store.deliveries[0].EventType)
	assert.Contains(t, string(store.deliveries[0].Body), `"refund"`)
	assert.Contains(t, string(store.deliveries[0].Body), `"invoice_id"`)

	// The recon handler acknowledges without deliveries.
	reconHandler := usecase.NewEventHandler(usecase.HandlerRecon, store, nil)
	require.NoError(t, reconHandler.Handle(context.Background(), nil, payoutMessage(t, "refund.completed")))
	assert.Len(t, store.deliveries, 1)

	// A malformed payout event is an invalid event.
	require.Error(t, payoutHandler.Handle(context.Background(), nil, inbox.Message{Value: []byte{0xff, 0xff}}))
}

func TestDispatcherPermanentFailures(t *testing.T) {
	store := newFakeDeliveryStore()
	key := make([]byte, 32)
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	dispatcher := usecase.NewDispatcher(store, key, nil, usecase.WithDispatcherClock(func() time.Time { return now }))

	// The endpoint is missing: the delivery fails without a send.
	store.claimed = []storage.DeliveryRecord{{
		ID: "01a154e0-0000-7000-8000-000000000201", EventID: "evt_01a154e0-0000-7000-8000-000000000202",
		EndpointID: "01a154e0-0000-7000-8000-000000000999", MerchantID: testMerchantID, Body: []byte(`{}`),
	}}
	require.NoError(t, dispatcher.ProcessDue(context.Background()))
	assert.Equal(t, "failed", store.statuses["01a154e0-0000-7000-8000-000000000201"])

	// An undecryptable secret follows the retry schedule (a key restore can
	// recover the deliveries), it does not fail the delivery immediately.
	store.endpoints = []storage.EndpointRecord{{
		ID: "01a154e0-0000-7000-8000-000000000101", MerchantID: testMerchantID,
		URL: "https://merchant.example.com/hooks", SecretCiphertext: []byte{1}, SecretNonce: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
	}}
	store.claimed = []storage.DeliveryRecord{{
		ID: "01a154e0-0000-7000-8000-000000000203", EventID: "evt_01a154e0-0000-7000-8000-000000000204",
		EndpointID: store.endpoints[0].ID, MerchantID: testMerchantID, Body: []byte(`{}`),
	}}
	require.NoError(t, dispatcher.ProcessDue(context.Background()))
	assert.Equal(t, "pending", store.statuses["01a154e0-0000-7000-8000-000000000203"])
	assert.Equal(t, now.Add(time.Second), store.next["01a154e0-0000-7000-8000-000000000203"])
}

// payoutMessage builds a payout inbox message with the refund marker.
func payoutMessage(t *testing.T, wantType string) inbox.Message {
	t.Helper()
	_ = wantType
	value, err := proto.Marshal(&eventsv1.PayoutEvent{
		PayoutId:        "01a154e0-0000-7000-8000-000000000010",
		MerchantId:      testMerchantID,
		Type:            eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPLETED,
		Amount:          &commonv1.Money{MinorUnits: 5_000000, Asset: "USDT", Decimals: 6},
		RefundInvoiceId: "01a154e0-0000-7000-8000-000000000020",
		OccurredAt:      timestamppb.New(time.Date(2026, 10, 19, 13, 0, 0, 0, time.UTC)),
	})
	require.NoError(t, err)
	return inbox.Message{Value: value, Headers: map[string]string{"event_id": "01a154e0-0000-7000-8000-000000000902"}}
}
