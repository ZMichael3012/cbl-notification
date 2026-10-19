package usecase_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/cbl/cbl-kit/inbox"

	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/usecase"
)

// timeoutDial returns a timeout network error.
func timeoutDial(context.Context, string, string) (net.Conn, error) {
	return nil, timeoutError{}
}

// timeoutError implements net.Error with a timeout.
type timeoutError struct{}

// Error implements the error interface.
func (timeoutError) Error() string { return "i/o timeout" }

// Timeout reports a timeout.
func (timeoutError) Timeout() bool { return true }

// Temporary reports a temporary failure.
func (timeoutError) Temporary() bool { return true }

func TestSenderErrorClassification(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	body := []byte(`{"id":"evt_x"}`)
	cases := []struct {
		name string
		dial usecase.DialFunc
		want string
	}{
		{"timeout", timeoutDial, "delivery timeout"},
		{"dns", func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("no such host") }, "delivery dns failure"},
		{"tls", func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("x509 certificate error")
		}, "delivery tls failure"},
		{"refused", func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("connection refused") }, "delivery connection failure"},
		{"other", func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("boom at https://secret.example.com/x?token=1")
		}, "delivery transport failure"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sender := usecase.NewHTTPSender(time.Second, usecase.WithHTTPSenderDial(tc.dial))
			_, err := sender.Send(context.Background(), "https://merchant.example.com/hooks", body, []byte("secret"), "evt_x", now)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.NotContains(t, err.Error(), "secret.example.com")
		})
	}
}

func TestHandlerRejectsOversizedAndUntaggedEvents(t *testing.T) {
	store := newFakeDeliveryStore()
	handler := usecase.NewEventHandler(usecase.HandlerPayments, store, nil)

	oversized := inbox.Message{Value: make([]byte, 300<<10), Headers: map[string]string{"event_id": "01a154e0-0000-7000-8000-000000000901"}}
	require.Error(t, handler.Handle(context.Background(), nil, oversized))

	untagged := paymentMessage(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED)
	untagged.Headers = map[string]string{}
	require.Error(t, handler.Handle(context.Background(), nil, untagged))

	// An uppercase merchant id is not canonical.
	event := &eventsv1.PaymentEvent{
		PaymentId:  "01a154e0-0000-7000-8000-000000000001",
		MerchantId: strings.ToUpper(testMerchantID),
		Type:       eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED,
		OccurredAt: timestamppb.New(time.Now().UTC()),
	}
	value, err := proto.Marshal(event)
	require.NoError(t, err)
	require.Error(t, handler.Handle(context.Background(), nil, inbox.Message{Value: value, Headers: map[string]string{
		"event_id": "01a154e0-0000-7000-8000-000000000902",
	}}))

	// An unknown payout enum value is an invalid event.
	payoutHandler := usecase.NewEventHandler(usecase.HandlerPayouts, store, nil)
	unknown := &eventsv1.PayoutEvent{
		PayoutId:   "01a154e0-0000-7000-8000-000000000010",
		MerchantId: testMerchantID,
		Type:       eventsv1.PayoutEventType(99),
		OccurredAt: timestamppb.New(time.Now().UTC()),
	}
	value, err = proto.Marshal(unknown)
	require.NoError(t, err)
	require.Error(t, payoutHandler.Handle(context.Background(), nil, inbox.Message{Value: value, Headers: map[string]string{
		"event_id": "01a154e0-0000-7000-8000-000000000903",
	}}))
}

func TestHandlerNoEndpointsAndMultipleEndpoints(t *testing.T) {
	// No endpoints: acknowledged, no deliveries.
	empty := newFakeDeliveryStore()
	handler := usecase.NewEventHandler(usecase.HandlerPayments, empty, nil)
	require.NoError(t, handler.Handle(context.Background(), nil, paymentMessage(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED)))
	assert.Empty(t, empty.deliveries)

	// Two endpoints of the same merchant subscribing to the type get one
	// delivery each with distinct event ids.
	store := newFakeDeliveryStore()
	store.endpoints = []storage.EndpointRecord{
		endpoint("01a154e0-0000-7000-8000-000000000101", testMerchantID, "invoice.completed"),
		endpoint("01a154e0-0000-7000-8000-000000000102", testMerchantID, "invoice.completed"),
	}
	handler = usecase.NewEventHandler(usecase.HandlerPayments, store, nil)
	require.NoError(t, handler.Handle(context.Background(), nil, paymentMessage(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED)))
	require.Len(t, store.deliveries, 2)
	assert.NotEqual(t, store.deliveries[0].EventID, store.deliveries[1].EventID)
	assert.Equal(t, store.deliveries[0].SourceEventID, store.deliveries[1].SourceEventID)
}

func TestHandlerClockOption(t *testing.T) {
	store := newFakeDeliveryStore()
	store.endpoints = []storage.EndpointRecord{endpoint("01a154e0-0000-7000-8000-000000000101", testMerchantID, "invoice.completed")}
	fixed := time.Date(2026, 10, 19, 12, 30, 0, 0, time.UTC)
	handler := usecase.NewEventHandler(usecase.HandlerPayments, store, nil,
		usecase.WithEventHandlerClock(func() time.Time { return fixed }))
	require.NoError(t, handler.Handle(context.Background(), nil, paymentMessage(t, eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED)))
	require.Len(t, store.deliveries, 1)
	require.NotNil(t, store.deliveries[0].NextAttemptAt)
	assert.Equal(t, fixed, *store.deliveries[0].NextAttemptAt)
}

func TestDispatcherRetryScheduleAttempts(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	for attempt := 1; attempt <= 6; attempt++ {
		store := newFakeDeliveryStore()
		store.endpoints = []storage.EndpointRecord{endpoint("01a154e0-0000-7000-8000-000000000101", testMerchantID)}
		key := make([]byte, 32)
		store.endpoints[0].URL = "https://merchant.example.com/hooks"
		delivery := storage.DeliveryRecord{
			ID: "01a154e0-0000-7000-8000-000000000201", EventID: "evt_x",
			MerchantID: testMerchantID, EndpointID: store.endpoints[0].ID, Body: []byte(`{}`),
			Attempts: attempt - 1,
		}
		store.claimed = []storage.DeliveryRecord{delivery}
		dispatcher := usecase.NewDispatcher(store, key, nil,
			usecase.WithDispatcherClock(func() time.Time { return now }),
			usecase.WithDispatcherSender(failingSender{}))
		require.NoError(t, dispatcher.ProcessDue(context.Background()))
		assert.Equal(t, "pending", store.statuses[delivery.ID], "attempt %d", attempt)
		assert.Equal(t, now.Add(usecase.RetrySchedule[attempt-1]), store.next[delivery.ID], "attempt %d", attempt)
	}

	// The seventh failed attempt marks the delivery failed.
	store := newFakeDeliveryStore()
	store.endpoints = []storage.EndpointRecord{endpoint("01a154e0-0000-7000-8000-000000000101", testMerchantID)}
	store.endpoints[0].URL = "https://merchant.example.com/hooks"
	delivery := storage.DeliveryRecord{
		ID: "01a154e0-0000-7000-8000-000000000202", EventID: "evt_y",
		MerchantID: testMerchantID, EndpointID: store.endpoints[0].ID, Body: []byte(`{}`),
		Attempts: len(usecase.RetrySchedule),
	}
	store.claimed = []storage.DeliveryRecord{delivery}
	dispatcher := usecase.NewDispatcher(store, make([]byte, 32), nil,
		usecase.WithDispatcherClock(func() time.Time { return now }),
		usecase.WithDispatcherSender(failingSender{}),
		usecase.WithDispatcherInterval(0),
		usecase.WithDispatcherAttemptTimeout(time.Second))
	require.NoError(t, dispatcher.ProcessDue(context.Background()))
	assert.Equal(t, "failed", store.statuses[delivery.ID])
}

func TestDispatcherOwnershipMismatch(t *testing.T) {
	store := newFakeDeliveryStore()
	store.endpoints = []storage.EndpointRecord{endpoint("01a154e0-0000-7000-8000-000000000101", "01a154aa-0000-7000-8000-000000000001")}
	store.claimed = []storage.DeliveryRecord{{
		ID: "01a154e0-0000-7000-8000-000000000201", EventID: "evt_x",
		MerchantID: testMerchantID, EndpointID: store.endpoints[0].ID, Body: []byte(`{}`),
	}}
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	dispatcher := usecase.NewDispatcher(store, make([]byte, 32), nil,
		usecase.WithDispatcherClock(func() time.Time { return now }),
		usecase.WithDispatcherSender(failingSender{}))
	require.NoError(t, dispatcher.ProcessDue(context.Background()))
	assert.Equal(t, "failed", store.statuses["01a154e0-0000-7000-8000-000000000201"])
}
