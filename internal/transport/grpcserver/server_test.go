package grpcserver_test

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	notificationv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/notification/v1"

	"github.com/ZMichael3012/cbl-kit/grpcmw"

	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/transport/grpcserver"
	"github.com/cbl/cbl-notification/internal/usecase"
)

// testMerchantID sets the canonical merchant UUID for the tests.
const testMerchantID = "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d"

// memStore is an in-memory EndpointStore for the transport tests.
type memStore struct {
	records []storage.EndpointRecord
}

// Insert appends the record enforcing both unique identities.
func (s *memStore) Insert(_ context.Context, record storage.EndpointRecord) error {
	for _, existing := range s.records {
		if existing.MerchantID == record.MerchantID &&
			(existing.IdempotencyKey == record.IdempotencyKey || existing.URL == record.URL) {
			return storage.ErrConflict
		}
	}
	s.records = append(s.records, record)
	return nil
}

// ByMerchantKey returns the record of the merchant and key.
func (s *memStore) ByMerchantKey(_ context.Context, merchantID, key string) (storage.EndpointRecord, error) {
	for _, existing := range s.records {
		if existing.MerchantID == merchantID && existing.IdempotencyKey == key {
			return existing, nil
		}
	}
	return storage.EndpointRecord{}, storage.ErrNotFound
}

// newClient builds the gRPC client over an in-process listener.
func newClient(t *testing.T) notificationv1.NotificationServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	grpcserver.NewServer(usecase.NewRegisterService(&memStore{}, bytes.Repeat([]byte{5}, 32)), nil, nil).Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return notificationv1.NewNotificationServiceClient(conn)
}

func TestRegisterWebhookEndpointHappyPath(t *testing.T) {
	client := newClient(t)
	request := &notificationv1.RegisterWebhookEndpointRequest{
		IdempotencyKey: "key-12345",
		MerchantId:     testMerchantID,
		Url:            "https://example.com/hooks",
		Events:         []string{"invoice.completed", "payout.failed"},
	}
	response, err := client.RegisterWebhookEndpoint(context.Background(), request)
	require.NoError(t, err)
	assert.NotEmpty(t, response.GetEndpointId())
	assert.Equal(t, "https://example.com/hooks", response.GetUrl())
	assert.Equal(t, []string{"invoice.completed", "payout.failed"}, response.GetEvents())
	assert.Len(t, response.GetSigningSecret(), 64)

	replay, err := client.RegisterWebhookEndpoint(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, response.GetEndpointId(), replay.GetEndpointId())
	assert.Equal(t, response.GetSigningSecret(), replay.GetSigningSecret())
}

func TestRegisterWebhookEndpointValidation(t *testing.T) {
	client := newClient(t)
	cases := map[string]*notificationv1.RegisterWebhookEndpointRequest{
		"missing merchant": {IdempotencyKey: "key-12345", Url: "https://example.com/hooks", Events: []string{"invoice.completed"}},
		"bad merchant":     {IdempotencyKey: "key-12345", MerchantId: "not-a-uuid", Url: "https://example.com/hooks", Events: []string{"invoice.completed"}},
		"http url":         {IdempotencyKey: "key-12345", MerchantId: testMerchantID, Url: "http://example.com/hooks", Events: []string{"invoice.completed"}},
		"private url":      {IdempotencyKey: "key-12345", MerchantId: testMerchantID, Url: "https://10.0.0.1/hooks", Events: []string{"invoice.completed"}},
		"no events":        {IdempotencyKey: "key-12345", MerchantId: testMerchantID, Url: "https://example.com/hooks"},
		"unknown event":    {IdempotencyKey: "key-12345", MerchantId: testMerchantID, Url: "https://example.com/hooks", Events: []string{"invoice.unknown"}},
		"short key":        {IdempotencyKey: "abc", MerchantId: testMerchantID, Url: "https://example.com/hooks", Events: []string{"invoice.completed"}},
	}
	for name, request := range cases {
		_, err := client.RegisterWebhookEndpoint(context.Background(), request)
		require.Error(t, err, name)
		assert.Equal(t, codes.InvalidArgument, status.Code(err), name)
	}
}

func TestRegisterWebhookEndpointConflict(t *testing.T) {
	client := newClient(t)
	first := &notificationv1.RegisterWebhookEndpointRequest{
		IdempotencyKey: "key-12345",
		MerchantId:     testMerchantID,
		Url:            "https://example.com/hooks",
		Events:         []string{"invoice.completed"},
	}
	_, err := client.RegisterWebhookEndpoint(context.Background(), first)
	require.NoError(t, err)

	changed := &notificationv1.RegisterWebhookEndpointRequest{
		IdempotencyKey: first.GetIdempotencyKey(),
		MerchantId:     first.GetMerchantId(),
		Url:            "https://other.example.com/hooks",
		Events:         first.GetEvents(),
	}
	_, err = client.RegisterWebhookEndpoint(context.Background(), changed)
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	// A fresh key with the same url answers the frozen url conflict.
	urlClash := &notificationv1.RegisterWebhookEndpointRequest{
		IdempotencyKey: "key-99999",
		MerchantId:     first.GetMerchantId(),
		Url:            first.GetUrl(),
		Events:         first.GetEvents(),
	}
	_, err = client.RegisterWebhookEndpoint(context.Background(), urlClash)
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "already has an active endpoint")
}

func TestRegisterWebhookEndpointMerchantMetadata(t *testing.T) {
	client := newClient(t)
	request := &notificationv1.RegisterWebhookEndpointRequest{
		IdempotencyKey: "key-12345",
		MerchantId:     testMerchantID,
		Url:            "https://example.com/hooks",
		Events:         []string{"invoice.completed"},
	}

	// A mismatching x-merchant-id is rejected (defense in depth).
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-merchant-id", "01a154aa-0000-7000-8000-000000000009")
	_, err := client.RegisterWebhookEndpoint(ctx, request)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	// A matching metadata is accepted.
	matching := metadata.AppendToOutgoingContext(context.Background(), "x-merchant-id", testMerchantID)
	_, err = client.RegisterWebhookEndpoint(matching, request)
	require.NoError(t, err)
}

// fakeAdmin implements grpcserver.AdminPort.
type fakeAdmin struct {
	page   usecase.DeliveryPage
	stats  usecase.DeliveryStats
	resend usecase.ResendResult
	err    error
}

// List returns the preconfigured page.
func (f *fakeAdmin) List(context.Context, usecase.ListQuery) (usecase.DeliveryPage, error) {
	return f.page, f.err
}

// Stats returns the preconfigured aggregates.
func (f *fakeAdmin) Stats(context.Context, string, *time.Time, *time.Time) (usecase.DeliveryStats, error) {
	return f.stats, f.err
}

// Resend returns the preconfigured result.
func (f *fakeAdmin) Resend(context.Context, string, string) (usecase.ResendResult, error) {
	return f.resend, f.err
}

// newAdminClient builds the gRPC client over an in-process listener with the
// fake operator service.
func newAdminClient(t *testing.T, admin *fakeAdmin) notificationv1.NotificationServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	grpcserver.NewServer(usecase.NewRegisterService(&memStore{}, bytes.Repeat([]byte{5}, 32)), admin, nil).Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return notificationv1.NewNotificationServiceClient(conn)
}

func TestListWebhookDeliveriesMapsThePage(t *testing.T) {
	delivered := time.Date(2026, 10, 19, 12, 30, 0, 0, time.UTC)
	next := time.Date(2026, 10, 19, 12, 31, 0, 0, time.UTC)
	admin := &fakeAdmin{page: usecase.DeliveryPage{
		Deliveries: []storage.DeliveryRecord{{
			ID: "01a154e0-0000-7000-8000-000000000201", EventID: "evt_01a154e0-0000-7000-8000-000000000202",
			EndpointID: "01a154e0-0000-7000-8000-000000000101", EventType: "invoice.completed",
			Status: storage.DeliveryDelivered, Attempts: 1, NextAttemptAt: &next, LastError: "",
			CreatedAt: delivered.Add(-time.Minute), DeliveredAt: &delivered, Body: []byte(`{"id":"evt_x"}`),
		}},
		NextToken: "cursor-1",
	}}
	client := newAdminClient(t, admin)
	response, err := client.ListWebhookDeliveries(context.Background(), &notificationv1.ListWebhookDeliveriesRequest{
		MerchantId: testMerchantID, PageSize: 20,
	})
	require.NoError(t, err)
	require.Len(t, response.GetDeliveries(), 1)
	row := response.GetDeliveries()[0]
	assert.Equal(t, "01a154e0-0000-7000-8000-000000000201", row.GetDeliveryId())
	assert.Equal(t, "invoice.completed", row.GetEventType())
	assert.Equal(t, "delivered", row.GetStatus())
	assert.Equal(t, int32(1), row.GetAttempts())
	assert.Equal(t, "cursor-1", response.GetNextPageToken())
	assert.NotEmpty(t, row.GetBodySha256())
	assert.Equal(t, delivered, row.GetDeliveredAt().AsTime())
}

func TestGetDeliveryStatsAndResend(t *testing.T) {
	next := time.Date(2026, 10, 19, 13, 0, 0, 0, time.UTC)
	admin := &fakeAdmin{
		stats: usecase.DeliveryStats{Total: 4, Pending: 1, Delivered: 2, Failed: 1, SuccessRate: 2.0 / 3.0},
		resend: usecase.ResendResult{Delivery: storage.DeliveryRecord{
			ID: "01a154e0-0000-7000-8000-000000000201", Status: storage.DeliveryPending,
			Attempts: 7, RetryAttempts: 0, NextAttemptAt: &next,
		}},
	}
	client := newAdminClient(t, admin)

	stats, err := client.GetDeliveryStats(context.Background(), &notificationv1.GetDeliveryStatsRequest{MerchantId: testMerchantID})
	require.NoError(t, err)
	assert.Equal(t, int64(4), stats.GetTotal())
	assert.Equal(t, int64(1), stats.GetFailed())
	assert.InDelta(t, 2.0/3.0, stats.GetSuccessRate(), 1e-9)

	resend, err := client.ResendWebhookDelivery(context.Background(), &notificationv1.ResendWebhookDeliveryRequest{
		DeliveryId: "01a154e0-0000-7000-8000-000000000201", IdempotencyKey: "key-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "pending", resend.GetStatus())
	assert.Equal(t, int32(7), resend.GetAttempts())
	assert.Equal(t, next, resend.GetNextAttemptAt().AsTime())
}

func TestDeliveryErrorMapping(t *testing.T) {
	cases := map[string]struct {
		err  error
		code codes.Code
	}{
		"not found":      {grpcmw.E(grpcmw.KindNotFound, "delivery_not_found", "missing", nil), codes.NotFound},
		"not resendable": {grpcmw.E(grpcmw.KindFailedPrecondition, "delivery_not_resendable", "delivered", nil), codes.FailedPrecondition},
		"invalid":        {grpcmw.E(grpcmw.KindInvalidArgument, "invalid_request", "bad", nil), codes.InvalidArgument},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			client := newAdminClient(t, &fakeAdmin{err: tc.err})
			_, err := client.ResendWebhookDelivery(context.Background(), &notificationv1.ResendWebhookDeliveryRequest{
				DeliveryId: "01a154e0-0000-7000-8000-000000000201", IdempotencyKey: "key-1",
			})
			require.Error(t, err)
			assert.Equal(t, tc.code, status.Code(err))
		})
	}

	invalidWindow := newAdminClient(t, &fakeAdmin{})
	_, err := invalidWindow.ListWebhookDeliveries(context.Background(), &notificationv1.ListWebhookDeliveriesRequest{
		MerchantId: testMerchantID,
		From:       &timestamppb.Timestamp{Seconds: 1, Nanos: -1},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
