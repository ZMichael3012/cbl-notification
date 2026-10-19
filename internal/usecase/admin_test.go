package usecase_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/grpcmw"

	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/usecase"
)

const adminMerchantID = "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d"

// fakeAdminStore implements the delivery journal reads and the resend reset.
type fakeAdminStore struct {
	mu       sync.Mutex
	records  []storage.DeliveryRecord
	queried  []storage.ListQuery
	resends  int
	stats    storage.DeliveryStats
	failWith error
}

// ListDeliveries returns the preconfigured page and records the query.
func (s *fakeAdminStore) ListDeliveries(_ context.Context, query storage.ListQuery) ([]storage.DeliveryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queried = append(s.queried, query)
	if s.failWith != nil {
		return nil, s.failWith
	}
	limit := query.Limit
	if limit <= 0 || limit > len(s.records) {
		limit = len(s.records)
	}
	return append([]storage.DeliveryRecord(nil), s.records[:limit]...), nil
}

// DeliveryByID returns the matching record.
func (s *fakeAdminStore) DeliveryByID(_ context.Context, deliveryID string) (storage.DeliveryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWith != nil {
		return storage.DeliveryRecord{}, s.failWith
	}
	for _, record := range s.records {
		if record.ID == deliveryID {
			return record, nil
		}
	}
	return storage.DeliveryRecord{}, storage.ErrNotFound
}

// DeliveryStats returns the preconfigured aggregates.
func (s *fakeAdminStore) DeliveryStats(context.Context, string, time.Time, time.Time) (storage.DeliveryStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWith != nil {
		return storage.DeliveryStats{}, s.failWith
	}
	return s.stats, nil
}

// ResendFailed resets a failed record.
func (s *fakeAdminStore) ResendFailed(_ context.Context, deliveryID string, now time.Time) (storage.DeliveryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWith != nil {
		return storage.DeliveryRecord{}, s.failWith
	}
	for i := range s.records {
		if s.records[i].ID != deliveryID || s.records[i].Status != storage.DeliveryFailed {
			continue
		}
		s.records[i].Status = storage.DeliveryPending
		s.records[i].RetryAttempts = 0
		s.records[i].NextAttemptAt = &now
		s.records[i].LastError = ""
		s.resends++
		return s.records[i], nil
	}
	return storage.DeliveryRecord{}, storage.ErrNotFound
}

// adminRecord builds a delivery journal row.
func adminRecord(id, status string, attempts, retryAttempts int) storage.DeliveryRecord {
	return storage.DeliveryRecord{
		ID:            id,
		EventID:       "evt_" + id,
		SourceEventID: "01a154e0-0000-7000-8000-000000000901",
		MerchantID:    adminMerchantID,
		EndpointID:    "01a154e0-0000-7000-8000-000000000101",
		EventType:     "invoice.completed",
		Status:        status,
		Attempts:      attempts,
		RetryAttempts: retryAttempts,
		Body:          []byte(`{"id":"evt_x"}`),
		CreatedAt:     time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC),
	}
}

func adminClock() func() time.Time {
	return func() time.Time { return time.Date(2026, 10, 19, 13, 0, 0, 0, time.UTC) }
}

func TestAdminResendFailedResetsTheScheduleRound(t *testing.T) {
	store := &fakeAdminStore{records: []storage.DeliveryRecord{adminRecord("01a154e0-0000-7000-8000-000000000201", storage.DeliveryFailed, 7, 7)}}
	service := usecase.NewAdminService(store, usecase.WithAdminClock(adminClock()))

	result, err := service.Resend(context.Background(), "01a154e0-0000-7000-8000-000000000201", "key-1")
	require.NoError(t, err)
	assert.Equal(t, storage.DeliveryPending, result.Delivery.Status)
	assert.Equal(t, 7, result.Delivery.Attempts, "the total performed attempts stay monotonic")
	assert.Equal(t, 0, result.Delivery.RetryAttempts, "the schedule round restarts")
	assert.Equal(t, "2026-10-19T13:00:00Z", result.Delivery.NextAttemptAt.Format(time.RFC3339))
	assert.Empty(t, result.Delivery.LastError)
	assert.Equal(t, 1, store.resends)
}

func TestAdminResendPendingIsANoOp(t *testing.T) {
	store := &fakeAdminStore{records: []storage.DeliveryRecord{adminRecord("01a154e0-0000-7000-8000-000000000201", storage.DeliveryPending, 2, 2)}}
	service := usecase.NewAdminService(store, usecase.WithAdminClock(adminClock()))

	result, err := service.Resend(context.Background(), "01a154e0-0000-7000-8000-000000000201", "key-1")
	require.NoError(t, err)
	assert.Equal(t, storage.DeliveryPending, result.Delivery.Status)
	assert.Equal(t, 2, result.Delivery.Attempts)
	assert.Equal(t, 2, result.Delivery.RetryAttempts)
	assert.Equal(t, 0, store.resends, "a repeated resend cannot duplicate the send")
}

func TestAdminResendDeliveredIsRejected(t *testing.T) {
	store := &fakeAdminStore{records: []storage.DeliveryRecord{adminRecord("01a154e0-0000-7000-8000-000000000201", storage.DeliveryDelivered, 1, 1)}}
	service := usecase.NewAdminService(store, usecase.WithAdminClock(adminClock()))

	_, err := service.Resend(context.Background(), "01a154e0-0000-7000-8000-000000000201", "key-1")
	require.Error(t, err)
	assert.Equal(t, "delivery_not_resendable", grpcmw.CodeOf(err))
	assert.Equal(t, 0, store.resends)
}

func TestAdminResendUnknownIsNotFound(t *testing.T) {
	store := &fakeAdminStore{}
	service := usecase.NewAdminService(store, usecase.WithAdminClock(adminClock()))

	_, err := service.Resend(context.Background(), "01a154e0-0000-7000-8000-000000000299", "key-1")
	require.Error(t, err)
	assert.Equal(t, "delivery_not_found", grpcmw.CodeOf(err))
}

func TestAdminResendValidatesTheIdentity(t *testing.T) {
	service := usecase.NewAdminService(&fakeAdminStore{}, usecase.WithAdminClock(adminClock()))

	_, err := service.Resend(context.Background(), "", "key-1")
	require.Error(t, err)
	assert.Equal(t, "invalid_request", grpcmw.CodeOf(err))

	_, err = service.Resend(context.Background(), "01a154e0-0000-7000-8000-000000000201", "")
	require.Error(t, err)
	assert.Equal(t, "invalid_request", grpcmw.CodeOf(err))
}

func TestAdminListDefaultsAndBounds(t *testing.T) {
	store := &fakeAdminStore{records: []storage.DeliveryRecord{
		adminRecord("01a154e0-0000-7000-8000-000000000201", storage.DeliveryFailed, 7, 7),
		adminRecord("01a154e0-0000-7000-8000-000000000202", storage.DeliveryFailed, 7, 7),
		adminRecord("01a154e0-0000-7000-8000-000000000203", storage.DeliveryFailed, 7, 7),
	}}
	service := usecase.NewAdminService(store, usecase.WithAdminClock(adminClock()))

	page, err := service.List(context.Background(), usecase.ListQuery{MerchantID: adminMerchantID, PageSize: 2})
	require.NoError(t, err)
	require.Len(t, page.Deliveries, 2)
	assert.NotEmpty(t, page.NextToken)
	require.Len(t, store.queried, 1)
	assert.Equal(t, 3, store.queried[0].Limit, "the page fetches page size + 1 to detect the next page")

	_, err = service.List(context.Background(), usecase.ListQuery{MerchantID: adminMerchantID, PageSize: 101})
	require.Error(t, err)
	assert.Equal(t, "invalid_request", grpcmw.CodeOf(err))

	_, err = service.List(context.Background(), usecase.ListQuery{MerchantID: adminMerchantID, PageSize: -1})
	require.Error(t, err)
	assert.Equal(t, "invalid_request", grpcmw.CodeOf(err))
}

func TestAdminListValidatesTheFilters(t *testing.T) {
	service := usecase.NewAdminService(&fakeAdminStore{}, usecase.WithAdminClock(adminClock()))

	cases := []struct {
		name  string
		query usecase.ListQuery
	}{
		{"empty merchant", usecase.ListQuery{}},
		{"bad status", usecase.ListQuery{MerchantID: adminMerchantID, Status: "sent"}},
		{"bad event type", usecase.ListQuery{MerchantID: adminMerchantID, EventType: "payment.completed"}},
		{"bad cursor", usecase.ListQuery{MerchantID: adminMerchantID, PageToken: "not-a-cursor"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.List(context.Background(), tc.query)
			require.Error(t, err)
			assert.Equal(t, "invalid_request", grpcmw.CodeOf(err))
		})
	}
}

func TestAdminCursorRoundtrip(t *testing.T) {
	at := time.Date(2026, 10, 19, 12, 34, 56, 789, time.UTC)
	token := usecase.EncodeDeliveryCursor(usecase.DeliveryCursor{CreatedAt: at, ID: "01a154e0-0000-7000-8000-000000000203"})
	cursor, err := usecase.DecodeDeliveryCursor(token)
	require.NoError(t, err)
	assert.True(t, at.Equal(cursor.CreatedAt))
	assert.Equal(t, "01a154e0-0000-7000-8000-000000000203", cursor.ID)

	store := &fakeAdminStore{}
	service := usecase.NewAdminService(store, usecase.WithAdminClock(adminClock()))
	_, err = service.List(context.Background(), usecase.ListQuery{MerchantID: adminMerchantID, PageToken: token})
	require.NoError(t, err)
	require.Len(t, store.queried, 1)
	require.NotNil(t, store.queried[0].CursorCreatedAt)
	assert.True(t, at.Equal(*store.queried[0].CursorCreatedAt))
	assert.Equal(t, "01a154e0-0000-7000-8000-000000000203", store.queried[0].CursorID)
}

func TestAdminWindowValidation(t *testing.T) {
	service := usecase.NewAdminService(&fakeAdminStore{}, usecase.WithAdminClock(adminClock()))
	from := time.Date(2026, 10, 19, 13, 0, 0, 0, time.UTC)
	to := from.Add(-time.Hour)
	_, err := service.List(context.Background(), usecase.ListQuery{MerchantID: adminMerchantID, From: &from, To: &to})
	require.Error(t, err)
	assert.Equal(t, "invalid_request", grpcmw.CodeOf(err))

	wideFrom := to.Add(-91 * 24 * time.Hour)
	_, err = service.Stats(context.Background(), adminMerchantID, &wideFrom, &to)
	require.Error(t, err)
	assert.Equal(t, "invalid_request", grpcmw.CodeOf(err))
}

func TestAdminStatsDefaultsAndRate(t *testing.T) {
	store := &fakeAdminStore{stats: storage.DeliveryStats{Pending: 2, Delivered: 3, Failed: 1}}
	service := usecase.NewAdminService(store, usecase.WithAdminClock(adminClock()))

	stats, err := service.Stats(context.Background(), adminMerchantID, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(6), stats.Total)
	assert.Equal(t, int64(2), stats.Pending)
	assert.Equal(t, int64(3), stats.Delivered)
	assert.Equal(t, int64(1), stats.Failed)
	assert.InDelta(t, 0.75, stats.SuccessRate, 1e-9)

	empty := usecase.NewAdminService(&fakeAdminStore{}, usecase.WithAdminClock(adminClock()))
	stats, err = empty.Stats(context.Background(), adminMerchantID, nil, nil)
	require.NoError(t, err)
	assert.Zero(t, stats.SuccessRate, "a delivery-free window has no rate")
}

func TestParseRetrySchedule(t *testing.T) {
	schedule, err := usecase.ParseRetrySchedule("")
	require.NoError(t, err)
	assert.Equal(t, usecase.RetrySchedule, schedule, "the contract default")

	schedule, err = usecase.ParseRetrySchedule("2s, 1m")
	require.NoError(t, err)
	assert.Equal(t, []time.Duration{2 * time.Second, time.Minute}, schedule)

	for _, raw := range []string{"1s,1s", "1m,1s", "-1s", "abc", "1s,x", "0s"} {
		_, err := usecase.ParseRetrySchedule(raw)
		require.Error(t, err, raw)
		assert.Equal(t, "invalid_request", grpcmw.CodeOf(err), raw)
	}

	tooMany := "1s"
	for i := 0; i < 20; i++ {
		tooMany += ",2s"
	}
	_, err = usecase.ParseRetrySchedule(tooMany)
	require.Error(t, err)
	assert.Equal(t, "invalid_request", grpcmw.CodeOf(err))
}
