//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-notification/internal/storage"
	"github.com/cbl/cbl-notification/internal/usecase"
)

// uuidN builds a valid UUID for the test fixtures.
func uuidN(n int) string {
	return fmt.Sprintf("01a154e0-0000-7000-8000-%012d", n)
}

func TestIntegrationDeliveryListingPages(t *testing.T) {
	db := setupNotificationDB(t)
	repo := storage.NewEndpointRepository(db)
	deliveries := storage.NewDeliveryRepository(db)
	service := usecase.NewAdminService(deliveries)
	ctx := context.Background()

	merchant := "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d"
	require.NoError(t, repo.Insert(ctx, record(merchant, "key-12345", "hash-1")))
	endpoint, err := repo.ByMerchantKey(ctx, merchant, "key-12345")
	require.NoError(t, err)

	base := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		row := storage.DeliveryRecord{
			ID:            uuidN(200 + i),
			EventID:       "evt_" + uuidN(300+i),
			SourceEventID: uuidN(400 + i),
			MerchantID:    merchant,
			EndpointID:    endpoint.ID,
			EventType:     "invoice.completed",
			Body:          []byte(`{"id":"evt_x"}`),
			Status:        storage.DeliveryPending,
			NextAttemptAt: ptrTime(base),
			CreatedAt:     base.Add(time.Duration(i) * time.Minute),
		}
		require.NoError(t, deliveries.InsertDelivery(ctx, db, row))
	}

	var seen []string
	token := ""
	for page := 0; page < 4; page++ {
		result, err := service.List(ctx, usecase.ListQuery{MerchantID: merchant, PageSize: 2, PageToken: token})
		require.NoError(t, err)
		for _, row := range result.Deliveries {
			seen = append(seen, row.ID)
		}
		token = result.NextToken
		if token == "" {
			break
		}
	}
	expected := []string{uuidN(204), uuidN(203), uuidN(202), uuidN(201), uuidN(200)}
	assert.Equal(t, expected, seen, "created_at DESC without duplicates across pages")
}

func TestIntegrationDeliveryFiltersAndStats(t *testing.T) {
	db := setupNotificationDB(t)
	repo := storage.NewEndpointRepository(db)
	deliveries := storage.NewDeliveryRepository(db)
	service := usecase.NewAdminService(deliveries)
	ctx := context.Background()

	merchant := "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d"
	require.NoError(t, repo.Insert(ctx, record(merchant, "key-12345", "hash-1")))
	endpoint, err := repo.ByMerchantKey(ctx, merchant, "key-12345")
	require.NoError(t, err)

	base := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	rows := []struct {
		status    string
		eventType string
		offset    time.Duration
	}{
		{storage.DeliveryDelivered, "invoice.completed", 0},
		{storage.DeliveryDelivered, "payout.failed", time.Hour},
		{storage.DeliveryFailed, "invoice.completed", 2 * time.Hour},
		{storage.DeliveryPending, "refund.completed", 3 * time.Hour},
		{storage.DeliveryFailed, "invoice.completed", 72 * time.Hour},
	}
	for i, spec := range rows {
		row := storage.DeliveryRecord{
			ID:            uuidN(500 + i),
			EventID:       "evt_" + uuidN(600+i),
			SourceEventID: uuidN(700 + i),
			MerchantID:    merchant,
			EndpointID:    endpoint.ID,
			EventType:     spec.eventType,
			Body:          []byte(`{"id":"evt_x"}`),
			Status:        spec.status,
			NextAttemptAt: ptrTime(base.Add(spec.offset)),
			CreatedAt:     base.Add(spec.offset),
		}
		require.NoError(t, deliveries.InsertDelivery(ctx, db, row))
	}

	from := base
	to := base.Add(24 * time.Hour)
	page, err := service.List(ctx, usecase.ListQuery{
		MerchantID: merchant, Status: storage.DeliveryFailed, EventType: "invoice.completed", From: &from, To: &to,
	})
	require.NoError(t, err)
	require.Len(t, page.Deliveries, 1)
	assert.Equal(t, uuidN(502), page.Deliveries[0].ID)

	stats, err := service.Stats(ctx, merchant, &from, &to)
	require.NoError(t, err)
	assert.Equal(t, int64(4), stats.Total, "the 72h row is outside the window")
	assert.Equal(t, int64(2), stats.Delivered)
	assert.Equal(t, int64(1), stats.Failed)
	assert.Equal(t, int64(1), stats.Pending)
	assert.InDelta(t, 2.0/3.0, stats.SuccessRate, 1e-9)
}

func TestIntegrationResendResetsTheRoundAndKeepsTheJournal(t *testing.T) {
	db := setupNotificationDB(t)
	repo := storage.NewEndpointRepository(db)
	deliveries := storage.NewDeliveryRepository(db)
	service := usecase.NewAdminService(deliveries)
	ctx := context.Background()

	merchant := "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d"
	require.NoError(t, repo.Insert(ctx, record(merchant, "key-12345", "hash-1")))
	endpoint, err := repo.ByMerchantKey(ctx, merchant, "key-12345")
	require.NoError(t, err)

	base := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	deliveryID := uuidN(800)
	exhausted := storage.DeliveryRecord{
		ID: deliveryID, EventID: "evt_" + uuidN(801), SourceEventID: uuidN(802),
		MerchantID: merchant, EndpointID: endpoint.ID, EventType: "invoice.completed",
		Body: []byte(`{"id":"evt_x"}`), Status: storage.DeliveryFailed, Attempts: 7, RetryAttempts: 7,
		LastError: "unreachable",
	}
	require.NoError(t, deliveries.InsertDelivery(ctx, db, exhausted))
	for attempt := 1; attempt <= 7; attempt++ {
		require.NoError(t, deliveries.RecordAttempt(ctx, storage.AttemptRecord{
			DeliveryID: deliveryID, Attempt: attempt, SentAt: base.Add(time.Duration(attempt) * time.Second),
			HTTPStatus: 0, Error: "dial tcp: connection refused",
		}))
	}

	result, err := service.Resend(ctx, deliveryID, "resend-key-1")
	require.NoError(t, err)
	assert.Equal(t, storage.DeliveryPending, result.Delivery.Status)
	assert.Equal(t, 7, result.Delivery.Attempts)
	assert.Equal(t, 0, result.Delivery.RetryAttempts)
	assert.Empty(t, result.Delivery.LastError)
	require.NotNil(t, result.Delivery.NextAttemptAt)

	// The journal is append-only and the attempt numbering continues.
	require.NoError(t, deliveries.RecordAttempt(ctx, storage.AttemptRecord{
		DeliveryID: deliveryID, Attempt: 8, SentAt: *result.Delivery.NextAttemptAt, HTTPStatus: 0,
	}))
	require.NoError(t, deliveries.ScheduleRetry(ctx, deliveryID, 8, 1, result.Delivery.NextAttemptAt.Add(time.Second), "still down"))
	claimed, err := deliveries.ClaimDue(ctx, result.Delivery.NextAttemptAt.Add(2*time.Second), 15*time.Second, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, 8, claimed[0].Attempts)
	assert.Equal(t, 1, claimed[0].RetryAttempts)

	var journal int64
	require.NoError(t, db.Raw("SELECT count(*) FROM webhook_delivery_attempts WHERE delivery_id = ?", deliveryID).Scan(&journal).Error)
	assert.Equal(t, int64(8), journal, "the historical attempts are preserved")

	// A concurrent repeated resend cannot reset twice.
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := service.Resend(ctx, deliveryID, "resend-key-2")
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	var pending int64
	require.NoError(t, db.Raw("SELECT count(*) FROM webhook_deliveries WHERE id = ? AND status = 'pending' AND retry_attempts <= 1", deliveryID).Scan(&pending).Error)
	assert.Equal(t, int64(1), pending)
}

// ptrTime returns a pointer to the time.
func ptrTime(at time.Time) *time.Time { return &at }
