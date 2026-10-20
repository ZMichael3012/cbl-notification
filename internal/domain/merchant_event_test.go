package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ZMichael3012/cbl-kit/hmac"

	"github.com/cbl/cbl-notification/internal/domain"
)

// moneyValue builds a proto money amount.
func moneyValue(minor int64, asset string, decimals uint32) *commonv1.Money {
	return &commonv1.Money{MinorUnits: minor, Asset: asset, Decimals: decimals}
}

// paymentEvent builds a payment event.
func paymentEvent(eventType eventsv1.PaymentEventType) *eventsv1.PaymentEvent {
	return &eventsv1.PaymentEvent{
		PaymentId:      "01a154e0-0000-7000-8000-000000000001",
		MerchantId:     testMerchantID,
		Type:           eventType,
		ExpectedAmount: moneyValue(60_000000, "USDT", 6),
		ReceivedAmount: moneyValue(60_000000, "USDT", 6),
		Fee:            moneyValue(0, "USDT", 6),
		QuoteId:        "01a154e0-0000-7000-8000-000000000002",
		Rate:           "60000.00",
		Txid:           "tx-1",
		OccurredAt:     timestamppb.New(time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)),
	}
}

func TestPaymentEventMapping(t *testing.T) {
	cases := map[eventsv1.PaymentEventType]string{
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_DETECTED:   "invoice.detected",
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CONFIRMING: "invoice.confirming",
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED:  "invoice.completed",
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_EXPIRED:    "invoice.expired",
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNDERPAID:  "invoice.underpaid",
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_OVERPAID:   "invoice.overpaid",
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_FAILED:     "invoice.failed",
	}
	for eventType, want := range cases {
		mapped, ok, err := domain.FromPaymentEvent(paymentEvent(eventType))
		require.NoError(t, err)
		require.True(t, ok, eventType.String())
		assert.Equal(t, want, mapped.Type)
		assert.Equal(t, time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC), mapped.OccurredAt)
	}

	// CREATED and UNSPECIFIED produce no webhook.
	for _, eventType := range []eventsv1.PaymentEventType{
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CREATED,
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNSPECIFIED,
	} {
		_, ok, err := domain.FromPaymentEvent(paymentEvent(eventType))
		require.NoError(t, err)
		assert.False(t, ok)
	}

	// The invoice payload carries the amounts as decimal strings and the status.
	mapped, _, err := domain.FromPaymentEvent(paymentEvent(eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED))
	require.NoError(t, err)
	body, err := domain.Envelope{ID: "evt_x", Type: mapped.Type, CreatedAt: mapped.OccurredAt, Data: mapped.Data}.Marshal()
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	invoice := decoded["data"].(map[string]any)["invoice"].(map[string]any)
	assert.Equal(t, "01a154e0-0000-7000-8000-000000000001", invoice["id"])
	assert.Equal(t, "completed", invoice["status"])
	assert.Equal(t, "60.000000", invoice["amount"].(map[string]any)["amount"])
	assert.Equal(t, "USDT", invoice["amount"].(map[string]any)["currency"])
	assert.Equal(t, "60000.00", invoice["rate"])
}

// payoutEvent builds a payout event of the type and refund marker.
func payoutEvent(eventType eventsv1.PayoutEventType, refundInvoiceID string) *eventsv1.PayoutEvent {
	return &eventsv1.PayoutEvent{
		PayoutId:        "01a154e0-0000-7000-8000-000000000010",
		MerchantId:      testMerchantID,
		Type:            eventType,
		Amount:          moneyValue(5_000000, "USDT", 6),
		Fee:             moneyValue(1000, "USDT", 6),
		Txid:            "ptx-1",
		RefundInvoiceId: refundInvoiceID,
		OccurredAt:      timestamppb.New(time.Date(2026, 10, 19, 13, 0, 0, 0, time.UTC)),
	}
}

func TestPayoutEventMapping(t *testing.T) {
	cases := []struct {
		eventType eventsv1.PayoutEventType
		wantType  string
		wantState string
	}{
		{eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPLETED, "payout.completed", "completed"},
		{eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_FAILED, "payout.failed", "failed"},
		{eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPENSATED, "payout.failed", "compensated"},
		{eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_REJECTED, "payout.failed", "rejected"},
	}
	for _, tc := range cases {
		event := payoutEvent(tc.eventType, "")
		mapped, ok, err := domain.FromPayoutEvent(event)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, tc.wantType, mapped.Type)
		body, err := domain.Envelope{ID: "evt_x", Type: mapped.Type, CreatedAt: mapped.OccurredAt, Data: mapped.Data}.Marshal()
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(body, &decoded))
		payout := decoded["data"].(map[string]any)["payout"].(map[string]any)
		assert.Equal(t, tc.wantState, payout["status"])
		assert.Equal(t, "5.000000", payout["amount"].(map[string]any)["amount"])
	}

	// REQUESTED/APPROVED and UNSPECIFIED produce no webhook.
	for _, eventType := range []eventsv1.PayoutEventType{
		eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_REQUESTED,
		eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_APPROVED,
		eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_UNSPECIFIED,
	} {
		_, ok, err := domain.FromPayoutEvent(payoutEvent(eventType, ""))
		require.NoError(t, err)
		assert.False(t, ok)
	}

	// The refund marker switches the type and carries the invoice id.
	const refundInvoiceID = "01a154e0-0000-7000-8000-000000000020"
	mapped, ok, err := domain.FromPayoutEvent(payoutEvent(eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPLETED, refundInvoiceID))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "refund.completed", mapped.Type)
	body, err := domain.Envelope{ID: "evt_x", Type: mapped.Type, CreatedAt: mapped.OccurredAt, Data: mapped.Data}.Marshal()
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	refundData := decoded["data"].(map[string]any)["refund"].(map[string]any)
	assert.Equal(t, refundInvoiceID, refundData["invoice_id"])
	assert.Equal(t, "01a154e0-0000-7000-8000-000000000010", refundData["id"])
}

func TestPayoutRefundFailedMapping(t *testing.T) {
	const refundInvoiceID = "01a154e0-0000-7000-8000-000000000020"
	mapped, ok, err := domain.FromPayoutEvent(payoutEvent(eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPENSATED, refundInvoiceID))
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "refund.failed", mapped.Type)
	body, err := domain.Envelope{ID: "evt_x", Type: mapped.Type, CreatedAt: mapped.OccurredAt, Data: mapped.Data}.Marshal()
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	refund := decoded["data"].(map[string]any)["refund"].(map[string]any)
	assert.Equal(t, "compensated", refund["status"])
}

func TestUntrustedAmountValidation(t *testing.T) {
	cases := []*eventsv1.PaymentEvent{
		func() *eventsv1.PaymentEvent {
			e := paymentEvent(eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED)
			e.ExpectedAmount = &commonv1.Money{MinorUnits: 1, Asset: "USDT", Decimals: 100}
			return e
		}(),
		func() *eventsv1.PaymentEvent {
			e := paymentEvent(eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED)
			e.ExpectedAmount = &commonv1.Money{MinorUnits: -5, Asset: "USDT", Decimals: 6}
			return e
		}(),
		func() *eventsv1.PaymentEvent {
			e := paymentEvent(eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED)
			e.ExpectedAmount = &commonv1.Money{MinorUnits: 1, Asset: "USDT", Decimals: 4294967295}
			return e
		}(),
	}
	for i, event := range cases {
		_, _, err := domain.FromPaymentEvent(event)
		require.ErrorIs(t, err, domain.ErrInvalidEvent, "case %d", i)
	}
}

func TestEnvelopeSignature(t *testing.T) {
	secret := []byte("a4029ad5874b8448ea9954bfce640266e7aae888480e8c136b19ff246c417c3a")
	now := time.Date(2026, 10, 19, 14, 0, 0, 0, time.UTC)
	eventID, err := domain.NewEventID()
	require.NoError(t, err)
	assert.Regexp(t, `^evt_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, eventID)

	envelope := domain.Envelope{ID: eventID, Type: "invoice.completed", CreatedAt: now, Data: domain.InvoiceEnvelope{
		Invoice: domain.InvoicePayload{ID: "01a154e0-0000-7000-8000-000000000001", Status: "completed"},
	}}
	body, err := envelope.Marshal()
	require.NoError(t, err)
	assert.Contains(t, string(body), eventID)

	header := hmac.BuildHeader(hmac.Signature{Timestamp: now, Hex: hmac.Sign(secret, now, body)})
	assert.Regexp(t, `^t=[0-9]+,v1=[0-9a-f]{64}$`, header)
	require.NoError(t, hmac.Verify(secret, header, body, now, hmac.DefaultWindow))
	require.Error(t, hmac.Verify(secret, header, body, now.Add(10*time.Minute), hmac.DefaultWindow))
}

func TestAssetOmission(t *testing.T) {
	event := paymentEvent(eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED)
	event.ReceivedAmount = nil
	event.Fee = nil
	event.Rate = ""
	event.Txid = ""
	mapped, _, err := domain.FromPaymentEvent(event)
	require.NoError(t, err)
	body, err := domain.Envelope{ID: "evt_x", Type: mapped.Type, CreatedAt: mapped.OccurredAt, Data: mapped.Data}.Marshal()
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	invoice := decoded["data"].(map[string]any)["invoice"].(map[string]any)
	assert.NotContains(t, invoice, "paid_amount")
	assert.NotContains(t, invoice, "fee")
}
