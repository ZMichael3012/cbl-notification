package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/cbl/cbl-kit/money"
)

// ErrInvalidEvent reports an unmappable or malformed platform event.
var ErrInvalidEvent = errors.New("notification: invalid event")

// Asset describes a CF-3 decimal amount (the asset units, never a float).
type Asset struct {
	// Amount sets the decimal string in asset units.
	Amount string `json:"amount"`
	// Currency sets the asset code.
	Currency string `json:"currency"`
}

// InvoicePayload describes the CF-3 invoice object.
type InvoicePayload struct {
	// ID sets the payment identifier.
	ID string `json:"id"`
	// Status sets the invoice state corresponding to the event type.
	Status string `json:"status"`
	// Amount sets the expected amount.
	Amount *Asset `json:"amount,omitempty"`
	// PaidAmount sets the received amount.
	PaidAmount *Asset `json:"paid_amount,omitempty"`
	// Fee sets the fee amount.
	Fee *Asset `json:"fee,omitempty"`
	// Rate sets the locked quote rate.
	Rate string `json:"rate,omitempty"`
	// TXID sets the on-chain transaction identifier.
	TXID string `json:"txid,omitempty"`
	// OccurredAt sets the event time.
	OccurredAt time.Time `json:"occurred_at"`
}

// PayoutPayload describes the CF-3 payout object.
type PayoutPayload struct {
	// ID sets the payout identifier.
	ID string `json:"id"`
	// Status sets the terminal state (completed, failed, compensated, rejected).
	Status string `json:"status"`
	// Amount sets the payout amount.
	Amount *Asset `json:"amount,omitempty"`
	// Fee sets the payout fee.
	Fee *Asset `json:"fee,omitempty"`
	// TXID sets the on-chain transaction identifier.
	TXID string `json:"txid,omitempty"`
	// FailureReason sets the failure or compensation reason.
	FailureReason string `json:"failure_reason,omitempty"`
	// OccurredAt sets the event time.
	OccurredAt time.Time `json:"occurred_at"`
}

// RefundPayload describes the CF-3 refund object.
type RefundPayload struct {
	// ID sets the refund payout identifier.
	ID string `json:"id"`
	// InvoiceID sets the source invoice the refund is made for.
	InvoiceID string `json:"invoice_id"`
	// Status sets the terminal state (completed, failed, compensated, rejected).
	Status string `json:"status"`
	// Amount sets the refund amount.
	Amount *Asset `json:"amount,omitempty"`
	// Fee sets the refund fee.
	Fee *Asset `json:"fee,omitempty"`
	// TXID sets the on-chain transaction identifier.
	TXID string `json:"txid,omitempty"`
	// FailureReason sets the failure or compensation reason.
	FailureReason string `json:"failure_reason,omitempty"`
	// OccurredAt sets the event time.
	OccurredAt time.Time `json:"occurred_at"`
}

// InvoiceEnvelope wraps an invoice payload.
type InvoiceEnvelope struct {
	// Invoice sets the CF-3 invoice object.
	Invoice InvoicePayload `json:"invoice"`
}

// PayoutEnvelope wraps a payout payload.
type PayoutEnvelope struct {
	// Payout sets the CF-3 payout object.
	Payout PayoutPayload `json:"payout"`
}

// RefundEnvelope wraps a refund payload.
type RefundEnvelope struct {
	// Refund sets the CF-3 refund object.
	Refund RefundPayload `json:"refund"`
}

// Envelope is the merchant webhook body of the frozen CF-3 contract.
type Envelope struct {
	// ID sets the evt_<uuidv7> event identifier (unchanged on retries).
	ID string `json:"id"`
	// Type sets the CF-3 event type.
	Type string `json:"type"`
	// CreatedAt sets the event time.
	CreatedAt time.Time `json:"created_at"`
	// Data sets the typed payload family.
	Data any `json:"data"`
}

// Marshal renders the exact delivery body.
func (e Envelope) Marshal() ([]byte, error) {
	body, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("domain: marshal envelope: %w", err)
	}
	return body, nil
}

// NewEventID returns the evt_<uuidv7> delivery event identifier.
func NewEventID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("domain: generate event id: %w", err)
	}
	return "evt_" + id.String(), nil
}

// MerchantEvent describes a mapped merchant delivery.
type MerchantEvent struct {
	// Type sets the CF-3 event type.
	Type string
	// Data sets the typed payload family (InvoiceEnvelope and so on).
	Data any
	// OccurredAt sets the event time.
	OccurredAt time.Time
}

// FromPaymentEvent maps a payment event onto the CF-3 invoice types; ok=false
// means the event produces no webhook (payment.created and unspecified).
func FromPaymentEvent(event *eventsv1.PaymentEvent) (MerchantEvent, bool, error) {
	if event == nil {
		return MerchantEvent{}, false, fmt.Errorf("%w: empty payment event", ErrInvalidEvent)
	}
	var status string
	switch event.GetType() {
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_DETECTED:
		status = "detected"
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CONFIRMING:
		status = "confirming"
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED:
		status = "completed"
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_EXPIRED:
		status = "expired"
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNDERPAID:
		status = "underpaid"
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_OVERPAID:
		status = "overpaid"
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_FAILED:
		status = "failed"
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_CREATED,
		eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNSPECIFIED:
		return MerchantEvent{}, false, nil
	default:
		return MerchantEvent{}, false, fmt.Errorf("%w: payment event type %s", ErrInvalidEvent, event.GetType().String())
	}
	occurred, err := eventTime(event.GetOccurredAt())
	if err != nil {
		return MerchantEvent{}, false, err
	}
	amount, err := assetOf(event.GetExpectedAmount())
	if err != nil {
		return MerchantEvent{}, false, err
	}
	paid, err := assetOf(event.GetReceivedAmount())
	if err != nil {
		return MerchantEvent{}, false, err
	}
	fee, err := assetOf(event.GetFee())
	if err != nil {
		return MerchantEvent{}, false, err
	}
	payload := InvoicePayload{
		ID:         event.GetPaymentId(),
		Status:     status,
		Amount:     amount,
		PaidAmount: paid,
		Fee:        fee,
		Rate:       event.GetRate(),
		TXID:       event.GetTxid(),
		OccurredAt: occurred,
	}
	return MerchantEvent{Type: "invoice." + status, Data: InvoiceEnvelope{Invoice: payload}, OccurredAt: occurred}, true, nil
}

// FromPayoutEvent maps a payout event onto the CF-3 payout/refund types; the
// refund marker switches the family, the terminal failure keeps the exact state
// in the payload status (CBL-003 section 4); ok=false for requested/approved.
func FromPayoutEvent(event *eventsv1.PayoutEvent) (MerchantEvent, bool, error) {
	if event == nil {
		return MerchantEvent{}, false, fmt.Errorf("%w: empty payout event", ErrInvalidEvent)
	}
	var status string
	failed := false
	switch event.GetType() {
	case eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPLETED:
		status = "completed"
	case eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_FAILED:
		status, failed = "failed", true
	case eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPENSATED:
		status, failed = "compensated", true
	case eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_REJECTED:
		status, failed = "rejected", true
	case eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_REQUESTED,
		eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_APPROVED,
		eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_UNSPECIFIED:
		return MerchantEvent{}, false, nil
	default:
		return MerchantEvent{}, false, fmt.Errorf("%w: payout event type %s", ErrInvalidEvent, event.GetType().String())
	}
	occurred, err := eventTime(event.GetOccurredAt())
	if err != nil {
		return MerchantEvent{}, false, err
	}
	amount, err := assetOf(event.GetAmount())
	if err != nil {
		return MerchantEvent{}, false, err
	}
	fee, err := assetOf(event.GetFee())
	if err != nil {
		return MerchantEvent{}, false, err
	}
	if refundInvoiceID := strings.TrimSpace(event.GetRefundInvoiceId()); refundInvoiceID != "" {
		eventType := "refund.completed"
		if failed {
			eventType = "refund.failed"
		}
		payload := RefundPayload{
			ID:            event.GetPayoutId(),
			InvoiceID:     refundInvoiceID,
			Status:        status,
			Amount:        amount,
			Fee:           fee,
			TXID:          event.GetTxid(),
			FailureReason: event.GetFailureReason(),
			OccurredAt:    occurred,
		}
		return MerchantEvent{Type: eventType, Data: RefundEnvelope{Refund: payload}, OccurredAt: occurred}, true, nil
	}
	eventType := "payout.completed"
	if failed {
		eventType = "payout.failed"
	}
	payload := PayoutPayload{
		ID:            event.GetPayoutId(),
		Status:        status,
		Amount:        amount,
		Fee:           fee,
		TXID:          event.GetTxid(),
		FailureReason: event.GetFailureReason(),
		OccurredAt:    occurred,
	}
	return MerchantEvent{Type: eventType, Data: PayoutEnvelope{Payout: payload}, OccurredAt: occurred}, true, nil
}

// eventTime returns the event time or an invalid event error.
func eventTime(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil || !value.IsValid() {
		return time.Time{}, fmt.Errorf("%w: occurred_at", ErrInvalidEvent)
	}
	return value.AsTime().UTC(), nil
}

// assetOf renders an event amount with the kit money validation (the untrusted
// decimals range cannot reach the string rendering); an absent or empty amount
// is omitted (the contract distinguishes "no evidence" from a genuine zero).
func assetOf(value *commonv1.Money) (*Asset, error) {
	if value == nil || strings.TrimSpace(value.GetAsset()) == "" {
		return nil, nil
	}
	if value.GetMinorUnits() < 0 {
		return nil, fmt.Errorf("%w: negative amount", ErrInvalidEvent)
	}
	amount, err := money.New(value.GetMinorUnits(), value.GetAsset(), value.GetDecimals())
	if err != nil {
		return nil, fmt.Errorf("%w: amount: %w", ErrInvalidEvent, err)
	}
	return &Asset{Amount: amount.String(), Currency: amount.Asset}, nil
}
