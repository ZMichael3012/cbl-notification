// Command paymentevent publishes one synthetic payment.completed event to the
// payment.events topic for the CBL-8.2 stand e2e (the header contract of the
// payment producer: event_id, producer, event_type, occurred_at, schema_version).
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	merchantID := os.Getenv("PAYMENT_MERCHANT")
	if merchantID == "" {
		merchantID = "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d"
	}
	paymentID := uuid.Must(uuid.NewV7()).String()
	eventID := uuid.Must(uuid.NewV7()).String()
	now := time.Now().UTC()

	event := &eventsv1.PaymentEvent{
		PaymentId:      paymentID,
		MerchantId:     merchantID,
		Type:           eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED,
		ExpectedAmount: &commonv1.Money{MinorUnits: 60_000000, Asset: "USDT", Decimals: 6},
		ReceivedAmount: &commonv1.Money{MinorUnits: 60_000000, Asset: "USDT", Decimals: 6},
		QuoteId:        uuid.Must(uuid.NewV7()).String(),
		Rate:           "60000.00",
		Txid:           "e2e-tx-82",
		StateVersion:   6,
		OccurredAt:     timestamppb.New(now),
	}
	value, err := proto.Marshal(event)
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal:", err)
		os.Exit(1)
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka client:", err)
		os.Exit(1)
	}
	defer client.Close()
	record := &kgo.Record{
		Topic: "payment.events",
		Key:   []byte(paymentID),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(eventID)},
			{Key: "producer", Value: []byte("cbl-payment")},
			{Key: "event_type", Value: []byte("payment.completed")},
			{Key: "occurred_at", Value: []byte(now.Format(time.RFC3339Nano))},
			{Key: "schema_version", Value: []byte("v1")},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
		fmt.Fprintln(os.Stderr, "produce:", err)
		os.Exit(1)
	}
	fmt.Printf("payment_id=%s event_id=%s\n", paymentID, eventID)
}
