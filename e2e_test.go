package minikafka_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	"github.com/j0sh/minikafka/storage/sqlite"
	kafka "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestE2ESegmentioKafkaGoProduceConsumeCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := startBroker(t)
	topic := "segmentio_events"

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(b.Addr()),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer writer.Close()
	if err := writer.WriteMessages(ctx,
		kafka.Message{Key: []byte("a"), Value: []byte("one")},
		kafka.Message{Key: []byte("b"), Value: []byte("two")},
	); err != nil {
		t.Fatal(err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{b.Addr()},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1 << 20,
		MaxWait:   50 * time.Millisecond,
	})
	defer reader.Close()
	if err := reader.SetOffset(0); err != nil {
		t.Fatal(err)
	}
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Offset != 0 || string(msg.Value) != "one" {
		t.Fatalf("unexpected first message: offset=%d value=%q", msg.Offset, msg.Value)
	}
	msg, err = reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Offset != 1 || string(msg.Value) != "two" {
		t.Fatalf("unexpected second message: offset=%d value=%q", msg.Offset, msg.Value)
	}

	conn, err := kafka.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 1 || partitions[0].ID != 0 {
		t.Fatalf("metadata partitions = %+v", partitions)
	}
	client := &kafka.Client{Addr: kafka.TCP(b.Addr())}
	if _, err := client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		GroupID: "segmentio-group",
		Topics: map[string][]kafka.OffsetCommit{
			topic: {{Partition: 0, Offset: 2}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	off, err := b.ListTopics(ctx)
	if err != nil || len(off) == 0 {
		t.Fatalf("topic list after e2e = %+v err=%v", off, err)
	}
}

func TestE2EFranzGoProduceConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := startBroker(t)
	topic := "franz_events"

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(b.Addr()),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Key: []byte("a"), Value: []byte("one")},
		&kgo.Record{Topic: topic, Key: []byte("b"), Value: []byte("two")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(b.Addr()),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtStart()},
		}),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var values []string
	for len(values) < 2 {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v", errs)
		}
		for _, rec := range fetches.Records() {
			values = append(values, string(rec.Value))
		}
	}
	if values[0] != "one" || values[1] != "two" {
		t.Fatalf("unexpected franz values: %+v", values)
	}
}

func TestE2ESQLiteRestartPreservesRecordsAndClientCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := filepath.Join(t.TempDir(), "events.db")
	store, err := sqlite.Open(root, sqlite.WithSynchronous(sqlite.SyncFull))
	if err != nil {
		t.Fatal(err)
	}
	b := startBrokerWithStore(t, store)
	topic := "restart_events"

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(b.Addr()),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
	if err := writer.WriteMessages(ctx, kafka.Message{Value: []byte("kept")}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	client := &kafka.Client{Addr: kafka.TCP(b.Addr())}
	if _, err := client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		GroupID: "restart-group",
		Topics: map[string][]kafka.OffsetCommit{
			topic: {{Partition: 0, Offset: 1}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := sqlite.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedStore.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer reopenedStore.Close()
	latest, err := reopenedStore.LatestOffset(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if latest != 1 {
		t.Fatalf("latest after broker restart = %d", latest)
	}
	got, err := reopenedStore.Fetch(ctx, minikafka.FetchRequest{Topic: topic, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 1 || string(got.Records[0].Value) != "kept" {
		t.Fatalf("records after restart = %+v", got.Records)
	}
	off, err := reopenedStore.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "restart-group", Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	if !off.Found || off.Offset != 1 {
		t.Fatalf("committed offset after restart = %+v", off)
	}
	topicDir := root[:len(root)-len(filepath.Ext(root))]
	if matches, err := filepath.Glob(filepath.Join(topicDir, topic+".db")); err != nil || len(matches) != 1 {
		t.Fatalf("topic sqlite file matches=%v err=%v", matches, err)
	}
}

func startBroker(t *testing.T) *minikafka.Broker {
	t.Helper()
	return startBrokerWithStore(t, memory.Open())
}

func startBrokerWithStore(t *testing.T, store minikafka.Store) *minikafka.Broker {
	t.Helper()
	b, err := minikafka.Open(minikafka.Config{
		Addr:             "127.0.0.1:0",
		Store:            store,
		AutoCreateTopics: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = b.Close()
	})
	errCh := make(chan error, 1)
	go func() { errCh <- b.Serve(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for b.Addr() == "127.0.0.1:0" {
		if time.Now().After(deadline) {
			t.Fatal("broker did not start")
		}
		select {
		case err := <-errCh:
			t.Fatalf("broker exited: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	waitTCP(t, b.Addr())
	return b
}

func waitTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker was not reachable at %s: %v", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
