package minikafka_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	"github.com/j0sh/minikafka/storage/sqlite"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
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
		kafka.Message{Key: []byte("c"), Value: []byte("three")},
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
	if err := reader.SetOffset(1); err != nil {
		t.Fatal(err)
	}
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Offset != 1 || string(msg.Value) != "two" {
		t.Fatalf("unexpected first message: offset=%d value=%q", msg.Offset, msg.Value)
	}
	msg, err = reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Offset != 2 || string(msg.Value) != "three" {
		t.Fatalf("unexpected second message: offset=%d value=%q", msg.Offset, msg.Value)
	}
	// Publishing after draining the initial records forces a new Fetch response.
	if err := writer.WriteMessages(ctx, kafka.Message{Value: []byte("four")}); err != nil {
		t.Fatal(err)
	}
	msg, err = reader.ReadMessage(ctx)
	if err != nil || msg.Offset != 3 || string(msg.Value) != "four" {
		t.Fatalf("message after second fetch: %+v err=%v", msg, err)
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
			topic: {{Partition: 0, Offset: 4}},
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
		&kgo.Record{Topic: topic, Key: []byte("c"), Value: []byte("three")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(b.Addr()),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(1)},
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
			if rec.Offset != int64(len(values)+1) {
				t.Fatalf("unexpected franz offset %d", rec.Offset)
			}
			values = append(values, string(rec.Value))
		}
	}
	if values[0] != "two" || values[1] != "three" {
		t.Fatalf("unexpected franz values: %+v", values)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("four")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	for {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v", errs)
		}
		if records := fetches.Records(); len(records) > 0 {
			if len(records) != 1 || records[0].Offset != 3 || string(records[0].Value) != "four" {
				t.Fatalf("unexpected franz second fetch: %+v", records)
			}
			break
		}
	}
}

func TestE2EMultiplePartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := memory.Open()
	b := startBrokerWithConfig(t, minikafka.Config{
		Store:             store,
		AutoCreateTopics:  true,
		DefaultPartitions: 3,
	})
	client := &kafka.Client{Addr: kafka.TCP(b.Addr())}
	topic := "partitioned_events"
	conn, err := kafka.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	partitions, err := conn.ReadPartitions(topic)
	_ = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 3 {
		t.Fatalf("auto-created metadata partitions = %+v", partitions)
	}

	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Topics) != 1 || len(meta.Topics[0].Partitions) != 3 {
		t.Fatalf("metadata topics = %+v", meta.Topics)
	}
	for i, partition := range meta.Topics[0].Partitions {
		if partition.ID != i || partition.Leader.ID != 1 {
			t.Fatalf("metadata partition %d = %+v", i, partition)
		}
	}

	produce := func(partition int, value string) *kafka.ProduceResponse {
		t.Helper()
		res, err := client.Produce(ctx, &kafka.ProduceRequest{
			Topic:        topic,
			Partition:    partition,
			RequiredAcks: kafka.RequireOne,
			Records: kafka.NewRecordReader(kafka.Record{
				Value: kafka.NewBytes([]byte(value)),
			}),
		})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	if res := produce(1, "p1-zero"); res.Error != nil || res.BaseOffset != 0 {
		t.Fatalf("partition 1 produce = %+v", res)
	}
	if res := produce(0, "p0-zero"); res.Error != nil || res.BaseOffset != 0 {
		t.Fatalf("partition 0 produce = %+v", res)
	}
	// Send directly: kafka.Client rejects unknown partitions while routing.
	wire, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer wire.Close()
	deadline, _ := ctx.Deadline()
	if err := wire.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []int32{-1, 3} {
		req := &produceAPI.Request{Acks: 1, Topics: []produceAPI.RequestTopic{{
			Topic: topic,
			Partitions: []produceAPI.RequestPartition{{Partition: invalid, RecordSet: protocol.RecordSet{
				Records: kafka.NewRecordReader(kafka.Record{Value: kafka.NewBytes([]byte("invalid"))}),
			}}},
		}}}
		req.Prepare(8)
		if err := protocol.WriteRequest(wire, 8, 42, "invalid-partition-test", req); err != nil {
			t.Fatal(err)
		}
		correlation, msg, err := protocol.ReadResponse(wire, protocol.Produce, 8)
		if err != nil {
			t.Fatal(err)
		}
		res := msg.(*produceAPI.Response)
		if correlation != 42 || len(res.Topics) != 1 || res.Topics[0].Topic != topic || len(res.Topics[0].Partitions) != 1 {
			t.Fatalf("invalid partition Produce response: correlation=%d response=%+v", correlation, res)
		}
		part := res.Topics[0].Partitions[0]
		if part.Partition != invalid || part.ErrorCode != int16(kafka.UnknownTopicOrPartition) || part.BaseOffset != -1 {
			t.Fatalf("invalid partition %d Produce response: %+v", invalid, part)
		}
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{b.Addr()},
		Topic:     topic,
		Partition: 1,
		MinBytes:  1,
		MaxBytes:  1 << 20,
		MaxWait:   time.Second,
	})
	defer reader.Close()
	first, err := reader.ReadMessage(ctx)
	if err != nil || first.Offset != 0 || string(first.Value) != "p1-zero" {
		t.Fatalf("first partition 1 fetch = %+v, err=%v", first, err)
	}
	readResult := make(chan kafka.Message, 1)
	readErr := make(chan error, 1)
	go func() {
		msg, err := reader.ReadMessage(ctx)
		readResult <- msg
		readErr <- err
	}()
	if offset, err := b.PublishToPartition(ctx, topic, 1, nil, []byte("p1-one")); err != nil || offset != 1 {
		t.Fatalf("PublishToPartition offset=%d err=%v", offset, err)
	}
	if msg, err := <-readResult, <-readErr; err != nil || msg.Offset != 1 || string(msg.Value) != "p1-one" {
		t.Fatalf("woken partition 1 fetch = %+v, err=%v", msg, err)
	}

	offsets, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{
		topic: {kafka.LastOffsetOf(0), kafka.LastOffsetOf(1), kafka.LastOffsetOf(2), kafka.LastOffsetOf(3)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	wantLatest := map[int]int64{0: 1, 1: 2, 2: 0}
	for _, partition := range offsets.Topics[topic] {
		if partition.Partition == 3 {
			if !errors.Is(partition.Error, kafka.UnknownTopicOrPartition) {
				t.Fatalf("invalid partition list offset error = %v", partition.Error)
			}
			continue
		}
		if partition.Error != nil || partition.LastOffset != wantLatest[partition.Partition] {
			t.Fatalf("partition offsets = %+v", offsets.Topics[topic])
		}
	}

	commit, err := client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		GroupID: "partitioned-group",
		Topics: map[string][]kafka.OffsetCommit{
			topic: {{Partition: 0, Offset: 4}, {Partition: 1, Offset: 5}, {Partition: 3, Offset: 6}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, partition := range commit.Topics[topic] {
		if partition.Partition == 3 {
			if !errors.Is(partition.Error, kafka.UnknownTopicOrPartition) {
				t.Fatalf("invalid partition commit error = %v", partition.Error)
			}
		} else if partition.Error != nil {
			t.Fatalf("partition %d commit error = %v", partition.Partition, partition.Error)
		}
	}

	fetched, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: "partitioned-group",
		Topics:  map[string][]int{topic: {0, 1, 2, 3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantCommitted := map[int]int64{0: 4, 1: 5, 2: -1}
	for _, partition := range fetched.Topics[topic] {
		if partition.Partition == 3 {
			if !errors.Is(partition.Error, kafka.UnknownTopicOrPartition) {
				t.Fatalf("invalid partition fetch offset error = %v", partition.Error)
			}
			continue
		}
		if partition.Error != nil || partition.CommittedOffset != wantCommitted[partition.Partition] {
			t.Fatalf("fetched offsets = %+v", fetched.Topics[topic])
		}
	}

	helperTopic := "publish_helpers"
	if err := b.CreateTopic(ctx, helperTopic, minikafka.TopicOptions{Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	key := []byte("stable-key")
	expectedPartition := int32((kafka.Murmur2Balancer{}).Balance(kafka.Message{Key: key}, 0, 1, 2))
	if offset, err := b.Publish(ctx, helperTopic, key, []byte("hashed")); err != nil || offset != 0 {
		t.Fatalf("Publish offset=%d err=%v", offset, err)
	}
	if got, err := store.Fetch(ctx, minikafka.FetchRequest{Topic: helperTopic, Partition: expectedPartition}); err != nil || len(got.Records) != 1 || string(got.Records[0].Value) != "hashed" {
		t.Fatalf("hashed publish partition=%d fetch=%+v err=%v", expectedPartition, got, err)
	}
	if err := b.ResetConsumerOffset(ctx, "helper-group", helperTopic, 2, 11); err != nil {
		t.Fatal(err)
	}
	if got, err := store.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "helper-group", Topic: helperTopic, Partition: 2}); err != nil || !got.Found || got.Offset != 11 {
		t.Fatalf("reset partitioned offset = %+v, err=%v", got, err)
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
	if err := writer.WriteMessages(ctx, kafka.Message{Value: []byte("kept")}, kafka.Message{Value: []byte("also-kept")}); err != nil {
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
	reopenedBroker := startBrokerWithStore(t, reopenedStore)
	// A protocol round trip completes only after Serve has initialized the store.
	// Open binds the listener, but direct store reads still need initialization.
	reopenedClient := &kafka.Client{Addr: kafka.TCP(reopenedBroker.Addr())}
	if _, err := reopenedClient.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}}); err != nil {
		t.Fatal(err)
	}
	latest, err := reopenedStore.LatestOffset(ctx, topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if latest != 2 {
		t.Fatalf("latest after broker restart = %d", latest)
	}
	got, err := reopenedStore.Fetch(ctx, minikafka.FetchRequest{Topic: topic, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 2 || string(got.Records[0].Value) != "kept" {
		t.Fatalf("records after restart = %+v", got.Records)
	}
	off, err := reopenedStore.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "restart-group", Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	if !off.Found || off.Offset != 1 {
		t.Fatalf("committed offset after restart = %+v", off)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{reopenedBroker.Addr()}, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 50 * time.Millisecond})
	defer reader.Close()
	if err := reader.SetOffset(off.Offset); err != nil {
		t.Fatal(err)
	}
	msg, err := reader.ReadMessage(ctx)
	if err != nil || msg.Offset != 1 || string(msg.Value) != "also-kept" {
		t.Fatalf("wire fetch after restart: %+v err=%v", msg, err)
	}
	topicDir := root[:len(root)-len(filepath.Ext(root))]
	if matches, err := filepath.Glob(filepath.Join(topicDir, "minikafka_"+topic+"_0.db")); err != nil || len(matches) != 1 {
		t.Fatalf("topic sqlite file matches=%v err=%v", matches, err)
	}
}

func startBroker(t *testing.T) *minikafka.Broker {
	t.Helper()
	return startBrokerWithStore(t, memory.Open())
}

func startBrokerWithStore(t *testing.T, store minikafka.Store) *minikafka.Broker {
	t.Helper()
	return startBrokerWithConfig(t, minikafka.Config{Store: store, AutoCreateTopics: true})
}

func startBrokerWithConfig(t *testing.T, cfg minikafka.Config) *minikafka.Broker {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	b, err := minikafka.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		if err := b.Close(); err != nil {
			t.Errorf("close broker: %v", err)
		}
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("serve broker: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("broker did not stop")
		}
	})
	go func() { errCh <- b.Serve(ctx) }()
	return b
}
