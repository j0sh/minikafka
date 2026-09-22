package minikafka_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	"github.com/j0sh/minikafka/storage/sqlite"
)

func TestStoresCoreSemantics(t *testing.T) {
	tests := []struct {
		name string
		open func(t *testing.T) minikafka.Store
	}{
		{name: "memory", open: func(t *testing.T) minikafka.Store { return memory.Open() }},
		{name: "sqlite", open: func(t *testing.T) minikafka.Store {
			store, err := sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := tt.open(t)
			if err := store.Init(ctx); err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			if err := store.CreateTopic(ctx, "events", minikafka.TopicOptions{Retention: minikafka.RetentionPolicy{MaxMessages: 2}}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Topic(ctx, "events"); err != nil {
				t.Fatal(err)
			}
			app, err := store.Append(ctx, minikafka.AppendRequest{Topic: "events", Records: []minikafka.Record{
				{Timestamp: time.Now(), Key: []byte("k1"), Value: []byte("one")},
				{Timestamp: time.Now(), Key: []byte("k2"), Value: []byte("two")},
				{Timestamp: time.Now(), Key: []byte("k3"), Value: []byte("three")},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if app.BaseOffset != 0 || app.LastOffset != 2 {
				t.Fatalf("unexpected append offsets: %+v", app)
			}
			got, err := store.Fetch(ctx, minikafka.FetchRequest{Topic: "events", Offset: 1, MaxBytes: 1, MaxRecords: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Records) == 0 || string(got.Records[0].Value) != "two" {
				t.Fatalf("unexpected fetch: %+v", got.Records)
			}
			if got.HighWatermark != 3 || got.EarliestOffset != 0 || got.LatestOffset != 3 {
				t.Fatalf("bad watermarks: %+v", got)
			}
			if err := store.CommitOffset(ctx, minikafka.CommitOffsetRequest{GroupID: "g", Topic: "events", Offset: 2}); err != nil {
				t.Fatal(err)
			}
			off, err := store.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "g", Topic: "events"})
			if err != nil {
				t.Fatal(err)
			}
			if !off.Found || off.Offset != 2 {
				t.Fatalf("bad committed offset: %+v", off)
			}
			if err := store.ApplyRetention(ctx, "events"); err != nil {
				t.Fatal(err)
			}
			earliest, err := store.EarliestOffset(ctx, "events", 0)
			if err != nil {
				t.Fatal(err)
			}
			if earliest != 1 {
				t.Fatalf("retention should preserve offsets and drop offset 0, earliest=%d", earliest)
			}
			if _, err := store.Fetch(ctx, minikafka.FetchRequest{Topic: "events", Offset: 0}); !errors.Is(err, minikafka.ErrOffsetOutOfRange) {
				t.Fatalf("fetch before earliest error = %v", err)
			}
			t.Run("age_retention_gaps", func(t *testing.T) {
				testStoreFetchAfterAgeRetention(t, store)
			})
		})
	}
}

func TestStoresPartitionSemantics(t *testing.T) {
	tests := []struct {
		name string
		open func(t *testing.T) minikafka.Store
	}{
		{name: "memory", open: func(t *testing.T) minikafka.Store { return memory.Open() }},
		{name: "sqlite", open: func(t *testing.T) minikafka.Store {
			store, err := sqlite.Open(filepath.Join(t.TempDir(), "partitions.db"))
			if err != nil {
				t.Fatal(err)
			}
			return store
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := tt.open(t)
			if err := store.Init(ctx); err != nil {
				t.Fatal(err)
			}
			defer store.Close()

			if err := store.CreateTopic(ctx, "default", minikafka.TopicOptions{}); err != nil {
				t.Fatal(err)
			}
			meta, err := store.Topic(ctx, "default")
			if err != nil || meta.Partitions != 1 {
				t.Fatalf("default topic metadata = %+v, err=%v", meta, err)
			}
			if err := store.CreateTopic(ctx, "invalid", minikafka.TopicOptions{Partitions: -1}); !errors.Is(err, minikafka.ErrInvalidPartition) {
				t.Fatalf("negative partition count error = %v", err)
			}

			opts := minikafka.TopicOptions{Partitions: 2, Retention: minikafka.RetentionPolicy{MaxMessages: 1}}
			if err := store.CreateTopic(ctx, "events_partitioned", opts); err != nil {
				t.Fatal(err)
			}
			meta, err = store.Topic(ctx, "events_partitioned")
			if err != nil || meta.Partitions != 2 {
				t.Fatalf("partitioned topic metadata = %+v, err=%v", meta, err)
			}

			p0, err := store.Append(ctx, minikafka.AppendRequest{Topic: meta.Topic, Partition: 0, Records: []minikafka.Record{{Value: []byte("p0-zero")}, {Value: []byte("p0-one")}}})
			if err != nil || p0.BaseOffset != 0 || p0.LastOffset != 1 {
				t.Fatalf("partition 0 append = %+v, err=%v", p0, err)
			}
			p1, err := store.Append(ctx, minikafka.AppendRequest{Topic: meta.Topic, Partition: 1, Records: []minikafka.Record{{Value: []byte("p1-zero")}}})
			if err != nil || p1.BaseOffset != 0 || p1.LastOffset != 0 {
				t.Fatalf("partition 1 append = %+v, err=%v", p1, err)
			}
			for partition, want := range map[int32]string{0: "p0-zero", 1: "p1-zero"} {
				got, err := store.Fetch(ctx, minikafka.FetchRequest{Topic: meta.Topic, Partition: partition})
				if err != nil || len(got.Records) == 0 || string(got.Records[0].Value) != want {
					t.Fatalf("partition %d fetch = %+v, err=%v", partition, got, err)
				}
			}

			for partition, offset := range map[int32]int64{0: 7, 1: 9} {
				if err := store.CommitOffset(ctx, minikafka.CommitOffsetRequest{GroupID: "g", Topic: meta.Topic, Partition: partition, Offset: offset}); err != nil {
					t.Fatal(err)
				}
				got, err := store.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "g", Topic: meta.Topic, Partition: partition})
				if err != nil || !got.Found || got.Offset != offset {
					t.Fatalf("partition %d committed offset = %+v, err=%v", partition, got, err)
				}
			}

			if err := store.ApplyRetention(ctx, meta.Topic); err != nil {
				t.Fatal(err)
			}
			if earliest, err := store.EarliestOffset(ctx, meta.Topic, 0); err != nil || earliest != 1 {
				t.Fatalf("partition 0 earliest after retention = %d, err=%v", earliest, err)
			}
			if earliest, err := store.EarliestOffset(ctx, meta.Topic, 1); err != nil || earliest != 0 {
				t.Fatalf("partition 1 earliest after retention = %d, err=%v", earliest, err)
			}

			invalidOps := []func() error{
				func() error {
					_, err := store.Append(ctx, minikafka.AppendRequest{Topic: meta.Topic, Partition: 2})
					return err
				},
				func() error {
					_, err := store.Fetch(ctx, minikafka.FetchRequest{Topic: meta.Topic, Partition: -1})
					return err
				},
				func() error { _, err := store.EarliestOffset(ctx, meta.Topic, 2); return err },
				func() error { _, err := store.LatestOffset(ctx, meta.Topic, -1); return err },
				func() error {
					return store.CommitOffset(ctx, minikafka.CommitOffsetRequest{GroupID: "g", Topic: meta.Topic, Partition: 2})
				},
				func() error {
					_, err := store.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "g", Topic: meta.Topic, Partition: 2})
					return err
				},
			}
			for i, op := range invalidOps {
				if err := op(); !errors.Is(err, minikafka.ErrInvalidPartition) {
					t.Fatalf("invalid partition operation %d error = %v", i, err)
				}
			}

			if err := store.DeleteTopic(ctx, meta.Topic); err != nil {
				t.Fatal(err)
			}
			if err := store.CreateTopic(ctx, meta.Topic, minikafka.TopicOptions{Partitions: 2}); err != nil {
				t.Fatal(err)
			}
			for partition := range int32(2) {
				if latest, err := store.LatestOffset(ctx, meta.Topic, partition); err != nil || latest != 0 {
					t.Fatalf("partition %d latest after recreation = %d, err=%v", partition, latest, err)
				}
				got, err := store.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "g", Topic: meta.Topic, Partition: partition})
				if err != nil || got.Found {
					t.Fatalf("partition %d offset survived recreation: %+v, err=%v", partition, got, err)
				}
			}
		})
	}
}

func testStoreFetchAfterAgeRetention(t *testing.T, store minikafka.Store) {
	t.Helper()
	ctx := context.Background()
	const topic = "age_retention"
	if err := store.CreateTopic(ctx, topic, minikafka.TopicOptions{Retention: minikafka.RetentionPolicy{MaxAge: time.Hour}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := store.Append(ctx, minikafka.AppendRequest{Topic: topic, Records: []minikafka.Record{
		{Timestamp: now, Value: []byte("zero")},
		{Timestamp: now, Value: []byte("one")},
		{Timestamp: now.Add(-2 * time.Hour), Value: []byte("expired")},
		{Timestamp: now, Value: []byte("three")},
		{Timestamp: now, Value: []byte("four")},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyRetention(ctx, topic); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		offset int64
		want   []int64
	}{
		{offset: 0, want: []int64{0, 1}},
		{offset: 2, want: []int64{3, 4}},
		{offset: 3, want: []int64{3, 4}},
		{offset: 5},
	} {
		got, err := store.Fetch(ctx, minikafka.FetchRequest{Topic: topic, Offset: tc.offset})
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Records) != len(tc.want) {
			t.Fatalf("fetch at %d: records = %+v, want offsets %v", tc.offset, got.Records, tc.want)
		}
		for i, rec := range got.Records {
			if rec.Offset != tc.want[i] {
				t.Fatalf("fetch at %d: offset = %d, want %d", tc.offset, rec.Offset, tc.want[i])
			}
		}
		if got.HighWatermark != 5 || got.LatestOffset != 5 || got.EarliestOffset != 0 {
			t.Fatalf("fetch at %d: incorrect watermarks: %+v", tc.offset, got)
		}
	}
}

func TestStoreConcurrentAppendOffsets(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var store minikafka.Store = memory.Open()
			if backend == "sqlite" {
				var err error
				store, err = sqlite.Open(filepath.Join(t.TempDir(), "events.db"))
				if err != nil {
					t.Fatal(err)
				}
			}
			defer store.Close()
			if err := store.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := store.CreateTopic(ctx, "events", minikafka.TopicOptions{}); err != nil {
				t.Fatal(err)
			}
			const n = 64
			var wg sync.WaitGroup
			start := make(chan struct{})
			errs := make(chan error, n)
			offsets := make(chan int64, n)
			for i := range n {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					app, err := store.Append(ctx, minikafka.AppendRequest{Topic: "events", Records: []minikafka.Record{{Value: []byte(fmt.Sprintf("%d", i))}}})
					if err == nil && app.BaseOffset != app.LastOffset {
						err = fmt.Errorf("unexpected append range: %+v", app)
					}
					errs <- err
					offsets <- app.BaseOffset
				}(i)
			}
			close(start)
			wg.Wait()
			close(errs)
			close(offsets)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			seen := make(map[int64]bool)
			for off := range offsets {
				if off < 0 || off >= n || seen[off] {
					t.Fatalf("invalid or duplicate offset: %d", off)
				}
				seen[off] = true
			}
			got, err := store.Fetch(ctx, minikafka.FetchRequest{Topic: "events"})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Records) != n || got.LatestOffset != n {
				t.Fatalf("records=%d latest=%d, want %d", len(got.Records), got.LatestOffset, n)
			}
			for i, rec := range got.Records {
				if rec.Offset != int64(i) {
					t.Fatalf("record %d has offset %d", i, rec.Offset)
				}
			}
		})
	}
}

func TestSQLiteRestartAndTopicFiles(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "events.db")
	store, err := sqlite.Open(root, sqlite.WithSynchronous(sqlite.SyncFull))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTopic(ctx, "billing_events", minikafka.TopicOptions{Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, minikafka.AppendRequest{Topic: "billing_events", Partition: 1, Records: []minikafka.Record{{Value: []byte("durable")}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitOffset(ctx, minikafka.CommitOffsetRequest{GroupID: "g", Topic: "billing_events", Partition: 1, Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	topicGlob := filepath.Join(root[:len(root)-len(filepath.Ext(root))], "minikafka_billing_events_*.db")
	if matches, err := filepath.Glob(topicGlob); err != nil || len(matches) != 2 {
		t.Fatalf("topic sqlite file matches=%v err=%v", matches, err)
	}

	reopened, err := sqlite.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	meta, err := reopened.Topic(ctx, "billing_events")
	if err != nil || meta.Partitions != 2 {
		t.Fatalf("topic metadata after restart = %+v, err=%v", meta, err)
	}
	latest, err := reopened.LatestOffset(ctx, "billing_events", 1)
	if err != nil {
		t.Fatal(err)
	}
	if latest != 1 {
		t.Fatalf("latest after restart = %d", latest)
	}
	off, err := reopened.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "g", Topic: "billing_events", Partition: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !off.Found || off.Offset != 1 {
		t.Fatalf("offset after restart = %+v", off)
	}
	if err := reopened.DeleteTopic(ctx, "billing_events"); err != nil {
		t.Fatal(err)
	}
	if matches, err := filepath.Glob(topicGlob); err != nil || len(matches) != 0 {
		t.Fatalf("topic files after deleting reopened topic=%v err=%v", matches, err)
	}
}
