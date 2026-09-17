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
			earliest, err := store.EarliestOffset(ctx, "events")
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
			for i := 0; i < n; i++ {
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
	if err := store.CreateTopic(ctx, "billing_events", minikafka.TopicOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, minikafka.AppendRequest{Topic: "billing_events", Records: []minikafka.Record{{Value: []byte("durable")}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitOffset(ctx, minikafka.CommitOffsetRequest{GroupID: "g", Topic: "billing_events", Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if matches, err := filepath.Glob(filepath.Join(root[:len(root)-len(filepath.Ext(root))], "minikafka_billing_events.db")); err != nil || len(matches) != 1 {
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
	latest, err := reopened.LatestOffset(ctx, "billing_events")
	if err != nil {
		t.Fatal(err)
	}
	if latest != 1 {
		t.Fatalf("latest after restart = %d", latest)
	}
	off, err := reopened.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "g", Topic: "billing_events"})
	if err != nil {
		t.Fatal(err)
	}
	if !off.Found || off.Offset != 1 {
		t.Fatalf("offset after restart = %+v", off)
	}
}
