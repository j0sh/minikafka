package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/mattn/go-sqlite3"
)

func newTestStore(t *testing.T, opts ...Option) (*Store, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	// Exercise URI escaping for both the metadata and topic database paths.
	s, err := Open(filepath.Join(t.TempDir(), "events ?#% &.db"), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	return s, ctx
}

func TestSQLiteURIPath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{{`/tmp/events ?#%.db`, `file:///tmp/events%20%3F%23%25.db`}}
	if runtime.GOOS == "windows" {
		tests = []struct {
			path string
			want string
		}{
			{`C:\data\events ?#%.db`, `file:///C:/data/events%20%3F%23%25.db`},
			{`\\server\share\events.db`, `file:////server/share/events.db`},
		}
	}
	for _, tc := range tests {
		got := (&url.URL{Scheme: "file", Path: sqliteURIPath(tc.path)}).String()
		if got != tc.want {
			t.Fatalf("SQLite URI for %q = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestDatabaseFilePrefix(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want []string
	}{
		{
			name: "default",
			want: []string{"minikafka_+meta.db", "minikafka_events_0.db", "minikafka_meta_0.db"},
		},
		{
			name: "custom",
			opts: []Option{WithFilePrefix("tenant_a_")},
			want: []string{"tenant_a_+meta.db", "tenant_a_events_0.db", "tenant_a_meta_0.db"},
		},
		{
			name: "empty",
			opts: []Option{WithFilePrefix("")},
			want: []string{"+meta.db", "events_0.db", "meta_0.db"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := Open(filepath.Join(t.TempDir(), "data.db"), tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Init(ctx); err != nil {
				t.Fatal(err)
			}
			for _, topic := range []string{"events", "meta", "delete_me"} {
				if err := s.CreateTopic(ctx, topic, minikafka.TopicOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			deletedPath := s.topicPath("delete_me", 0)
			if err := s.DeleteTopic(ctx, "delete_me"); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(deletedPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("deleted topic path %q still exists: %v", deletedPath, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			entries, err := os.ReadDir(s.root)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, entry := range entries {
				if !entry.IsDir() && filepath.Ext(entry.Name()) == ".db" {
					got = append(got, entry.Name())
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("database files = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFilePrefixRejectsPathSeparators(t *testing.T) {
	for _, prefix := range []string{"nested/", `nested\`} {
		if _, err := Open("data.db", WithFilePrefix(prefix)); err == nil {
			t.Fatalf("Open accepted file prefix %q", prefix)
		}
	}
}

func TestConnectionSettingsSurviveReplacement(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    []Option
		sync    int
		timeout int
		journal string
	}{
		{"defaults", nil, 1, 5000, "wal"},
		{"full", []Option{WithSynchronous(SyncFull), WithBusyTimeout(7311 * time.Millisecond)}, 2, 7311, "wal"},
		{"off_no_wal", []Option{WithSynchronous(SyncOff), WithBusyTimeout(0), WithWAL(false)}, 0, 0, "delete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newTestStore(t, tc.opts...)
			if err := s.CreateTopic(ctx, "events", minikafka.TopicOptions{}); err != nil {
				t.Fatal(err)
			}
			for name, db := range map[string]*sql.DB{"meta": s.meta, "topic": s.dbs[dbKey{topic: "events", partition: 0}]} {
				t.Run(name, func(t *testing.T) {
					if db.Stats().MaxOpenConnections != 1 {
						t.Fatal("expected a one-connection pool")
					}
					for attempt := 0; attempt < 2; attempt++ {
						conn, err := db.Conn(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer conn.Close()
						var journal string
						var synchronous, timeout int
						if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
							t.Fatal(err)
						}
						if err := conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
							t.Fatal(err)
						}
						if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeout); err != nil {
							t.Fatal(err)
						}
						if journal != tc.journal || synchronous != tc.sync || timeout != tc.timeout {
							t.Fatalf("attempt %d: journal=%s sync=%d timeout=%d", attempt, journal, synchronous, timeout)
						}
						if attempt == 0 {
							if err := conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
								t.Fatalf("discard connection: %v", err)
							}
						} else if err := conn.Close(); err != nil {
							t.Fatal(err)
						}
					}
					if db.Stats().Idle != 1 {
						t.Fatal("replacement connection was not retained")
					}
				})
			}
		})
	}
}

func TestTransactionsAcquireWriterLockAtBegin(t *testing.T) {
	s, ctx := newTestStore(t, WithBusyTimeout(0))
	if err := s.CreateTopic(ctx, "events", minikafka.TopicOptions{}); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"meta": s.metaPath(), "topic": s.topicPath("events", 0)} {
		t.Run(name, func(t *testing.T) {
			db := s.meta
			if name == "topic" {
				db = s.dbs[dbKey{topic: "events", partition: 0}]
			}
			peer, err := s.openDB(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			other, err := peer.BeginTx(ctx, nil)
			if err == nil {
				other.Rollback()
				t.Fatal("second transaction began before the first released the writer lock")
			}
			var busy sqlite3.Error
			if !errors.As(err, &busy) || busy.Code != sqlite3.ErrBusy {
				t.Fatalf("expected SQLITE_BUSY at BEGIN, got %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			other, err = peer.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			other.Rollback()
		})
	}
}

func TestInitReusesHandle(t *testing.T) {
	s, ctx := newTestStore(t)
	meta := s.meta
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if s.meta != meta {
		t.Fatal("repeated Init replaced the metadata pool")
	}
}

func TestInitRetriesAfterFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := s.Init(canceled); !errors.Is(err, context.Canceled) || s.meta != nil {
		t.Fatalf("canceled initialization: meta=%v err=%v", s.meta, err)
	}
	db, err := s.openDB(ctx, s.metaPath())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// An index with the schema's table name forces initialization to fail after
	// the connection has opened successfully.
	if _, err := db.ExecContext(ctx, "CREATE TABLE seed (id INTEGER); CREATE INDEX topics ON seed(id)"); err != nil {
		t.Fatal(err)
	}
	if err := s.Init(ctx); err == nil || s.meta != nil {
		t.Fatalf("failed schema initialization: meta=%v err=%v", s.meta, err)
	}
	if _, err := db.ExecContext(ctx, "DROP INDEX topics"); err != nil {
		t.Fatal(err)
	}
	if err := s.Init(ctx); err != nil {
		t.Fatalf("retry initialization: %v", err)
	}
	if err := s.CreateTopic(ctx, "events", minikafka.TopicOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestAppendInitializesVisibleTopic(t *testing.T) {
	s, ctx := newTestStore(t)
	// Reproduce the publication boundary: CreateTopic has inserted metadata but
	// has not opened the topic database yet. Append must initialize it safely.
	if _, err := s.meta.ExecContext(ctx, "INSERT INTO topics(topic, partitions, created_at_ms) VALUES ('events', 1, 0)"); err != nil {
		t.Fatal(err)
	}
	app, err := s.Append(ctx, minikafka.AppendRequest{Topic: "events", Records: []minikafka.Record{{Value: []byte("first")}}})
	if err != nil || app.BaseOffset != 0 {
		t.Fatalf("append to visible topic: %+v, %v", app, err)
	}
	// Reopening must preserve the next offset instead of resetting it.
	key := dbKey{topic: "events", partition: 0}
	if err := s.dbs[key].Close(); err != nil {
		t.Fatal(err)
	}
	delete(s.dbs, key)
	app, err = s.Append(ctx, minikafka.AppendRequest{Topic: "events", Records: []minikafka.Record{{Value: []byte("second")}}})
	if err != nil || app.BaseOffset != 1 {
		t.Fatalf("append after reopening: %+v, %v", app, err)
	}
}

func TestConcurrentCreateAndAppend(t *testing.T) {
	s, ctx := newTestStore(t)
	const n = 16
	start := make(chan struct{})
	errs := make(chan error, 2*n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		topic := fmt.Sprintf("events_%d", i)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			errs <- s.CreateTopic(ctx, topic, minikafka.TopicOptions{})
		}()
		go func() {
			defer wg.Done()
			<-start
			for {
				_, err := s.Append(ctx, minikafka.AppendRequest{Topic: topic, Records: []minikafka.Record{{Value: []byte("record")}}})
				if !errors.Is(err, minikafka.ErrTopicNotFound) {
					errs <- err
					return
				}
				runtime.Gosched()
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		if next, err := s.LatestOffset(ctx, fmt.Sprintf("events_%d", i), 0); err != nil || next != 1 {
			t.Fatalf("topic %d: next=%d err=%v", i, next, err)
		}
	}
}

func TestFetchDuringAppends(t *testing.T) {
	s, ctx := newTestStore(t)
	if err := s.CreateTopic(ctx, "events", minikafka.TopicOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, minikafka.AppendRequest{Topic: "events", Records: []minikafka.Record{{}, {}}}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 100; i++ {
			if _, err := s.Append(ctx, minikafka.AppendRequest{Topic: "events", Records: []minikafka.Record{{}}}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for _, maxBytes := range []int32{0, 1} {
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 100; i++ {
				got, err := s.Fetch(ctx, minikafka.FetchRequest{Topic: "events", MaxBytes: maxBytes})
				if err != nil {
					t.Error(err)
					return
				}
				if len(got.Records) == 0 || got.HighWatermark != got.LatestOffset || got.EarliestOffset != 0 {
					t.Errorf("invalid fetch: %+v", got)
					return
				}
				if maxBytes == 1 && len(got.Records) != 1 {
					t.Errorf("byte-limited fetch returned %d records", len(got.Records))
					return
				}
				for i, rec := range got.Records {
					if rec.Offset != int64(i) || rec.Offset >= got.HighWatermark {
						t.Errorf("record offset=%d, index=%d, watermark=%d", rec.Offset, i, got.HighWatermark)
						return
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}
