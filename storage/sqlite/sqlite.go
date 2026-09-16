package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/j0sh/minikafka"
	_ "github.com/mattn/go-sqlite3"
)

// Store supports concurrent operations after initialization. Callers must stop
// operations on a topic before deleting it, and stop all operations before Close.
type Store struct {
	root string
	opts options
	meta *sql.DB
	mu   sync.Mutex
	dbs  map[string]*sql.DB
}

type Option func(*options)

type options struct {
	wal         bool
	busyTimeout time.Duration
	synchronous SynchronousMode
}

type SynchronousMode int

const (
	SyncOff SynchronousMode = iota
	SyncNormal
	SyncFull
)

func Open(path string, opts ...Option) (*Store, error) {
	if path == "" {
		return nil, errors.New("sqlite path is required")
	}
	o := options{wal: true, busyTimeout: 5 * time.Second, synchronous: SyncNormal}
	for _, opt := range opts {
		opt(&o)
	}
	root := path
	if filepath.Ext(path) != "" {
		root = strings.TrimSuffix(path, filepath.Ext(path))
	}
	return &Store{root: root, opts: o, dbs: make(map[string]*sql.DB)}, nil
}

func WithWAL(enabled bool) Option {
	return func(o *options) { o.wal = enabled }
}

func WithBusyTimeout(d time.Duration) Option {
	return func(o *options) { o.busyTimeout = d }
}

func WithSynchronous(mode SynchronousMode) Option {
	return func(o *options) { o.synchronous = mode }
}

// Init reuses an already initialized database. Concurrent calls may race and
// replace the handle, so bring up the database just once. Fixing this is not
// worth the squeeze; it's a usage problem and not a correctness problem.
func (s *Store) Init(ctx context.Context) error {
	if s.meta != nil {
		return nil
	}
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return err
	}
	db, err := s.openDB(ctx, filepath.Join(s.root, "_meta.db"))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS topics (
	topic TEXT PRIMARY KEY,
	created_at_ms INTEGER NOT NULL,
	retention_max_age_ms INTEGER,
	retention_max_bytes INTEGER,
	retention_max_messages INTEGER
);
CREATE TABLE IF NOT EXISTS consumer_offsets (
	group_id TEXT NOT NULL,
	topic TEXT NOT NULL,
	offset INTEGER NOT NULL,
	updated_at_ms INTEGER NOT NULL,
	PRIMARY KEY (group_id, topic)
);`)
	if err != nil {
		db.Close()
		return err
	}
	s.meta = db
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	for topic, db := range s.dbs {
		if e := db.Close(); err == nil {
			err = e
		}
		delete(s.dbs, topic)
	}
	if s.meta != nil {
		if e := s.meta.Close(); err == nil {
			err = e
		}
	}
	return err
}

func (s *Store) CreateTopic(ctx context.Context, topic string, opts minikafka.TopicOptions) error {
	if s.meta == nil {
		if err := s.Init(ctx); err != nil {
			return err
		}
	}
	_, err := s.meta.ExecContext(ctx, `INSERT INTO topics(topic, created_at_ms, retention_max_age_ms, retention_max_bytes, retention_max_messages) VALUES (?, ?, ?, ?, ?)`,
		topic, time.Now().UnixMilli(), durMS(opts.Retention.MaxAge), nullInt(opts.Retention.MaxBytes), nullInt(opts.Retention.MaxMessages))
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return minikafka.ErrTopicExists
		}
		return err
	}
	_, err = s.topicDB(ctx, topic)
	return err
}

func (s *Store) DeleteTopic(ctx context.Context, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.meta.ExecContext(ctx, `DELETE FROM topics WHERE topic = ?`, topic)
	if db := s.dbs[topic]; db != nil {
		_ = db.Close()
		delete(s.dbs, topic)
	}
	_ = os.Remove(s.topicPath(topic))
	return err
}

func (s *Store) Topic(ctx context.Context, topic string) (minikafka.TopicMetadata, error) {
	row := s.meta.QueryRowContext(ctx, `SELECT topic, created_at_ms, retention_max_age_ms, retention_max_bytes, retention_max_messages FROM topics WHERE topic = ?`, topic)
	return scanTopic(row)
}

func (s *Store) ListTopics(ctx context.Context) ([]minikafka.TopicMetadata, error) {
	rows, err := s.meta.QueryContext(ctx, `SELECT topic, created_at_ms, retention_max_age_ms, retention_max_bytes, retention_max_messages FROM topics ORDER BY topic`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var topics []minikafka.TopicMetadata
	for rows.Next() {
		t, err := scanTopic(rows)
		if err != nil {
			return nil, err
		}
		topics = append(topics, t)
	}
	return topics, rows.Err()
}

func (s *Store) Append(ctx context.Context, req minikafka.AppendRequest) (minikafka.AppendResult, error) {
	db, err := s.topicDB(ctx, req.Topic)
	if err != nil {
		return minikafka.AppendResult{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return minikafka.AppendResult{}, err
	}
	defer tx.Rollback()
	var base int64
	if err := tx.QueryRowContext(ctx, `SELECT next_offset FROM topic_offsets WHERE topic = ?`, req.Topic).Scan(&base); err != nil {
		return minikafka.AppendResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE topic_offsets SET next_offset = next_offset + ? WHERE topic = ?`, len(req.Records), req.Topic); err != nil {
		return minikafka.AppendResult{}, err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO messages(topic, offset, timestamp_ms, key, value, headers, size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return minikafka.AppendResult{}, err
	}
	defer stmt.Close()
	for i, rec := range req.Records {
		if rec.Timestamp.IsZero() {
			rec.Timestamp = time.Now()
		}
		headers, err := json.Marshal(rec.Headers)
		if err != nil {
			return minikafka.AppendResult{}, err
		}
		if _, err := stmt.ExecContext(ctx, req.Topic, base+int64(i), rec.Timestamp.UnixMilli(), rec.Key, rec.Value, headers, recordSize(rec)); err != nil {
			return minikafka.AppendResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return minikafka.AppendResult{}, err
	}
	return minikafka.AppendResult{BaseOffset: base, LastOffset: base + int64(len(req.Records)) - 1}, nil
}

func (s *Store) Fetch(ctx context.Context, req minikafka.FetchRequest) (minikafka.FetchResult, error) {
	db, err := s.topicDB(ctx, req.Topic)
	if err != nil {
		return minikafka.FetchResult{}, err
	}
	earliest, err := earliestOffset(ctx, db, req.Topic)
	if err != nil {
		return minikafka.FetchResult{}, err
	}
	if req.Offset < earliest {
		return minikafka.FetchResult{}, minikafka.ErrOffsetOutOfRange
	}
	limit := req.MaxRecords
	if limit <= 0 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, `SELECT offset, timestamp_ms, key, value, headers, size_bytes FROM messages WHERE topic = ? AND offset >= ? ORDER BY offset LIMIT ?`, req.Topic, req.Offset, limit)
	if err != nil {
		return minikafka.FetchResult{}, err
	}
	defer rows.Close()
	var records []minikafka.Record
	var total int32
	for rows.Next() {
		var rec minikafka.Record
		var ts int64
		var headers []byte
		var size int32
		if err := rows.Scan(&rec.Offset, &ts, &rec.Key, &rec.Value, &headers, &size); err != nil {
			return minikafka.FetchResult{}, err
		}
		// Return one contiguous run, even after age-based retention leaves gaps.
		if len(records) > 0 && rec.Offset-1 != records[len(records)-1].Offset {
			break
		}
		if req.MaxBytes > 0 && len(records) > 0 && total+size > req.MaxBytes {
			break
		}
		rec.Timestamp = time.UnixMilli(ts)
		_ = json.Unmarshal(headers, &rec.Headers)
		records = append(records, rec)
		total += size
	}
	if err := rows.Err(); err != nil {
		return minikafka.FetchResult{}, err
	}
	// Release the pool's only connection, including when a limit or gap ended
	// iteration early, before querying the watermark.
	if err := rows.Close(); err != nil {
		return minikafka.FetchResult{}, err
	}
	// Read the watermark after the records so concurrent appends cannot put a
	// returned record beyond it. Bounds and records need not share one snapshot.
	latest, err := latestOffset(ctx, db, req.Topic)
	if err != nil {
		return minikafka.FetchResult{}, err
	}
	return minikafka.FetchResult{Records: records, HighWatermark: latest, EarliestOffset: earliest, LatestOffset: latest}, nil
}

func (s *Store) CommitOffset(ctx context.Context, req minikafka.CommitOffsetRequest) error {
	_, err := s.Topic(ctx, req.Topic)
	if err != nil {
		return err
	}
	_, err = s.meta.ExecContext(ctx, `INSERT INTO consumer_offsets(group_id, topic, offset, updated_at_ms) VALUES (?, ?, ?, ?)
ON CONFLICT(group_id, topic) DO UPDATE SET offset = excluded.offset, updated_at_ms = excluded.updated_at_ms`,
		req.GroupID, req.Topic, req.Offset, time.Now().UnixMilli())
	return err
}

func (s *Store) FetchOffset(ctx context.Context, req minikafka.FetchOffsetRequest) (minikafka.FetchOffsetResult, error) {
	var off int64
	err := s.meta.QueryRowContext(ctx, `SELECT offset FROM consumer_offsets WHERE group_id = ? AND topic = ?`, req.GroupID, req.Topic).Scan(&off)
	if errors.Is(err, sql.ErrNoRows) {
		return minikafka.FetchOffsetResult{}, nil
	}
	return minikafka.FetchOffsetResult{Offset: off, Found: err == nil}, err
}

func (s *Store) EarliestOffset(ctx context.Context, topic string) (int64, error) {
	db, err := s.topicDB(ctx, topic)
	if err != nil {
		return 0, err
	}
	return earliestOffset(ctx, db, topic)
}

func earliestOffset(ctx context.Context, db *sql.DB, topic string) (int64, error) {
	var next int64
	if err := db.QueryRowContext(ctx, `SELECT next_offset FROM topic_offsets WHERE topic = ?`, topic).Scan(&next); err != nil {
		return 0, err
	}
	var earliest sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MIN(offset) FROM messages WHERE topic = ?`, topic).Scan(&earliest); err != nil {
		return 0, err
	}
	if earliest.Valid {
		return earliest.Int64, nil
	}
	return next, nil
}

func (s *Store) LatestOffset(ctx context.Context, topic string) (int64, error) {
	db, err := s.topicDB(ctx, topic)
	if err != nil {
		return 0, err
	}
	return latestOffset(ctx, db, topic)
}

func latestOffset(ctx context.Context, db *sql.DB, topic string) (int64, error) {
	var next int64
	err := db.QueryRowContext(ctx, `SELECT next_offset FROM topic_offsets WHERE topic = ?`, topic).Scan(&next)
	return next, err
}

func (s *Store) ApplyRetention(ctx context.Context, topic string) error {
	meta, err := s.Topic(ctx, topic)
	if err != nil {
		return err
	}
	db, err := s.topicDB(ctx, topic)
	if err != nil {
		return err
	}
	if meta.Retention.MaxAge > 0 {
		if _, err := db.ExecContext(ctx, `DELETE FROM messages WHERE topic = ? AND timestamp_ms < ?`, topic, time.Now().Add(-meta.Retention.MaxAge).UnixMilli()); err != nil {
			return err
		}
	}
	if meta.Retention.MaxMessages > 0 {
		if _, err := db.ExecContext(ctx, `DELETE FROM messages WHERE topic = ? AND offset < (SELECT COALESCE(MAX(offset), -1) - ? + 1 FROM messages WHERE topic = ?)`, topic, meta.Retention.MaxMessages, topic); err != nil {
			return err
		}
	}
	if meta.Retention.MaxBytes > 0 {
		for {
			var total int64
			if err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size_bytes), 0) FROM messages WHERE topic = ?`, topic).Scan(&total); err != nil {
				return err
			}
			if total <= meta.Retention.MaxBytes {
				break
			}
			if _, err := db.ExecContext(ctx, `DELETE FROM messages WHERE topic = ? AND offset = (SELECT MIN(offset) FROM messages WHERE topic = ?)`, topic, topic); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) topicDB(ctx context.Context, topic string) (*sql.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.Topic(ctx, topic); err != nil {
		return nil, err
	}
	if db := s.dbs[topic]; db != nil {
		return db, nil
	}
	db, err := s.openDB(ctx, s.topicPath(topic))
	if err != nil {
		return nil, err
	}
	_, err = db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS topic_offsets (
	topic TEXT PRIMARY KEY,
	next_offset INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
	topic TEXT NOT NULL,
	offset INTEGER NOT NULL,
	timestamp_ms INTEGER NOT NULL,
	key BLOB,
	value BLOB,
	headers BLOB,
	size_bytes INTEGER NOT NULL,
	PRIMARY KEY (topic, offset)
);
CREATE INDEX IF NOT EXISTS messages_topic_ts ON messages(topic, timestamp_ms);
CREATE INDEX IF NOT EXISTS messages_topic_offset ON messages(topic, offset);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO topic_offsets(topic, next_offset) VALUES (?, 0)`, topic); err != nil {
		db.Close()
		return nil, err
	}
	s.dbs[topic] = db
	return db, nil
}

func (s *Store) openDB(ctx context.Context, path string) (*sql.DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: sqliteURIPath(abs)}
	q := u.Query()
	if s.opts.wal {
		q.Set("_journal_mode", "WAL")
	}
	syncMode := "NORMAL"
	if s.opts.synchronous == SyncFull {
		syncMode = "FULL"
	} else if s.opts.synchronous == SyncOff {
		syncMode = "OFF"
	}
	q.Set("_synchronous", syncMode)
	q.Set("_busy_timeout", strconv.FormatInt(s.opts.busyTimeout.Milliseconds(), 10))
	q.Set("_txlock", "immediate")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func sqliteURIPath(path string) string {
	path = filepath.ToSlash(path)
	// Absolute Unix and UNC paths already begin with a slash. A Windows
	// drive-letter path needs one so the drive is not encoded as URI authority.
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func (s *Store) topicPath(topic string) string {
	slug := regexp.MustCompile(`[^A-Za-z0-9_.-]+`).ReplaceAllString(topic, "_")
	if slug == "" {
		slug = "topic"
	}
	return filepath.Join(s.root, slug+".db")
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTopic(row rowScanner) (minikafka.TopicMetadata, error) {
	var t minikafka.TopicMetadata
	var created int64
	var maxAge, maxBytes, maxMessages sql.NullInt64
	if err := row.Scan(&t.Topic, &created, &maxAge, &maxBytes, &maxMessages); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t, minikafka.ErrTopicNotFound
		}
		return t, err
	}
	t.CreatedAt = time.UnixMilli(created)
	if maxAge.Valid {
		t.Retention.MaxAge = time.Duration(maxAge.Int64) * time.Millisecond
	}
	if maxBytes.Valid {
		t.Retention.MaxBytes = maxBytes.Int64
	}
	if maxMessages.Valid {
		t.Retention.MaxMessages = maxMessages.Int64
	}
	return t, nil
}

func durMS(d time.Duration) any {
	if d <= 0 {
		return nil
	}
	return d.Milliseconds()
}

func nullInt(v int64) any {
	if v <= 0 {
		return nil
	}
	return v
}

func recordSize(r minikafka.Record) int {
	size := len(r.Key) + len(r.Value) + 16
	for _, h := range r.Headers {
		size += len(h.Key) + len(h.Value)
	}
	return size
}

var _ minikafka.Store = (*Store)(nil)
