package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/j0sh/minikafka"
)

type Store struct {
	db  *sql.DB
	dsn string
}

type Option func(*options)
type options struct{}

func Open(dsn string, _ ...Option) (*Store, error) {
	if dsn == "" {
		return nil, errors.New("postgres dsn is required")
	}
	return &Store{dsn: dsn}, nil
}

func OpenDB(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) Init(ctx context.Context) error {
	if s.db == nil {
		db, err := sql.Open("postgres", s.dsn)
		if err != nil {
			return err
		}
		s.db = db
	}
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) CreateTopic(ctx context.Context, topic string, opts minikafka.TopicOptions) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO topics(topic, retention_max_age_ms, retention_max_bytes, retention_max_messages) VALUES ($1, $2, $3, $4)`,
		topic, durMS(opts.Retention.MaxAge), nullInt(opts.Retention.MaxBytes), nullInt(opts.Retention.MaxMessages))
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			return minikafka.ErrTopicExists
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO topic_offsets(topic, next_offset) VALUES ($1, 0)`, topic); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DeleteTopic(ctx context.Context, topic string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM topics WHERE topic = $1`, topic)
	return err
}

func (s *Store) Topic(ctx context.Context, topic string) (minikafka.TopicMetadata, error) {
	return scanTopic(s.db.QueryRowContext(ctx, `SELECT topic, created_at, retention_max_age_ms, retention_max_bytes, retention_max_messages FROM topics WHERE topic = $1`, topic))
}

func (s *Store) ListTopics(ctx context.Context) ([]minikafka.TopicMetadata, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT topic, created_at, retention_max_age_ms, retention_max_bytes, retention_max_messages FROM topics ORDER BY topic`)
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return minikafka.AppendResult{}, err
	}
	defer tx.Rollback()
	var base int64
	if err := tx.QueryRowContext(ctx, `UPDATE topic_offsets SET next_offset = next_offset + $1 WHERE topic = $2 RETURNING next_offset - $1 AS base_offset`, len(req.Records), req.Topic).Scan(&base); err != nil {
		return minikafka.AppendResult{}, err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO messages(topic, offset, timestamp_ms, key, value, headers, size_bytes) VALUES ($1, $2, $3, $4, $5, $6, $7)`)
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
	earliest, err := s.EarliestOffset(ctx, req.Topic)
	if err != nil {
		return minikafka.FetchResult{}, err
	}
	latest, err := s.LatestOffset(ctx, req.Topic)
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
	rows, err := s.db.QueryContext(ctx, `SELECT offset, timestamp_ms, key, value, headers, size_bytes FROM messages WHERE topic = $1 AND offset >= $2 ORDER BY offset LIMIT $3`, req.Topic, req.Offset, limit)
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
	return minikafka.FetchResult{Records: records, HighWatermark: latest, EarliestOffset: earliest, LatestOffset: latest}, rows.Err()
}

func (s *Store) CommitOffset(ctx context.Context, req minikafka.CommitOffsetRequest) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO consumer_offsets(group_id, topic, offset, updated_at) VALUES ($1, $2, $3, now())
ON CONFLICT(group_id, topic) DO UPDATE SET offset = excluded.offset, updated_at = excluded.updated_at`, req.GroupID, req.Topic, req.Offset)
	return err
}

func (s *Store) FetchOffset(ctx context.Context, req minikafka.FetchOffsetRequest) (minikafka.FetchOffsetResult, error) {
	var off int64
	err := s.db.QueryRowContext(ctx, `SELECT offset FROM consumer_offsets WHERE group_id = $1 AND topic = $2`, req.GroupID, req.Topic).Scan(&off)
	if errors.Is(err, sql.ErrNoRows) {
		return minikafka.FetchOffsetResult{}, nil
	}
	return minikafka.FetchOffsetResult{Offset: off, Found: err == nil}, err
}

func (s *Store) EarliestOffset(ctx context.Context, topic string) (int64, error) {
	var earliest sql.NullInt64
	var next int64
	if err := s.db.QueryRowContext(ctx, `SELECT next_offset FROM topic_offsets WHERE topic = $1`, topic).Scan(&next); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(offset) FROM messages WHERE topic = $1`, topic).Scan(&earliest); err != nil {
		return 0, err
	}
	if earliest.Valid {
		return earliest.Int64, nil
	}
	return next, nil
}

func (s *Store) LatestOffset(ctx context.Context, topic string) (int64, error) {
	var next int64
	err := s.db.QueryRowContext(ctx, `SELECT next_offset FROM topic_offsets WHERE topic = $1`, topic).Scan(&next)
	return next, err
}

func (s *Store) ApplyRetention(ctx context.Context, topic string) error {
	meta, err := s.Topic(ctx, topic)
	if err != nil {
		return err
	}
	if meta.Retention.MaxAge > 0 {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM messages WHERE topic = $1 AND timestamp_ms < $2`, topic, time.Now().Add(-meta.Retention.MaxAge).UnixMilli()); err != nil {
			return err
		}
	}
	if meta.Retention.MaxMessages > 0 {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM messages WHERE topic = $1 AND offset < (SELECT COALESCE(MAX(offset), -1) - $2 + 1 FROM messages WHERE topic = $1)`, topic, meta.Retention.MaxMessages); err != nil {
			return err
		}
	}
	if meta.Retention.MaxBytes > 0 {
		for {
			var total int64
			if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size_bytes), 0) FROM messages WHERE topic = $1`, topic).Scan(&total); err != nil {
				return err
			}
			if total <= meta.Retention.MaxBytes {
				break
			}
			if _, err := s.db.ExecContext(ctx, `DELETE FROM messages WHERE topic = $1 AND offset = (SELECT MIN(offset) FROM messages WHERE topic = $1)`, topic); err != nil {
				return err
			}
		}
	}
	return nil
}

const schema = `
CREATE TABLE IF NOT EXISTS topics (
	topic TEXT PRIMARY KEY,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	retention_max_age_ms BIGINT,
	retention_max_bytes BIGINT,
	retention_max_messages BIGINT
);
CREATE TABLE IF NOT EXISTS topic_offsets (
	topic TEXT PRIMARY KEY REFERENCES topics(topic) ON DELETE CASCADE,
	next_offset BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
	topic TEXT NOT NULL REFERENCES topics(topic) ON DELETE CASCADE,
	offset BIGINT NOT NULL,
	timestamp_ms BIGINT NOT NULL,
	key BYTEA,
	value BYTEA NOT NULL,
	headers BYTEA,
	size_bytes BIGINT NOT NULL,
	PRIMARY KEY (topic, offset)
);
CREATE TABLE IF NOT EXISTS consumer_offsets (
	group_id TEXT NOT NULL,
	topic TEXT NOT NULL REFERENCES topics(topic) ON DELETE CASCADE,
	offset BIGINT NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	PRIMARY KEY (group_id, topic)
);
CREATE INDEX IF NOT EXISTS messages_topic_ts ON messages(topic, timestamp_ms);
CREATE INDEX IF NOT EXISTS messages_topic_offset ON messages(topic, offset);`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTopic(row rowScanner) (minikafka.TopicMetadata, error) {
	var t minikafka.TopicMetadata
	var maxAge, maxBytes, maxMessages sql.NullInt64
	if err := row.Scan(&t.Topic, &t.CreatedAt, &maxAge, &maxBytes, &maxMessages); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t, minikafka.ErrTopicNotFound
		}
		return t, err
	}
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
