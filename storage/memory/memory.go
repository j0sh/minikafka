package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/j0sh/minikafka"
)

type Store struct {
	mu      sync.RWMutex
	topics  map[string]*topic
	offsets map[offsetKey]int64
}

type topic struct {
	meta       minikafka.TopicMetadata
	nextOffset int64
	records    []minikafka.Record
}

type offsetKey struct {
	group string
	topic string
}

func Open() *Store {
	return &Store{
		topics:  make(map[string]*topic),
		offsets: make(map[offsetKey]int64),
	}
}

func (s *Store) Init(context.Context) error { return nil }
func (s *Store) Close() error               { return nil }

func (s *Store) CreateTopic(_ context.Context, name string, opts minikafka.TopicOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[name]; ok {
		return minikafka.ErrTopicExists
	}
	s.topics[name] = &topic{meta: minikafka.TopicMetadata{Topic: name, CreatedAt: time.Now(), Retention: opts.Retention}}
	return nil
}

func (s *Store) DeleteTopic(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.topics, name)
	for k := range s.offsets {
		if k.topic == name {
			delete(s.offsets, k)
		}
	}
	return nil
}

func (s *Store) Topic(_ context.Context, name string) (minikafka.TopicMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[name]
	if !ok {
		return minikafka.TopicMetadata{}, minikafka.ErrTopicNotFound
	}
	return t.meta, nil
}

func (s *Store) ListTopics(context.Context) ([]minikafka.TopicMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]minikafka.TopicMetadata, 0, len(s.topics))
	for _, t := range s.topics {
		out = append(out, t.meta)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out, nil
}

func (s *Store) Append(_ context.Context, req minikafka.AppendRequest) (minikafka.AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.topics[req.Topic]
	if !ok {
		return minikafka.AppendResult{}, minikafka.ErrTopicNotFound
	}
	base := t.nextOffset
	for i := range req.Records {
		rec := cloneRecord(req.Records[i])
		rec.Offset = base + int64(i)
		if rec.Timestamp.IsZero() {
			rec.Timestamp = time.Now()
		}
		t.records = append(t.records, rec)
	}
	t.nextOffset += int64(len(req.Records))
	return minikafka.AppendResult{BaseOffset: base, LastOffset: t.nextOffset - 1}, nil
}

func (s *Store) Fetch(_ context.Context, req minikafka.FetchRequest) (minikafka.FetchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[req.Topic]
	if !ok {
		return minikafka.FetchResult{}, minikafka.ErrTopicNotFound
	}
	earliest := earliest(t)
	if req.Offset < earliest {
		return minikafka.FetchResult{}, minikafka.ErrOffsetOutOfRange
	}
	maxRecords := req.MaxRecords
	if maxRecords <= 0 {
		maxRecords = len(t.records)
	}
	var out []minikafka.Record
	var bytes int32
	for _, rec := range t.records {
		if rec.Offset < req.Offset {
			continue
		}
		size := int32(recordSize(rec))
		if req.MaxBytes > 0 && len(out) > 0 && bytes+size > req.MaxBytes {
			break
		}
		out = append(out, cloneRecord(rec))
		bytes += size
		if len(out) >= maxRecords {
			break
		}
	}
	return minikafka.FetchResult{Records: out, HighWatermark: t.nextOffset, EarliestOffset: earliest, LatestOffset: t.nextOffset}, nil
}

func (s *Store) CommitOffset(_ context.Context, req minikafka.CommitOffsetRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[req.Topic]; !ok {
		return minikafka.ErrTopicNotFound
	}
	s.offsets[offsetKey{group: req.GroupID, topic: req.Topic}] = req.Offset
	return nil
}

func (s *Store) FetchOffset(_ context.Context, req minikafka.FetchOffsetRequest) (minikafka.FetchOffsetResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	off, ok := s.offsets[offsetKey{group: req.GroupID, topic: req.Topic}]
	return minikafka.FetchOffsetResult{Offset: off, Found: ok}, nil
}

func (s *Store) EarliestOffset(_ context.Context, name string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[name]
	if !ok {
		return 0, minikafka.ErrTopicNotFound
	}
	return earliest(t), nil
}

func (s *Store) LatestOffset(_ context.Context, name string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[name]
	if !ok {
		return 0, minikafka.ErrTopicNotFound
	}
	return t.nextOffset, nil
}

func (s *Store) ApplyRetention(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.topics[name]
	if !ok {
		return minikafka.ErrTopicNotFound
	}
	p := t.meta.Retention
	if p.MaxAge > 0 {
		cutoff := time.Now().Add(-p.MaxAge)
		t.records = filter(t.records, func(r minikafka.Record) bool { return !r.Timestamp.Before(cutoff) })
	}
	if p.MaxMessages > 0 && int64(len(t.records)) > p.MaxMessages {
		t.records = t.records[len(t.records)-int(p.MaxMessages):]
	}
	if p.MaxBytes > 0 {
		for totalSize(t.records) > p.MaxBytes && len(t.records) > 0 {
			t.records = t.records[1:]
		}
	}
	return nil
}

func earliest(t *topic) int64 {
	if len(t.records) == 0 {
		return t.nextOffset
	}
	return t.records[0].Offset
}

func cloneRecord(r minikafka.Record) minikafka.Record {
	out := r
	out.Key = append([]byte(nil), r.Key...)
	out.Value = append([]byte(nil), r.Value...)
	out.Headers = make([]minikafka.Header, len(r.Headers))
	for i, h := range r.Headers {
		out.Headers[i] = minikafka.Header{Key: h.Key, Value: append([]byte(nil), h.Value...)}
	}
	return out
}

func filter(records []minikafka.Record, keep func(minikafka.Record) bool) []minikafka.Record {
	out := records[:0]
	for _, r := range records {
		if keep(r) {
			out = append(out, r)
		}
	}
	return out
}

func totalSize(records []minikafka.Record) int64 {
	var total int64
	for _, r := range records {
		total += int64(recordSize(r))
	}
	return total
}

func recordSize(r minikafka.Record) int {
	size := len(r.Key) + len(r.Value) + 16
	for _, h := range r.Headers {
		size += len(h.Key) + len(h.Value)
	}
	return size
}

var _ minikafka.Store = (*Store)(nil)

func IsMemoryStore(store minikafka.Store) bool {
	_, ok := store.(*Store)
	return ok
}
