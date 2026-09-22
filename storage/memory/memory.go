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
	partitions []partition
}

type partition struct {
	nextOffset int64
	records    []minikafka.Record
}

type offsetKey struct {
	group     string
	topic     string
	partition int32
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
	if opts.Partitions < 0 {
		return minikafka.ErrInvalidPartition
	}
	opts.Partitions = max(1, opts.Partitions)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[name]; ok {
		return minikafka.ErrTopicExists
	}
	s.topics[name] = &topic{
		meta:       minikafka.TopicMetadata{Topic: name, Partitions: opts.Partitions, CreatedAt: time.Now(), Retention: opts.Retention},
		partitions: make([]partition, opts.Partitions),
	}
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
	p, err := topicPartition(t, req.Partition)
	if err != nil {
		return minikafka.AppendResult{}, err
	}
	base := p.nextOffset
	for i := range req.Records {
		rec := cloneRecord(req.Records[i])
		rec.Offset = base + int64(i)
		if rec.Timestamp.IsZero() {
			rec.Timestamp = time.Now()
		}
		p.records = append(p.records, rec)
	}
	p.nextOffset += int64(len(req.Records))
	return minikafka.AppendResult{BaseOffset: base, LastOffset: p.nextOffset - 1}, nil
}

func (s *Store) Fetch(_ context.Context, req minikafka.FetchRequest) (minikafka.FetchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[req.Topic]
	if !ok {
		return minikafka.FetchResult{}, minikafka.ErrTopicNotFound
	}
	p, err := topicPartition(t, req.Partition)
	if err != nil {
		return minikafka.FetchResult{}, err
	}
	earliest := earliest(p)
	if req.Offset < earliest {
		return minikafka.FetchResult{}, minikafka.ErrOffsetOutOfRange
	}
	maxRecords := req.MaxRecords
	if maxRecords <= 0 {
		maxRecords = len(p.records)
	}
	var out []minikafka.Record
	var bytes int32
	for _, rec := range p.records {
		if rec.Offset < req.Offset {
			continue
		}
		// Age-based retention can leave gaps when timestamps arrive out of order.
		if len(out) > 0 && rec.Offset-1 != out[len(out)-1].Offset {
			break
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
	return minikafka.FetchResult{Records: out, HighWatermark: p.nextOffset, EarliestOffset: earliest, LatestOffset: p.nextOffset}, nil
}

func (s *Store) CommitOffset(_ context.Context, req minikafka.CommitOffsetRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[req.Topic]; !ok {
		return minikafka.ErrTopicNotFound
	}
	if _, err := topicPartition(s.topics[req.Topic], req.Partition); err != nil {
		return err
	}
	s.offsets[offsetKey{group: req.GroupID, topic: req.Topic, partition: req.Partition}] = req.Offset
	return nil
}

func (s *Store) FetchOffset(_ context.Context, req minikafka.FetchOffsetRequest) (minikafka.FetchOffsetResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, exists := s.topics[req.Topic]
	if !exists {
		return minikafka.FetchOffsetResult{}, minikafka.ErrTopicNotFound
	}
	if _, err := topicPartition(t, req.Partition); err != nil {
		return minikafka.FetchOffsetResult{}, err
	}
	off, ok := s.offsets[offsetKey{group: req.GroupID, topic: req.Topic, partition: req.Partition}]
	return minikafka.FetchOffsetResult{Offset: off, Found: ok}, nil
}

func (s *Store) EarliestOffset(_ context.Context, name string, partition int32) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[name]
	if !ok {
		return 0, minikafka.ErrTopicNotFound
	}
	p, err := topicPartition(t, partition)
	if err != nil {
		return 0, err
	}
	return earliest(p), nil
}

func (s *Store) LatestOffset(_ context.Context, name string, partition int32) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[name]
	if !ok {
		return 0, minikafka.ErrTopicNotFound
	}
	p, err := topicPartition(t, partition)
	if err != nil {
		return 0, err
	}
	return p.nextOffset, nil
}

func (s *Store) ApplyRetention(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.topics[name]
	if !ok {
		return minikafka.ErrTopicNotFound
	}
	policy := t.meta.Retention
	for i := range t.partitions {
		p := &t.partitions[i]
		if policy.MaxAge > 0 {
			cutoff := time.Now().Add(-policy.MaxAge)
			p.records = filter(p.records, func(r minikafka.Record) bool { return !r.Timestamp.Before(cutoff) })
		}
		if policy.MaxMessages > 0 && int64(len(p.records)) > policy.MaxMessages {
			p.records = p.records[len(p.records)-int(policy.MaxMessages):]
		}
		if policy.MaxBytes > 0 {
			for totalSize(p.records) > policy.MaxBytes && len(p.records) > 0 {
				p.records = p.records[1:]
			}
		}
	}
	return nil
}

func topicPartition(t *topic, partition int32) (*partition, error) {
	if partition < 0 || partition >= int32(len(t.partitions)) {
		return nil, minikafka.ErrInvalidPartition
	}
	return &t.partitions[partition], nil
}

func earliest(p *partition) int64 {
	if len(p.records) == 0 {
		return p.nextOffset
	}
	return p.records[0].Offset
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
