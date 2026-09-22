package minikafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/fetch"
	"github.com/segmentio/kafka-go/protocol/findcoordinator"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/offsetcommit"
	"github.com/segmentio/kafka-go/protocol/offsetfetch"
	"github.com/segmentio/kafka-go/protocol/produce"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	kerrNone                    int16 = 0
	kerrOffsetOutOfRange        int16 = 1
	kerrUnknownTopicOrPartition int16 = 3
	kerrUnsupportedVersion      int16 = 35
)

type Broker struct {
	cfg       Config
	ln        net.Listener
	addr      string
	host      string
	port      int32
	closeCh   chan struct{}
	closeOnce sync.Once
	waitMu    sync.Mutex
	waiters   map[topicPartition]map[chan struct{}]struct{}
}

type topicPartition struct {
	topic     string
	partition int32
}

// Open binds the configured TCP address synchronously. The caller must Close the
// broker even if Serve is never called or fails. On failure, the store is untouched.
func Open(cfg Config) (*Broker, error) {
	if cfg.Store == nil {
		return nil, ErrNoStoreConfigured
	}
	if cfg.DefaultPartitions < 0 {
		return nil, ErrInvalidPartition
	}
	if cfg.DefaultPartitions == 0 {
		cfg.DefaultPartitions = 1
	}
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}
	addr := ln.Addr().String()
	host, port := splitAddr(addr)
	return &Broker{
		cfg:     cfg,
		ln:      ln,
		addr:    addr,
		host:    host,
		port:    port,
		closeCh: make(chan struct{}),
		waiters: make(map[topicPartition]map[chan struct{}]struct{}),
	}, nil
}

// Addr returns the bound address, including the resolved port. It is available
// immediately after Open and remains unchanged, including after Close.
func (b *Broker) Addr() string {
	return b.addr
}

// Serve initializes the store and accepts connections until Close or context
// cancellation. Call Serve only once per broker. Open binds the listener but does
// not initialize the store; queued connections are handled after initialization.
func (b *Broker) Serve(ctx context.Context) error {
	if err := b.cfg.Store.Init(ctx); err != nil {
		return err
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = b.Close()
		case <-b.closeCh:
		}
	}()

	for {
		conn, err := b.ln.Accept()
		if err != nil {
			select {
			case <-b.closeCh:
				return nil
			default:
				return err
			}
		}
		go b.serveConn(ctx, conn)
	}
}

// Close releases the listener and store, including when Serve has not started.
// Shutdown is best-effort: Close does not close active client connections or
// wait for in-flight requests or direct broker operations. Callers should stop
// clients and broker operations before calling Close.
func (b *Broker) Close() error {
	var err error
	b.closeOnce.Do(func() {
		close(b.closeCh)
		err = b.ln.Close()
		if storeErr := b.cfg.Store.Close(); err == nil {
			err = storeErr
		}
	})
	return err
}

func (b *Broker) CreateTopic(ctx context.Context, topic string, opts TopicOptions) error {
	if opts.Partitions < 0 {
		return ErrInvalidPartition
	}
	if opts.Partitions == 0 {
		opts.Partitions = 1
	}
	return b.cfg.Store.CreateTopic(ctx, topic, opts)
}

func (b *Broker) DeleteTopic(ctx context.Context, topic string) error {
	return b.cfg.Store.DeleteTopic(ctx, topic)
}

func (b *Broker) ListTopics(ctx context.Context) ([]TopicMetadata, error) {
	return b.cfg.Store.ListTopics(ctx)
}

func (b *Broker) ApplyRetention(ctx context.Context) error {
	topics, err := b.cfg.Store.ListTopics(ctx)
	if err != nil {
		return err
	}
	for _, topic := range topics {
		if err := b.cfg.Store.ApplyRetention(ctx, topic.Topic); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) ResetConsumerOffset(ctx context.Context, groupID, topic string, partition int32, offset int64) error {
	return b.cfg.Store.CommitOffset(ctx, CommitOffsetRequest{GroupID: groupID, Topic: topic, Partition: partition, Offset: offset})
}

func (b *Broker) Publish(ctx context.Context, topic string, key, value []byte, headers ...Header) (int64, error) {
	if err := b.ensureTopic(ctx, topic); err != nil {
		return 0, err
	}
	meta, err := b.cfg.Store.Topic(ctx, topic)
	if err != nil {
		return 0, err
	}
	partitions := make([]int, meta.Partitions)
	for i := range partitions {
		partitions[i] = i
	}
	partition := int32((kafka.Murmur2Balancer{}).Balance(kafka.Message{Key: key, Value: value}, partitions...))
	return b.PublishToPartition(ctx, topic, partition, key, value, headers...)
}

func (b *Broker) PublishToPartition(ctx context.Context, topic string, partition int32, key, value []byte, headers ...Header) (int64, error) {
	if err := b.ensureTopic(ctx, topic); err != nil {
		return 0, err
	}
	res, err := b.cfg.Store.Append(ctx, AppendRequest{Topic: topic, Partition: partition, Records: []Record{{
		Timestamp: time.Now(),
		Key:       append([]byte(nil), key...),
		Value:     append([]byte(nil), value...),
		Headers:   cloneHeaders(headers),
	}}})
	if err != nil {
		return 0, err
	}
	b.wake(topic, partition)
	return res.BaseOffset, nil
}

func (b *Broker) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	for {
		apiVersion, correlationID, _, msg, err := protocol.ReadRequest(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				_ = conn.SetDeadline(time.Now())
			}
			return
		}
		if req, ok := msg.(*fetch.Request); ok {
			res, err := b.handleFetch(ctx, req)
			if err != nil || writeFetchResponse(conn, apiVersion, correlationID, res) != nil {
				return
			}
			continue
		}
		res := b.handle(ctx, apiVersion, msg)
		if res == nil {
			continue
		}
		if err := protocol.WriteResponse(conn, apiVersion, correlationID, res); err != nil {
			return
		}
	}
}

func (b *Broker) handle(ctx context.Context, apiVersion int16, msg protocol.Message) protocol.Message {
	switch req := msg.(type) {
	case *apiversions.Request:
		return &apiversions.Response{ApiKeys: []apiversions.ApiKeyResponse{
			{ApiKey: int16(protocol.ApiVersions), MinVersion: 0, MaxVersion: 2},
			{ApiKey: int16(protocol.Metadata), MinVersion: 0, MaxVersion: 8},
			{ApiKey: int16(protocol.Produce), MinVersion: 0, MaxVersion: 8},
			{ApiKey: int16(protocol.Fetch), MinVersion: 0, MaxVersion: 11},
			{ApiKey: int16(protocol.ListOffsets), MinVersion: 1, MaxVersion: 5},
			{ApiKey: int16(protocol.FindCoordinator), MinVersion: 0, MaxVersion: 2},
			{ApiKey: int16(protocol.OffsetCommit), MinVersion: 0, MaxVersion: 7},
			{ApiKey: int16(protocol.OffsetFetch), MinVersion: 0, MaxVersion: 5},
		}}
	case *metadata.Request:
		return b.handleMetadata(ctx, req)
	case *produce.Request:
		if req.Acks == 0 {
			_ = b.handleProduce(ctx, req)
			return nil
		}
		return b.handleProduce(ctx, req)
	case *listoffsets.Request:
		return b.handleListOffsets(ctx, req)
	case *findcoordinator.Request:
		return &findcoordinator.Response{NodeID: 1, Host: b.host, Port: b.port}
	case *offsetcommit.Request:
		return b.handleOffsetCommit(ctx, req)
	case *offsetfetch.Request:
		return b.handleOffsetFetch(ctx, req)
	default:
		_ = apiVersion
		return &apiversions.Response{ErrorCode: kerrUnsupportedVersion}
	}
}

func (b *Broker) handleMetadata(ctx context.Context, req *metadata.Request) *metadata.Response {
	names := req.TopicNames
	if names == nil {
		topics, _ := b.cfg.Store.ListTopics(ctx)
		for _, topic := range topics {
			names = append(names, topic.Topic)
		}
	}
	res := &metadata.Response{
		Brokers:      []metadata.ResponseBroker{{NodeID: 1, Host: b.host, Port: b.port}},
		ClusterID:    "minikafka",
		ControllerID: 1,
	}
	for _, name := range names {
		errCode := kerrNone
		if err := b.ensureTopic(ctx, name); err != nil {
			errCode = kerrUnknownTopicOrPartition
		}
		t := metadata.ResponseTopic{ErrorCode: errCode, Name: name}
		if errCode == kerrNone {
			meta, err := b.cfg.Store.Topic(ctx, name)
			if err != nil {
				t.ErrorCode = kerrUnknownTopicOrPartition
			} else {
				for partition := int32(0); partition < meta.Partitions; partition++ {
					t.Partitions = append(t.Partitions, metadata.ResponsePartition{
						PartitionIndex: partition,
						LeaderID:       1,
						LeaderEpoch:    0,
						ReplicaNodes:   []int32{1},
						IsrNodes:       []int32{1},
					})
				}
			}
		}
		res.Topics = append(res.Topics, t)
	}
	return res
}

func (b *Broker) handleProduce(ctx context.Context, req *produce.Request) *produce.Response {
	res := &produce.Response{}
	for _, topicReq := range req.Topics {
		topicRes := produce.ResponseTopic{Topic: topicReq.Topic}
		for _, partReq := range topicReq.Partitions {
			partRes := produce.ResponsePartition{Partition: partReq.Partition, BaseOffset: -1}
			if err := b.ensureTopic(ctx, topicReq.Topic); err != nil {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
				topicRes.Partitions = append(topicRes.Partitions, partRes)
				continue
			}
			records, err := recordsFromKafka(partReq.RecordSet.Records)
			if err != nil {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
				topicRes.Partitions = append(topicRes.Partitions, partRes)
				continue
			}
			app, err := b.cfg.Store.Append(ctx, AppendRequest{Topic: topicReq.Topic, Partition: partReq.Partition, Records: records})
			if err != nil {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
				topicRes.Partitions = append(topicRes.Partitions, partRes)
				continue
			}
			partRes.BaseOffset = app.BaseOffset
			b.wake(topicReq.Topic, partReq.Partition)
			topicRes.Partitions = append(topicRes.Partitions, partRes)
		}
		res.Topics = append(res.Topics, topicRes)
	}
	return res
}

func (b *Broker) handleFetch(ctx context.Context, req *fetch.Request) (*kmsg.FetchResponse, error) {
	deadline := time.Now().Add(time.Duration(req.MaxWaitTime) * time.Millisecond)
	// Subscribe before reading so an append between a read and the wait is seen.
	wait, unregister := b.registerWaiter(req)
	defer unregister()
	timeout := time.NewTimer(time.Until(deadline))
	defer timeout.Stop()
	for {
		res := kmsg.NewPtrFetchResponse()
		var size int64
		var partitions int
		var failed bool
		for _, topicReq := range req.Topics {
			topicRes := kmsg.FetchResponseTopic{Topic: topicReq.Topic}
			for _, partReq := range topicReq.Partitions {
				partRes, err := b.fetchPartition(ctx, topicReq.Topic, partReq)
				if err != nil {
					return nil, err
				}
				topicRes.Partitions = append(topicRes.Partitions, partRes)
				size += int64(len(partRes.RecordBatches))
				partitions++
				failed = failed || partRes.ErrorCode != kerrNone
			}
			res.Topics = append(res.Topics, topicRes)
		}
		if failed || partitions == 0 || size >= int64(req.MinBytes) || !time.Now().Before(deadline) {
			return res, nil
		}
		select {
		case <-ctx.Done():
			return res, nil
		case <-b.closeCh:
			return res, nil
		case <-wait:
		case <-timeout.C:
			// Read once more at the deadline to return the latest available data.
		}
	}
}

func (b *Broker) fetchPartition(ctx context.Context, topic string, partReq fetch.RequestPartition) (kmsg.FetchResponseTopicPartition, error) {
	partRes := kmsg.NewFetchResponseTopicPartition()
	partRes.Partition = partReq.Partition
	maxBytes := partReq.PartitionMaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	fr, err := b.cfg.Store.Fetch(ctx, FetchRequest{Topic: topic, Partition: partReq.Partition, Offset: partReq.FetchOffset, MaxBytes: maxBytes, MaxRecords: 1000})
	if err != nil {
		if errors.Is(err, ErrOffsetOutOfRange) {
			partRes.ErrorCode = kerrOffsetOutOfRange
		} else {
			partRes.ErrorCode = kerrUnknownTopicOrPartition
		}
		return partRes, nil
	}
	partRes.HighWatermark = fr.HighWatermark
	partRes.LastStableOffset = fr.HighWatermark
	partRes.LogStartOffset = fr.EarliestOffset
	partRes.RecordBatches, err = encodeFetchRecords(fr.Records)
	return partRes, err
}

func (b *Broker) handleListOffsets(ctx context.Context, req *listoffsets.Request) *listoffsets.Response {
	res := &listoffsets.Response{}
	for _, topicReq := range req.Topics {
		topicRes := listoffsets.ResponseTopic{Topic: topicReq.Topic}
		for _, partReq := range topicReq.Partitions {
			partRes := listoffsets.ResponsePartition{Partition: partReq.Partition, Timestamp: partReq.Timestamp, LeaderEpoch: -1}
			var err error
			switch partReq.Timestamp {
			case -2:
				partRes.Offset, err = b.cfg.Store.EarliestOffset(ctx, topicReq.Topic, partReq.Partition)
			case -1:
				partRes.Offset, err = b.cfg.Store.LatestOffset(ctx, topicReq.Topic, partReq.Partition)
			default:
				partRes.Offset, err = b.cfg.Store.EarliestOffset(ctx, topicReq.Topic, partReq.Partition)
			}
			if err != nil {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
			}
			topicRes.Partitions = append(topicRes.Partitions, partRes)
		}
		res.Topics = append(res.Topics, topicRes)
	}
	return res
}

func (b *Broker) handleOffsetCommit(ctx context.Context, req *offsetcommit.Request) *offsetcommit.Response {
	res := &offsetcommit.Response{}
	for _, topicReq := range req.Topics {
		topicRes := offsetcommit.ResponseTopic{Name: topicReq.Name}
		for _, partReq := range topicReq.Partitions {
			partRes := offsetcommit.ResponsePartition{PartitionIndex: partReq.PartitionIndex}
			if err := b.cfg.Store.CommitOffset(ctx, CommitOffsetRequest{GroupID: req.GroupID, Topic: topicReq.Name, Partition: partReq.PartitionIndex, Offset: partReq.CommittedOffset}); err != nil {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
			}
			topicRes.Partitions = append(topicRes.Partitions, partRes)
		}
		res.Topics = append(res.Topics, topicRes)
	}
	return res
}

func (b *Broker) handleOffsetFetch(ctx context.Context, req *offsetfetch.Request) *offsetfetch.Response {
	res := &offsetfetch.Response{}
	for _, topicReq := range req.Topics {
		topicRes := offsetfetch.ResponseTopic{Name: topicReq.Name}
		for _, part := range topicReq.PartitionIndexes {
			partRes := offsetfetch.ResponsePartition{PartitionIndex: part, CommittedOffset: -1}
			off, err := b.cfg.Store.FetchOffset(ctx, FetchOffsetRequest{GroupID: req.GroupID, Topic: topicReq.Name, Partition: part})
			if err != nil {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
			} else if off.Found {
				partRes.CommittedOffset = off.Offset
			}
			topicRes.Partitions = append(topicRes.Partitions, partRes)
		}
		res.Topics = append(res.Topics, topicRes)
	}
	return res
}

func (b *Broker) ensureTopic(ctx context.Context, topic string) error {
	if _, err := b.cfg.Store.Topic(ctx, topic); err == nil {
		return nil
	}
	if !b.cfg.AutoCreateTopics {
		return ErrTopicNotFound
	}
	err := b.cfg.Store.CreateTopic(ctx, topic, TopicOptions{Partitions: b.cfg.DefaultPartitions, Retention: b.cfg.DefaultRetention})
	if errors.Is(err, ErrTopicExists) {
		return nil
	}
	return err
}

func recordsFromKafka(rr protocol.RecordReader) ([]Record, error) {
	var records []Record
	for {
		rec, err := rr.ReadRecord()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		key, err := protocol.ReadAll(rec.Key)
		if err != nil {
			return nil, err
		}
		value, err := protocol.ReadAll(rec.Value)
		if err != nil {
			return nil, err
		}
		ts := rec.Time
		if ts.IsZero() {
			ts = time.Now()
		}
		headers := make([]Header, len(rec.Headers))
		for i, h := range rec.Headers {
			headers[i] = Header{Key: h.Key, Value: append([]byte(nil), h.Value...)}
		}
		records = append(records, Record{Timestamp: ts, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...), Headers: headers})
	}
}

func cloneHeaders(headers []Header) []Header {
	out := make([]Header, len(headers))
	for i, h := range headers {
		out[i] = Header{Key: h.Key, Value: append([]byte(nil), h.Value...)}
	}
	return out
}

func splitAddr(addr string) (string, int32) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1", 0
	}
	if host == "" || strings.HasPrefix(host, "[::]") || host == "::" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(portStr)
	return host, int32(port)
}

func (b *Broker) registerWaiter(req *fetch.Request) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	b.waitMu.Lock()
	for _, topic := range req.Topics {
		for _, part := range topic.Partitions {
			key := topicPartition{topic: topic.Topic, partition: part.Partition}
			if b.waiters[key] == nil {
				b.waiters[key] = make(map[chan struct{}]struct{})
			}
			b.waiters[key][ch] = struct{}{}
		}
	}
	b.waitMu.Unlock()
	return ch, func() {
		b.waitMu.Lock()
		defer b.waitMu.Unlock()
		for _, topic := range req.Topics {
			for _, part := range topic.Partitions {
				key := topicPartition{topic: topic.Topic, partition: part.Partition}
				delete(b.waiters[key], ch)
				if len(b.waiters[key]) == 0 {
					delete(b.waiters, key)
				}
			}
		}
	}
}

func (b *Broker) wake(topic string, partition int32) {
	b.waitMu.Lock()
	defer b.waitMu.Unlock()
	for ch := range b.waiters[topicPartition{topic: topic, partition: partition}] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (b *Broker) String() string {
	return fmt.Sprintf("minikafka://%s", b.Addr())
}
