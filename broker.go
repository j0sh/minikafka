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
	waiters   map[string][]chan struct{}
}

// Open binds the configured TCP address synchronously. The caller must Close the
// broker even if Serve is never called or fails. On failure, the store is untouched.
func Open(cfg Config) (*Broker, error) {
	if cfg.Store == nil {
		return nil, ErrNoStoreConfigured
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
		waiters: make(map[string][]chan struct{}),
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
func (b *Broker) Close() error {
	var err error
	b.closeOnce.Do(func() {
		close(b.closeCh)
		b.wakeAll()
		err = b.ln.Close()
		if storeErr := b.cfg.Store.Close(); err == nil {
			err = storeErr
		}
	})
	return err
}

func (b *Broker) CreateTopic(ctx context.Context, topic string, opts TopicOptions) error {
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

func (b *Broker) ResetConsumerOffset(ctx context.Context, groupID, topic string, offset int64) error {
	return b.cfg.Store.CommitOffset(ctx, CommitOffsetRequest{GroupID: groupID, Topic: topic, Offset: offset})
}

func (b *Broker) Publish(ctx context.Context, topic string, key, value []byte, headers ...Header) (int64, error) {
	if err := b.ensureTopic(ctx, topic); err != nil {
		return 0, err
	}
	res, err := b.cfg.Store.Append(ctx, AppendRequest{Topic: topic, Records: []Record{{
		Timestamp: time.Now(),
		Key:       append([]byte(nil), key...),
		Value:     append([]byte(nil), value...),
		Headers:   cloneHeaders(headers),
	}}})
	if err != nil {
		return 0, err
	}
	b.wake(topic)
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
			t.Partitions = []metadata.ResponsePartition{{
				PartitionIndex: 0,
				LeaderID:       1,
				LeaderEpoch:    0,
				ReplicaNodes:   []int32{1},
				IsrNodes:       []int32{1},
			}}
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
			if partReq.Partition != 0 {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
				topicRes.Partitions = append(topicRes.Partitions, partRes)
				continue
			}
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
			app, err := b.cfg.Store.Append(ctx, AppendRequest{Topic: topicReq.Topic, Records: records})
			if err != nil {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
				topicRes.Partitions = append(topicRes.Partitions, partRes)
				continue
			}
			partRes.BaseOffset = app.BaseOffset
			b.wake(topicReq.Topic)
			topicRes.Partitions = append(topicRes.Partitions, partRes)
		}
		res.Topics = append(res.Topics, topicRes)
	}
	return res
}

func (b *Broker) handleFetch(ctx context.Context, req *fetch.Request) (*kmsg.FetchResponse, error) {
	res := kmsg.NewPtrFetchResponse()
	for _, topicReq := range req.Topics {
		topicRes := kmsg.FetchResponseTopic{Topic: topicReq.Topic}
		for _, partReq := range topicReq.Partitions {
			partRes, err := b.fetchPartition(ctx, topicReq.Topic, partReq, req.MaxWaitTime)
			if err != nil {
				return nil, err
			}
			topicRes.Partitions = append(topicRes.Partitions, partRes)
		}
		res.Topics = append(res.Topics, topicRes)
	}
	return res, nil
}

func (b *Broker) fetchPartition(ctx context.Context, topic string, partReq fetch.RequestPartition, maxWaitMs int32) (kmsg.FetchResponseTopicPartition, error) {
	partRes := kmsg.NewFetchResponseTopicPartition()
	partRes.Partition = partReq.Partition
	if partReq.Partition != 0 {
		partRes.ErrorCode = kerrUnknownTopicOrPartition
		return partRes, nil
	}
	maxBytes := partReq.PartitionMaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	for {
		fr, err := b.cfg.Store.Fetch(ctx, FetchRequest{Topic: topic, Offset: partReq.FetchOffset, MaxBytes: maxBytes, MaxRecords: 1000})
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
		if len(fr.Records) > 0 || maxWaitMs <= 0 || partReq.FetchOffset < fr.LatestOffset {
			partRes.RecordBatches, err = encodeFetchRecords(fr.Records)
			return partRes, err
		}
		wait := b.registerWaiter(topic)
		timeout := time.NewTimer(time.Duration(maxWaitMs) * time.Millisecond)
		select {
		case <-ctx.Done():
			timeout.Stop()
			return partRes, nil
		case <-b.closeCh:
			timeout.Stop()
			return partRes, nil
		case <-wait:
			timeout.Stop()
		case <-timeout.C:
			return partRes, nil
		}
	}
}

func (b *Broker) handleListOffsets(ctx context.Context, req *listoffsets.Request) *listoffsets.Response {
	res := &listoffsets.Response{}
	for _, topicReq := range req.Topics {
		topicRes := listoffsets.ResponseTopic{Topic: topicReq.Topic}
		for _, partReq := range topicReq.Partitions {
			partRes := listoffsets.ResponsePartition{Partition: partReq.Partition, Timestamp: partReq.Timestamp, LeaderEpoch: -1}
			if partReq.Partition != 0 {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
			} else {
				switch partReq.Timestamp {
				case -2:
					partRes.Offset, _ = b.cfg.Store.EarliestOffset(ctx, topicReq.Topic)
				case -1:
					partRes.Offset, _ = b.cfg.Store.LatestOffset(ctx, topicReq.Topic)
				default:
					partRes.Offset, _ = b.cfg.Store.EarliestOffset(ctx, topicReq.Topic)
				}
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
			if partReq.PartitionIndex != 0 {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
			} else if err := b.cfg.Store.CommitOffset(ctx, CommitOffsetRequest{GroupID: req.GroupID, Topic: topicReq.Name, Offset: partReq.CommittedOffset}); err != nil {
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
			if part != 0 {
				partRes.ErrorCode = kerrUnknownTopicOrPartition
			} else if off, err := b.cfg.Store.FetchOffset(ctx, FetchOffsetRequest{GroupID: req.GroupID, Topic: topicReq.Name}); err == nil && off.Found {
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
	err := b.cfg.Store.CreateTopic(ctx, topic, TopicOptions{Retention: b.cfg.DefaultRetention})
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

func (b *Broker) registerWaiter(topic string) chan struct{} {
	ch := make(chan struct{})
	b.waitMu.Lock()
	b.waiters[topic] = append(b.waiters[topic], ch)
	b.waitMu.Unlock()
	return ch
}

func (b *Broker) wake(topic string) {
	b.waitMu.Lock()
	waiters := b.waiters[topic]
	delete(b.waiters, topic)
	b.waitMu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

func (b *Broker) wakeAll() {
	b.waitMu.Lock()
	defer b.waitMu.Unlock()
	for topic, waiters := range b.waiters {
		for _, ch := range waiters {
			close(ch)
		}
		delete(b.waiters, topic)
	}
}

func (b *Broker) String() string {
	return fmt.Sprintf("minikafka://%s", b.Addr())
}
