package minikafka

import (
	"context"
	"time"
)

type Store interface {
	Init(ctx context.Context) error
	Close() error

	CreateTopic(ctx context.Context, topic string, opts TopicOptions) error
	DeleteTopic(ctx context.Context, topic string) error
	Topic(ctx context.Context, topic string) (TopicMetadata, error)
	ListTopics(ctx context.Context) ([]TopicMetadata, error)

	Append(ctx context.Context, req AppendRequest) (AppendResult, error)
	Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)

	CommitOffset(ctx context.Context, req CommitOffsetRequest) error
	FetchOffset(ctx context.Context, req FetchOffsetRequest) (FetchOffsetResult, error)

	EarliestOffset(ctx context.Context, topic string) (int64, error)
	LatestOffset(ctx context.Context, topic string) (int64, error)

	ApplyRetention(ctx context.Context, topic string) error
}

type Record struct {
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
}

type Header struct {
	Key   string
	Value []byte
}

type AppendRequest struct {
	Topic   string
	Records []Record
}

type AppendResult struct {
	BaseOffset int64
	LastOffset int64
}

type FetchRequest struct {
	Topic      string
	Offset     int64
	MaxBytes   int32
	MaxRecords int
}

type FetchResult struct {
	Records        []Record
	HighWatermark  int64
	EarliestOffset int64
	LatestOffset   int64
}

type CommitOffsetRequest struct {
	GroupID string
	Topic   string
	Offset  int64
}

type FetchOffsetRequest struct {
	GroupID string
	Topic   string
}

type FetchOffsetResult struct {
	Offset int64
	Found  bool
}
