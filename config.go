package minikafka

import "time"

type Config struct {
	Addr              string
	Store             Store
	AutoCreateTopics  bool
	DefaultPartitions int32
	DefaultRetention  RetentionPolicy
}

type TopicOptions struct {
	Partitions int32
	Retention  RetentionPolicy
}

type TopicMetadata struct {
	Topic      string
	Partitions int32
	CreatedAt  time.Time
	Retention  RetentionPolicy
}

type RetentionPolicy struct {
	MaxAge      time.Duration
	MaxBytes    int64
	MaxMessages int64
}
