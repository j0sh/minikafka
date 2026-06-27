package minikafka

import "time"

type Config struct {
	Addr             string
	Store            Store
	AutoCreateTopics bool
	DefaultRetention RetentionPolicy
}

type TopicOptions struct {
	Retention RetentionPolicy
}

type TopicMetadata struct {
	Topic     string
	CreatedAt time.Time
	Retention RetentionPolicy
}

type RetentionPolicy struct {
	MaxAge      time.Duration
	MaxBytes    int64
	MaxMessages int64
}
