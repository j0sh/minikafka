package minikafka

import "errors"

var (
	ErrTopicNotFound     = errors.New("topic not found")
	ErrTopicExists       = errors.New("topic already exists")
	ErrInvalidPartition  = errors.New("invalid partition")
	ErrOffsetOutOfRange  = errors.New("offset out of range")
	ErrNoStoreConfigured = errors.New("no store configured")
	ErrInvalidSASLConfig = errors.New("invalid SASL config")
)
