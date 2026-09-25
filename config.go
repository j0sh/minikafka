package minikafka

import "time"

type Config struct {
	Addr              string
	Store             Store
	SASL              *SASLConfig
	Authorization     *AuthorizationConfig
	AutoCreateTopics  bool
	DefaultPartitions int32
	DefaultRetention  RetentionPolicy
}

type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "PLAIN"
	SASLSCRAMSHA512 SASLMechanism = "SCRAM-SHA-512"
)

// SASLConfig enables the listed mechanisms for all TCP client connections.
// An empty Mechanisms list defaults to SCRAM-SHA-512. Users maps usernames to
// passwords. Both fields are copied when Open is called.
type SASLConfig struct {
	Mechanisms []SASLMechanism
	Users      map[string]string
}

// AuthorizationConfig enables deny-by-default topic permissions for SASL users.
// A nil config leaves Kafka TCP clients unrestricted. Open copies the grants.
type AuthorizationConfig struct {
	Grants []TopicGrant
}

type TopicAction string

const (
	TopicRead  TopicAction = "read"
	TopicWrite TopicAction = "write"
	TopicAll   TopicAction = "all"
)

// TopicGrant permits an action on a topic. "*" matches every user or topic;
// TopicAll permits both read and write. Grants are additive.
type TopicGrant struct {
	User   string
	Topic  string
	Action TopicAction
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
