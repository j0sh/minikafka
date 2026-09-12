# MiniKafka

MiniKafka is an embeddable, single-node Kafka-compatible broker for tests,
local development, and small-scale apps. It speaks a focused subset
of the Kafka protocol and can be backed by in-memory or persistent storage.

## Install

```sh
go get github.com/j0sh/minikafka
```

## SQLite Backend

Use SQLite when records, topics, and committed consumer offsets should survive
broker restarts. SQLite durability settings are configured on the SQLite store
itself.

```go
store, err := sqlite.Open(
	"./minikafka.db",
	sqlite.WithSynchronous(sqlite.SyncFull),
)
if err != nil {
	log.Fatal(err)
}

broker, err := minikafka.Open(minikafka.Config{
	Addr:             "127.0.0.1:9092",
	Store:            store,
	AutoCreateTopics: true,
})
```

## In-Memory Backend

Use the memory store for fast tests or ephemeral workloads where data can
disappear when the broker stops.

```go
package main

import (
	"context"
	"log"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, err := minikafka.Open(minikafka.Config{
		Addr:             "127.0.0.1:0",
		Store:            memory.Open(),
		AutoCreateTopics: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	go func() {
		if err := broker.Serve(ctx); err != nil {
			log.Printf("broker stopped: %v", err)
		}
	}()

	log.Printf("broker listening at %s", broker.Addr())
}
```

`Open` binds the TCP listener synchronously and returns any bind error. `Addr()`
immediately returns the actual bound address, including the assigned port when
using `:0`, and remains unchanged after shutdown. Always call `Close` after a
successful `Open`, even if `Serve` is never called or returns an error. If `Open`
fails, it leaves the supplied store untouched.

Call `Serve(ctx)` once per broker to initialize the store and accept connections.
Connections can queue on the bound listener before initialization finishes, but
requests are handled only afterward. Binding does not mean the store is ready for
direct access. `Close` releases both the listener and the store.

## Topics, Retention, and Offsets

Topics can be created explicitly:

```go
err := broker.CreateTopic(ctx, "events", minikafka.TopicOptions{
	Retention: minikafka.RetentionPolicy{
		MaxMessages: 10000,
	},
})
```

When `AutoCreateTopics` is enabled, producing to an unknown topic or requesting
metadata for it creates the topic with `DefaultRetention`. This mirrors Kafka's
common broker-level auto-create behavior. When `AutoCreateTopics` is disabled,
unknown topics return Kafka's `UNKNOWN_TOPIC_OR_PARTITION` error.

`DefaultRetention` is a `RetentionPolicy` applied to auto-created topics. Its
zero value has no limits: records are retained forever unless a topic-specific
retention policy or non-zero default is configured. The available limits are:

- `MaxAge`: delete records older than this duration
- `MaxBytes`: delete oldest records until the topic is at or below this size
- `MaxMessages`: keep only the newest N records

Retention is enforced manually by calling:

```go
err := broker.ApplyRetention(ctx)
```

`ApplyRetention` lists all topics and asks the configured store to apply each
topic's policy. It does not run in the background, so applications can call it
from a timer, maintenance job, or test step when cleanup should happen.

Consumer offsets are persisted through Kafka `OffsetCommit` requests from
Kafka clients. The broker also exposes `ResetConsumerOffset` as a direct helper
for tests and embedded administration code:

```go
err := broker.ResetConsumerOffset(ctx, "group-a", "events", 42)
```

## Limitations

- Single broker only
- Single-partition topic semantics
- Focused Kafka protocol subset for common produce, fetch, metadata, list
  offsets, and offset commit/fetch flows
- Intended to feel like Kafka without the distributed-system pieces: no broker
  replication, controller quorum, partition reassignment, or consumer group
  coordination

## Test

```sh
go test ./...
```
