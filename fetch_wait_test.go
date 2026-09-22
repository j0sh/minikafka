package minikafka

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/segmentio/kafka-go/protocol/fetch"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type fetchTestStore struct {
	Store
	read func(FetchRequest) (FetchResult, error)
}

func (s fetchTestStore) Fetch(_ context.Context, req FetchRequest) (FetchResult, error) {
	return s.read(req)
}

func newFetchTestBroker(read func(FetchRequest) (FetchResult, error)) (*Broker, *fetch.Request) {
	b := &Broker{
		cfg: Config{Store: fetchTestStore{read: read}}, closeCh: make(chan struct{}),
		waiters: make(map[topicPartition]map[chan struct{}]struct{}),
	}
	req := &fetch.Request{MaxWaitTime: 1000, MinBytes: 1, Topics: []fetch.RequestTopic{{
		Topic: "events", Partitions: []fetch.RequestPartition{{Partition: 0}, {Partition: 1}, {Partition: 2}},
	}}}
	return b, req
}

func TestFetchRequestWaiting(t *testing.T) {
	for _, tc := range []struct {
		name      string
		populated []int32
		minBytes  int32
		maxWait   int32
		err       error
		code      int16
		elapsed   time.Duration
	}{
		{name: "empty", minBytes: 1, maxWait: 1000, elapsed: time.Second},
		{name: "last_partition_ready", populated: []int32{2}, minBytes: 1, maxWait: 1000},
		{name: "aggregate_bytes", populated: []int32{0, 2}, minBytes: 250, maxWait: 1000},
		{name: "below_minimum", populated: []int32{2}, minBytes: 250, maxWait: 1000, elapsed: time.Second},
		{name: "zero_minimum", maxWait: 1000},
		{name: "zero_wait", minBytes: 1},
		{name: "invalid_partition", minBytes: 250, maxWait: 1000, err: ErrInvalidPartition, code: kerrUnknownTopicOrPartition},
		{name: "expired_offset", minBytes: 250, maxWait: 1000, err: ErrOffsetOutOfRange, code: kerrOffsetOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b, req := newFetchTestBroker(func(req FetchRequest) (FetchResult, error) {
					if req.Partition == 2 && tc.err != nil {
						return FetchResult{}, tc.err
					}
					if slices.Contains(tc.populated, req.Partition) {
						return FetchResult{Records: []Record{{Value: make([]byte, 100)}}, HighWatermark: 1}, nil
					}
					return FetchResult{}, nil
				})
				req.MinBytes, req.MaxWaitTime = tc.minBytes, tc.maxWait
				start := time.Now()
				res, err := b.handleFetch(context.Background(), req)
				if err != nil || time.Since(start) != tc.elapsed {
					t.Fatalf("fetch elapsed=%s, want %s, err=%v", time.Since(start), tc.elapsed, err)
				}
				if len(res.Topics) != 1 || len(res.Topics[0].Partitions) != 3 || res.Topics[0].Partitions[2].ErrorCode != tc.code {
					t.Fatalf("unexpected response: %+v", res)
				}
				var populated int
				for _, p := range res.Topics[0].Partitions {
					if len(p.RecordBatches) > 0 {
						populated++
					}
				}
				if populated != len(tc.populated) || len(b.waiters) != 0 {
					t.Fatalf("populated partitions=%d, remaining waiter keys=%d", populated, len(b.waiters))
				}
			})
		})
	}
}

func TestFetchWakeups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var records [3][]Record
		b, req := newFetchTestBroker(func(req FetchRequest) (FetchResult, error) {
			return FetchResult{Records: records[req.Partition]}, nil
		})
		// Two requests share partitions but have different minimum byte counts.
		// Completing one must leave the other registered for later appends.
		first, second := make(chan *kmsg.FetchResponse, 1), make(chan *kmsg.FetchResponse, 1)
		for i, done := range []chan *kmsg.FetchResponse{first, second} {
			copy := *req
			copy.MinBytes = int32(1 + i*249)
			go func() {
				res, err := b.handleFetch(context.Background(), &copy)
				if err != nil {
					t.Error(err)
				}
				done <- res
			}()
		}
		synctest.Wait()
		if len(first) != 0 || len(second) != 0 {
			t.Fatal("empty fetch returned before its deadline")
		}
		records[2] = []Record{{Value: make([]byte, 100)}}
		b.wake("events", 2)
		synctest.Wait()
		if len(first) != 1 || len(second) != 0 {
			t.Fatal("wake did not respect each request's minimum bytes")
		}
		records[0] = []Record{{Value: make([]byte, 100)}}
		b.wake("events", 0)
		synctest.Wait()
		if len(second) != 1 || len(b.waiters) != 0 {
			t.Fatal("remaining fetch was not woken or left registered waiters")
		}
		res := <-second
		if res == nil || len(res.Topics[0].Partitions[0].RecordBatches) == 0 || len(res.Topics[0].Partitions[2].RecordBatches) == 0 {
			t.Fatal("fetch omitted records from a requested partition")
		}
	})
}

func TestFetchWakeDoesNotExtendDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, req := newFetchTestBroker(func(FetchRequest) (FetchResult, error) { return FetchResult{}, nil })
		go func() {
			for range 3 {
				time.Sleep(300 * time.Millisecond)
				b.wake("events", 2)
			}
		}()
		start := time.Now()
		if _, err := b.handleFetch(context.Background(), req); err != nil || time.Since(start) != time.Second {
			t.Fatalf("fetch elapsed=%s, err=%v", time.Since(start), err)
		}
	})
}

func TestFetchAppendDuringRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var b *Broker
		appended := false
		b, req := newFetchTestBroker(func(req FetchRequest) (FetchResult, error) {
			if req.Partition == 2 {
				if appended {
					return FetchResult{Records: []Record{{Value: []byte("new")}}}, nil
				}
				// The first read saw no records, but an append completed before it returned.
				appended = true
				b.wake("events", 2)
			}
			return FetchResult{}, nil
		})
		start := time.Now()
		res, err := b.handleFetch(context.Background(), req)
		if err != nil || time.Since(start) != 0 || len(res.Topics[0].Partitions[2].RecordBatches) == 0 {
			t.Fatalf("missed append: elapsed=%s, err=%v", time.Since(start), err)
		}
	})
}

func TestFetchCancellation(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			b, req := newFetchTestBroker(func(FetchRequest) (FetchResult, error) { return FetchResult{}, nil })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				if _, err := b.handleFetch(ctx, req); err != nil {
					t.Error(err)
				}
			}()
			synctest.Wait()
			if shutdown {
				close(b.closeCh)
			} else {
				cancel()
			}
			synctest.Wait()
			select {
			case <-done:
			default:
				t.Fatal("fetch did not stop")
			}
			if len(b.waiters) != 0 {
				t.Fatal("canceled fetch left registered waiters")
			}
		})
	}
}
