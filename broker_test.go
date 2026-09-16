package minikafka_test

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	kafka "github.com/segmentio/kafka-go"
)

type lifecycleStore struct {
	minikafka.Store
	initFn     func(context.Context) error
	initCalls  atomic.Int32
	closeCalls atomic.Int32
}

func (s *lifecycleStore) Init(ctx context.Context) error {
	s.initCalls.Add(1)
	if s.initFn != nil {
		return s.initFn(ctx)
	}
	return nil
}

func (s *lifecycleStore) Close() error {
	s.closeCalls.Add(1)
	return nil
}

func TestOpenBindsBeforeServe(t *testing.T) {
	for _, addr := range []string{"", "127.0.0.1:0"} {
		t.Run("addr="+addr, func(t *testing.T) {
			store := &lifecycleStore{}
			b, err := minikafka.Open(minikafka.Config{Addr: addr, Store: store})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = b.Close() })
			host, port, err := net.SplitHostPort(b.Addr())
			if err != nil || host != "127.0.0.1" {
				t.Fatalf("bound address = %q, err=%v", b.Addr(), err)
			}
			if n, err := strconv.Atoi(port); err != nil || n <= 0 {
				t.Fatalf("bound port = %q, err=%v", port, err)
			}
			if store.initCalls.Load() != 0 || store.closeCalls.Load() != 0 {
				t.Fatal("Open touched the store")
			}
			conn, err := net.DialTimeout("tcp", b.Addr(), time.Second)
			if err != nil {
				t.Fatalf("listener is not bound before Serve: %v", err)
			}
			_ = conn.Close()
		})
	}
}

func TestOpenBindFailureLeavesStoreUntouched(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	store := &lifecycleStore{}
	b, err := minikafka.Open(minikafka.Config{Addr: ln.Addr().String(), Store: store})
	if b != nil {
		_ = b.Close()
		t.Fatal("Open returned a broker for an occupied address")
	}
	if err == nil {
		t.Fatal("Open succeeded for an occupied address")
	}
	if store.initCalls.Load() != 0 || store.closeCalls.Load() != 0 {
		t.Fatal("failed Open touched the store")
	}
}

func TestBrokerAddrStableThroughServeAndClose(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			if got := b.Addr(); got != addr {
				t.Errorf("Addr changed from %q to %q", addr, got)
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := &kafka.Client{Addr: kafka.TCP(addr)}
	res, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Brokers) != 1 || net.JoinHostPort(res.Brokers[0].Host, strconv.Itoa(res.Brokers[0].Port)) != addr {
		t.Fatalf("advertised brokers = %+v, want %s", res.Brokers, addr)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	if got := b.Addr(); got != addr {
		t.Fatalf("Addr after Close = %q, want %q", got, addr)
	}
}

func TestBrokerCloseBeforeServe(t *testing.T) {
	store := &lifecycleStore{}
	b, err := minikafka.Open(minikafka.Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	assertListenerClosed(t, b.Addr())
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if store.initCalls.Load() != 0 || store.closeCalls.Load() != 1 {
		t.Fatalf("store Init calls=%d Close calls=%d", store.initCalls.Load(), store.closeCalls.Load())
	}
}

func TestBrokerConcurrentClose(t *testing.T) {
	initialized := make(chan struct{})
	store := &lifecycleStore{initFn: func(context.Context) error {
		close(initialized)
		return nil
	}}
	b, err := minikafka.Open(minikafka.Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- b.Serve(ctx) }()
	select {
	case <-initialized:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not initialize the store")
	}

	const callers = 64
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- b.Close()
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve after Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop after Close")
	}
	assertListenerClosed(t, b.Addr())
	if store.initCalls.Load() != 1 || store.closeCalls.Load() != 1 {
		t.Fatalf("store Init calls=%d Close calls=%d", store.initCalls.Load(), store.closeCalls.Load())
	}
}

func TestBrokerCloseDuringStoreInit(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	store := &lifecycleStore{initFn: func(context.Context) error {
		close(entered)
		<-release
		return nil
	}}
	b, err := minikafka.Open(minikafka.Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	defer close(release)
	errCh := make(chan error, 1)
	go func() { errCh <- b.Serve(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not enter Init")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	assertListenerClosed(t, b.Addr())
	// Unblock Init without sleeps; Serve must observe the already-closed listener.
	release <- struct{}{}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve after Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop after Init returned")
	}
	if store.closeCalls.Load() != 1 {
		t.Fatalf("store Close calls=%d", store.closeCalls.Load())
	}
}

func TestBrokerCloseAfterInitFailure(t *testing.T) {
	want := errors.New("init failed")
	store := &lifecycleStore{initFn: func(context.Context) error { return want }}
	b, err := minikafka.Open(minikafka.Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Serve(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Serve error = %v, want %v", err, want)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	assertListenerClosed(t, b.Addr())
	if store.closeCalls.Load() != 1 {
		t.Fatalf("store Close calls=%d", store.closeCalls.Load())
	}
}

func assertListenerClosed(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("listener still accepts connections at %s", addr)
	}
}
