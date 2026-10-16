package minikafka_test

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	"github.com/segmentio/kafka-go/protocol/findcoordinator"
	"github.com/segmentio/kafka-go/protocol/metadata"
)

func TestBrokerDiscoveryAddress(t *testing.T) {
	for _, tc := range []struct {
		name           string
		advertisedAddr string
	}{
		{name: "listener default"},
		{name: "IPv4", advertisedAddr: "192.0.2.10:1"},
		{name: "hostname", advertisedAddr: "broker.example.invalid:443"},
		{name: "IPv6", advertisedAddr: "[2001:db8::10]:65535"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := startBrokerWithConfig(t, minikafka.Config{
				Addr:           "127.0.0.1:0",
				AdvertisedAddr: tc.advertisedAddr,
				Store:          memory.Open(),
			})
			addr := b.Addr()
			host, portStr, err := net.SplitHostPort(addr)
			if err != nil || host != "127.0.0.1" {
				t.Fatalf("bound address = %q, err=%v", addr, err)
			}
			if port, err := strconv.Atoi(portStr); err != nil || port <= 0 {
				t.Fatalf("bound port = %q, err=%v", portStr, err)
			}
			wantAddr := tc.advertisedAddr
			if wantAddr == "" {
				wantAddr = addr
			}

			// Query the listener directly so discovery never dials the advertised endpoint.
			conn := authTestConn(t, addr)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for range 10000 {
					if got := b.Addr(); got != addr {
						t.Errorf("Addr changed from %q to %q", addr, got)
						return
					}
				}
			}()
			for version := int16(0); version <= 8; version++ {
				t.Run("Metadata/v"+strconv.Itoa(int(version)), func(t *testing.T) {
					res := authTestExchange(t, conn, version, &metadata.Request{}).(*metadata.Response)
					if len(res.Brokers) != 1 {
						t.Fatalf("metadata brokers = %+v", res.Brokers)
					}
					broker := res.Brokers[0]
					if broker.NodeID != 1 || net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port))) != wantAddr {
						t.Fatalf("metadata broker = %+v, want node 1 at %s", broker, wantAddr)
					}
				})
			}
			for version := int16(0); version <= 2; version++ {
				t.Run("FindCoordinator/v"+strconv.Itoa(int(version)), func(t *testing.T) {
					res := authTestExchange(t, conn, version, &findcoordinator.Request{Key: "discovery-test"}).(*findcoordinator.Response)
					if res.ErrorCode != 0 || res.NodeID != 1 || net.JoinHostPort(res.Host, strconv.Itoa(int(res.Port))) != wantAddr {
						t.Fatalf("coordinator = %+v, want node 1 at %s", res, wantAddr)
					}
				})
			}
			if got := b.Addr(); got != addr {
				t.Fatalf("Addr during Serve = %q, want %q", got, addr)
			}
			_ = conn.Close()
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			<-done
			if got := b.Addr(); got != addr {
				t.Fatalf("Addr after Close = %q, want %q", got, addr)
			}
		})
	}
}

func TestOpenRejectsInvalidAdvertisedAddress(t *testing.T) {
	// An occupied bind address also verifies validation precedes binding.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	for _, addr := range []string{
		"broker.example.invalid",
		"broker.example.invalid:",
		":9092",
		":",
		"2001:db8::1:9092",
		"[2001:db8::1:9092",
		"broker.example.invalid:https",
		"broker.example.invalid:0",
		"broker.example.invalid:-1",
		"broker.example.invalid:65536",
		"broker.example.invalid:999999999999999999999",
	} {
		t.Run(addr, func(t *testing.T) {
			store := &lifecycleStore{}
			b, err := minikafka.Open(minikafka.Config{
				Addr:           ln.Addr().String(),
				AdvertisedAddr: addr,
				Store:          store,
			})
			if b != nil {
				_ = b.Close()
				t.Fatal("Open returned a broker for an invalid advertised address")
			}
			if err == nil || !strings.Contains(err.Error(), "invalid advertised address") {
				t.Fatalf("Open error = %v, want an advertised address validation error", err)
			}
			if store.initCalls.Load() != 0 || store.closeCalls.Load() != 0 {
				t.Fatal("failed Open touched the store")
			}
		})
	}
}
