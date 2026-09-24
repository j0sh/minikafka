package minikafka_test

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/saslauthenticate"
	"github.com/segmentio/kafka-go/protocol/saslhandshake"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	segmentScram "github.com/segmentio/kafka-go/sasl/scram"
	"github.com/twmb/franz-go/pkg/kgo"
	franzPlain "github.com/twmb/franz-go/pkg/sasl/plain"
	xdgScram "github.com/xdg-go/scram"
)

func TestOpenRejectsInvalidSASLConfig(t *testing.T) {
	tests := []minikafka.SASLConfig{
		{Mechanisms: []minikafka.SASLMechanism{"UNKNOWN"}, Users: map[string]string{"alice": "password"}},
		{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain, minikafka.SASLPlain}, Users: map[string]string{"alice": "password"}},
		{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLSCRAMSHA512, minikafka.SASLSCRAMSHA512}, Users: map[string]string{"alice": "password"}},
		{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}},
		{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"": "password"}},
		{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"alice": ""}},
		{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLSCRAMSHA512}, Users: map[string]string{"alice": "bad\x00password"}},
	}
	for _, sasl := range tests {
		store := &lifecycleStore{}
		b, err := minikafka.Open(minikafka.Config{Store: store, SASL: &sasl})
		if b != nil {
			_ = b.Close()
			t.Fatalf("Open returned broker for %+v", sasl)
		}
		if !errors.Is(err, minikafka.ErrInvalidSASLConfig) {
			t.Fatalf("Open error = %v for %+v", err, sasl)
		}
		if store.initCalls.Load() != 0 || store.closeCalls.Load() != 0 {
			t.Fatal("invalid config touched the store")
		}
	}
}

func TestSASLEmptyMechanismsDefaultsToSCRAMSHA512(t *testing.T) {
	b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), SASL: &minikafka.SASLConfig{
		Users: map[string]string{"alice": "secret"},
	}})
	conn := authTestConn(t, b.Addr())
	res := authTestExchange(t, conn, 1, &saslhandshake.Request{Mechanism: "PLAIN"}).(*saslhandshake.Response)
	if res.ErrorCode != 33 || len(res.Mechanisms) != 1 || res.Mechanisms[0] != "SCRAM-SHA-512" {
		t.Fatalf("default handshake = %+v", res)
	}
	assertAuthConnectionClosed(t, conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	authenticated, err := (&kafka.Dialer{Timeout: time.Second, SASLMechanism: segmentMechanism(t, minikafka.SASLSCRAMSHA512, "alice", "secret")}).DialContext(ctx, "tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	_ = authenticated.Close()
}

func TestSASLMultipleMechanisms(t *testing.T) {
	mechanisms := []minikafka.SASLMechanism{minikafka.SASLSCRAMSHA512, minikafka.SASLPlain}
	users := map[string]string{"alice": "first-secret", "bob": "second-secret"}
	b := startBrokerWithConfig(t, minikafka.Config{
		Store: memory.Open(), AutoCreateTopics: true,
		SASL: &minikafka.SASLConfig{Mechanisms: mechanisms, Users: users},
	})
	mechanisms[0] = "CHANGED"
	users["alice"] = "changed-after-open"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	plainDialer := &kafka.Dialer{Timeout: time.Second, SASLMechanism: segmentMechanism(t, minikafka.SASLPlain, "alice", "first-secret")}
	producer, err := plainDialer.DialLeader(ctx, "tcp", b.Addr(), "both-mechanisms", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if _, err := producer.WriteMessages(kafka.Message{Value: []byte("message")}); err != nil {
		t.Fatal(err)
	}

	scramDialer := &kafka.Dialer{Timeout: time.Second, SASLMechanism: segmentMechanism(t, minikafka.SASLSCRAMSHA512, "bob", "second-secret")}
	consumer, err := scramDialer.DialLeader(ctx, "tcp", b.Addr(), "both-mechanisms", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	if _, err := consumer.Seek(0, kafka.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := consumer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	message, err := consumer.ReadMessage(1 << 20)
	if err != nil || string(message.Value) != "message" {
		t.Fatalf("message = %+v, error = %v", message, err)
	}

	conn := authTestConn(t, b.Addr())
	res := authTestExchange(t, conn, 1, &saslhandshake.Request{Mechanism: "SCRAM-SHA-256"}).(*saslhandshake.Response)
	if res.ErrorCode != 33 || len(res.Mechanisms) != 2 || res.Mechanisms[0] != "SCRAM-SHA-512" || res.Mechanisms[1] != "PLAIN" {
		t.Fatalf("enabled mechanisms = %+v", res)
	}
	assertAuthConnectionClosed(t, conn)
}

func TestSASLClientsProduceAndFetch(t *testing.T) {
	for _, mechanism := range []minikafka.SASLMechanism{minikafka.SASLPlain, minikafka.SASLSCRAMSHA512} {
		t.Run(string(mechanism), func(t *testing.T) {
			users := map[string]string{"alice": "first-secret", "bob": "second-secret"}
			b := startBrokerWithConfig(t, minikafka.Config{
				Store: memory.Open(), AutoCreateTopics: true,
				SASL: &minikafka.SASLConfig{Mechanisms: []minikafka.SASLMechanism{mechanism}, Users: users},
			})
			users["alice"] = "changed-after-open"
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			producerDialer := &kafka.Dialer{Timeout: time.Second, SASLMechanism: segmentMechanism(t, mechanism, "alice", "first-secret")}
			consumerDialer := &kafka.Dialer{Timeout: time.Second, SASLMechanism: segmentMechanism(t, mechanism, "bob", "second-secret")}
			topic := "sasl-" + string(mechanism)
			producer, err := producerDialer.DialLeader(ctx, "tcp", b.Addr(), topic, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer producer.Close()
			if _, err := producer.WriteMessages(kafka.Message{Value: []byte("message")}); err != nil {
				t.Fatal(err)
			}
			consumer, err := consumerDialer.DialLeader(ctx, "tcp", b.Addr(), topic, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()
			if _, err := consumer.Seek(0, kafka.SeekStart); err != nil {
				t.Fatal(err)
			}
			if err := consumer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			msg, err := consumer.ReadMessage(1 << 20)
			if err != nil || string(msg.Value) != "message" {
				t.Fatalf("message = %+v, error = %v", msg, err)
			}
		})
	}
}

func TestSASLRejectsCredentials(t *testing.T) {
	for _, mechanism := range []minikafka.SASLMechanism{minikafka.SASLPlain, minikafka.SASLSCRAMSHA512} {
		t.Run(string(mechanism), func(t *testing.T) {
			b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), SASL: &minikafka.SASLConfig{
				Mechanisms: []minikafka.SASLMechanism{mechanism}, Users: map[string]string{"alice": "secret"},
			}})
			for _, credentials := range [][2]string{{"alice", "wrong"}, {"missing", "secret"}} {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				conn, err := (&kafka.Dialer{Timeout: time.Second, SASLMechanism: segmentMechanism(t, mechanism, credentials[0], credentials[1])}).DialContext(ctx, "tcp", b.Addr())
				cancel()
				if err == nil {
					_ = conn.Close()
					t.Fatalf("credentials %q unexpectedly authenticated", credentials[0])
				}
			}
		})
	}
}

func TestSASLFranzPlainProduce(t *testing.T) {
	b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), AutoCreateTopics: true, SASL: &minikafka.SASLConfig{
		Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"alice": "secret"},
	}})
	client, err := kgo.NewClient(
		kgo.SeedBrokers(b.Addr()),
		kgo.SASL(franzPlain.Auth{User: "alice", Pass: "secret"}.AsMechanism()),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: "franz-sasl", Value: []byte("message")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestSASLHandshakeAndFraming(t *testing.T) {
	b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), AutoCreateTopics: true, SASL: &minikafka.SASLConfig{
		Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"alice": "secret"},
	}})
	t.Run("api versions and unsupported mechanism", func(t *testing.T) {
		conn := authTestConn(t, b.Addr())
		res := authTestExchange(t, conn, 2, &apiversions.Request{}).(*apiversions.Response)
		if !hasAPI(res, protocol.SaslHandshake) || !hasAPI(res, protocol.SaslAuthenticate) {
			t.Fatalf("SASL APIs missing: %+v", res.ApiKeys)
		}
		hs := authTestExchange(t, conn, 1, &saslhandshake.Request{Mechanism: "SCRAM-SHA-512"}).(*saslhandshake.Response)
		if hs.ErrorCode != 33 || len(hs.Mechanisms) != 1 || hs.Mechanisms[0] != "PLAIN" {
			t.Fatalf("handshake = %+v", hs)
		}
		assertAuthConnectionClosed(t, conn)
	})
	t.Run("v1 bad payload", func(t *testing.T) {
		conn := authTestConn(t, b.Addr())
		authTestExchange(t, conn, 1, &saslhandshake.Request{Mechanism: "PLAIN"})
		res := authTestExchange(t, conn, 0, &saslauthenticate.Request{AuthBytes: []byte("invalid")}).(*saslauthenticate.Response)
		if res.ErrorCode != 58 {
			t.Fatalf("authenticate = %+v", res)
		}
		assertAuthConnectionClosed(t, conn)
	})
	t.Run("v1 authorization identity", func(t *testing.T) {
		conn := authTestConn(t, b.Addr())
		authTestExchange(t, conn, 1, &saslhandshake.Request{Mechanism: "PLAIN"})
		res := authTestExchange(t, conn, 0, &saslauthenticate.Request{AuthBytes: []byte("bob\x00alice\x00secret")}).(*saslauthenticate.Response)
		if res.ErrorCode != 58 {
			t.Fatalf("authenticate = %+v", res)
		}
		assertAuthConnectionClosed(t, conn)
	})
	t.Run("v1 oversized declared field", func(t *testing.T) {
		conn := authTestConn(t, b.Addr())
		authTestExchange(t, conn, 1, &saslhandshake.Request{Mechanism: "PLAIN"})
		frame := make([]byte, 18)
		binary.BigEndian.PutUint32(frame[:4], 14)
		binary.BigEndian.PutUint16(frame[4:6], uint16(protocol.SaslAuthenticate))
		binary.BigEndian.PutUint32(frame[8:12], 42)
		binary.BigEndian.PutUint32(frame[14:18], 0x7fffffff)
		if _, err := conn.Write(frame); err != nil {
			t.Fatal(err)
		}
		assertAuthConnectionClosed(t, conn)
	})
	t.Run("v0 raw plain", func(t *testing.T) {
		conn := authTestConn(t, b.Addr())
		authTestExchange(t, conn, 0, &saslhandshake.Request{Mechanism: "PLAIN"})
		writeRawToken(t, conn, []byte("\x00alice\x00secret"))
		if token := readRawToken(t, conn); len(token) != 0 {
			t.Fatalf("PLAIN response = %q", token)
		}
		authTestExchange(t, conn, 0, &metadata.Request{TopicNames: []string{"raw-plain"}})
	})
}

func TestSASLRawSCRAMSHA512(t *testing.T) {
	b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), AutoCreateTopics: true, SASL: &minikafka.SASLConfig{
		Mechanisms: []minikafka.SASLMechanism{minikafka.SASLSCRAMSHA512}, Users: map[string]string{"alice": "secret"},
	}})
	conn := authTestConn(t, b.Addr())
	authTestExchange(t, conn, 0, &saslhandshake.Request{Mechanism: "SCRAM-SHA-512"})
	client, err := xdgScram.SHA512.NewClient("alice", "secret", "")
	if err != nil {
		t.Fatal(err)
	}
	conversation := client.NewConversation()
	first, err := conversation.Step("")
	if err != nil {
		t.Fatal(err)
	}
	writeRawToken(t, conn, []byte(first))
	challenge := readRawToken(t, conn)
	final, err := conversation.Step(string(challenge))
	if err != nil {
		t.Fatal(err)
	}
	writeRawToken(t, conn, []byte(final))
	verifier := readRawToken(t, conn)
	if _, err := conversation.Step(string(verifier)); err != nil || !conversation.Valid() {
		t.Fatalf("server verifier = %q, error = %v", verifier, err)
	}
	authTestExchange(t, conn, 0, &metadata.Request{TopicNames: []string{"raw-scram"}})
}

func TestSASLSCRAMMatchingAuthorizationIdentity(t *testing.T) {
	b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), SASL: &minikafka.SASLConfig{
		Mechanisms: []minikafka.SASLMechanism{minikafka.SASLSCRAMSHA512},
		Users:      map[string]string{"alice": "secret", "ali,ce=1": "secret"},
	}})
	for _, user := range []struct{ name, encoded string }{
		{"alice", "alice"},
		{"ali,ce=1", "ali=2Cce=3D1"},
	} {
		for _, handshake := range []struct {
			name    string
			version int16
		}{{"raw", 0}, {"framed", 1}} {
			t.Run(user.name+"/"+handshake.name, func(t *testing.T) {
				conn := authTestConn(t, b.Addr())
				authTestExchange(t, conn, handshake.version, &saslhandshake.Request{Mechanism: "SCRAM-SHA-512"})
				exchange := func(token string) string {
					if handshake.version == 0 {
						writeRawToken(t, conn, []byte(token))
						return string(readRawToken(t, conn))
					}
					res := authTestExchange(t, conn, 1, &saslauthenticate.Request{AuthBytes: []byte(token)}).(*saslauthenticate.Response)
					if res.ErrorCode != 0 {
						t.Fatalf("authenticate = %+v", res)
					}
					return string(res.AuthBytes)
				}
				client, err := xdgScram.SHA512.NewClient(user.name, "secret", user.name)
				if err != nil {
					t.Fatal(err)
				}
				conversation := client.NewConversation()
				first, err := conversation.Step("")
				if err != nil {
					t.Fatal(err)
				}
				// Assert the wire header independently of the shared SCRAM library.
				header := "n,a=" + user.encoded + ","
				if !strings.HasPrefix(first, header) {
					t.Fatalf("client first message = %q, want header %q", first, header)
				}
				final, err := conversation.Step(exchange(first))
				if err != nil {
					t.Fatal(err)
				}
				binding := "c=" + base64.StdEncoding.EncodeToString([]byte(header)) + ","
				if !strings.HasPrefix(final, binding) {
					t.Fatalf("client final message = %q, want binding %q", final, binding)
				}
				if _, err := conversation.Step(exchange(final)); err != nil || !conversation.Valid() {
					t.Fatalf("server verification failed: %v", err)
				}
				authTestExchange(t, conn, 0, &metadata.Request{})
			})
		}
	}
}

func TestSASLRejectsMalformedSCRAMAndMismatchedIdentity(t *testing.T) {
	b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), SASL: &minikafka.SASLConfig{
		Mechanisms: []minikafka.SASLMechanism{minikafka.SASLSCRAMSHA512}, Users: map[string]string{"alice": "secret"},
	}})
	for _, payload := range [][]byte{
		[]byte(",,n=alice,r=nonce"),
		[]byte("n,a=bob,n=alice,r=nonce"),
	} {
		conn := authTestConn(t, b.Addr())
		authTestExchange(t, conn, 1, &saslhandshake.Request{Mechanism: "SCRAM-SHA-512"})
		res := authTestExchange(t, conn, 0, &saslauthenticate.Request{AuthBytes: payload}).(*saslauthenticate.Response)
		if res.ErrorCode != 58 {
			t.Fatalf("authenticate %q = %+v", payload, res)
		}
		assertAuthConnectionClosed(t, conn)
	}
	connWithProof := authTestConn(t, b.Addr())
	authTestExchange(t, connWithProof, 1, &saslhandshake.Request{Mechanism: "SCRAM-SHA-512"})
	client, err := xdgScram.SHA512.NewClient("alice", "secret", "")
	if err != nil {
		t.Fatal(err)
	}
	conversation := client.NewConversation()
	first, err := conversation.Step("")
	if err != nil {
		t.Fatal(err)
	}
	challenge := authTestExchange(t, connWithProof, 0, &saslauthenticate.Request{AuthBytes: []byte(first)}).(*saslauthenticate.Response)
	final, err := conversation.Step(string(challenge.AuthBytes))
	if err != nil {
		t.Fatal(err)
	}
	proofStart := strings.LastIndex(final, ",p=")
	if proofStart < 0 {
		t.Fatalf("SCRAM client final message has no proof: %q", final)
	}
	badProof := final[:proofStart+3] + base64.StdEncoding.EncodeToString(make([]byte, 65))
	res := authTestExchange(t, connWithProof, 0, &saslauthenticate.Request{AuthBytes: []byte(badProof)}).(*saslauthenticate.Response)
	if res.ErrorCode != 58 {
		t.Fatalf("oversized proof response = %+v", res)
	}
	assertAuthConnectionClosed(t, connWithProof)
	// A malformed exchange must not disrupt later connections.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := (&kafka.Dialer{Timeout: time.Second, SASLMechanism: segmentMechanism(t, minikafka.SASLSCRAMSHA512, "alice", "secret")}).DialContext(ctx, "tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

func TestSASLRawRejectsOversizedToken(t *testing.T) {
	b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), SASL: &minikafka.SASLConfig{
		Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"alice": "secret"},
	}})
	conn := authTestConn(t, b.Addr())
	authTestExchange(t, conn, 0, &saslhandshake.Request{Mechanism: "PLAIN"})
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], 1<<20+1)
	if _, err := conn.Write(length[:]); err != nil {
		t.Fatal(err)
	}
	assertAuthConnectionClosed(t, conn)
}

func TestSASLDisabledDoesNotAdvertiseAuthentication(t *testing.T) {
	b := startBroker(t)
	conn := authTestConn(t, b.Addr())
	res := authTestExchange(t, conn, 2, &apiversions.Request{}).(*apiversions.Response)
	if hasAPI(res, protocol.SaslHandshake) || hasAPI(res, protocol.SaslAuthenticate) {
		t.Fatalf("disabled SASL APIs advertised: %+v", res.ApiKeys)
	}
}

func TestSASLRejectsRequestsBeforeAuthentication(t *testing.T) {
	b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), AutoCreateTopics: true, SASL: &minikafka.SASLConfig{
		Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"alice": "secret"},
	}})
	conn := authTestConn(t, b.Addr())
	if err := protocol.WriteRequest(conn, 0, 1, "test", &metadata.Request{TopicNames: []string{"unauthorized"}}); err != nil {
		t.Fatal(err)
	}
	assertAuthConnectionClosed(t, conn)
	topics, err := b.ListTopics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 0 {
		t.Fatalf("unauthenticated request created topics: %+v", topics)
	}
}

func authTestConn(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func segmentMechanism(t *testing.T, mechanism minikafka.SASLMechanism, user, password string) sasl.Mechanism {
	t.Helper()
	if mechanism == minikafka.SASLPlain {
		return plain.Mechanism{Username: user, Password: password}
	}
	m, err := segmentScram.Mechanism(segmentScram.SHA512, user, password)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func authTestExchange(t *testing.T, conn net.Conn, version int16, req protocol.Message) protocol.Message {
	t.Helper()
	if err := protocol.WriteRequest(conn, version, 42, "test", req); err != nil {
		t.Fatal(err)
	}
	id, res, err := protocol.ReadResponse(conn, req.ApiKey(), version)
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("correlation ID = %d", id)
	}
	return res
}

func hasAPI(res *apiversions.Response, key protocol.ApiKey) bool {
	for _, entry := range res.ApiKeys {
		if entry.ApiKey == int16(key) {
			return true
		}
	}
	return false
}

func assertAuthConnectionClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	var one [1]byte
	_, err := conn.Read(one[:])
	var netErr net.Error
	if err == nil || (errors.As(err, &netErr) && netErr.Timeout()) {
		t.Fatalf("connection remained open: read error = %v", err)
	}
}

func writeRawToken(t *testing.T, conn net.Conn, token []byte) {
	t.Helper()
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(token)))
	if _, err := conn.Write(append(length[:], token...)); err != nil {
		t.Fatal(err)
	}
}

func readRawToken(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	var length [4]byte
	if _, err := io.ReadFull(conn, length[:]); err != nil {
		t.Fatal(err)
	}
	size := binary.BigEndian.Uint32(length[:])
	if size > 1<<20 {
		t.Fatalf("raw token size = %d", size)
	}
	token := make([]byte, size)
	if _, err := io.ReadFull(conn, token); err != nil {
		t.Fatal(err)
	}
	return token
}
