package minikafka_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/fetch"
	"github.com/segmentio/kafka-go/protocol/listoffsets"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/offsetcommit"
	"github.com/segmentio/kafka-go/protocol/offsetfetch"
	"github.com/segmentio/kafka-go/protocol/produce"
	"github.com/segmentio/kafka-go/protocol/saslauthenticate"
	"github.com/segmentio/kafka-go/protocol/saslhandshake"
)

func TestAuthorizationConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sasl   *minikafka.SASLConfig
		grants []minikafka.TopicGrant
	}{
		{name: "requires SASL"},
		{name: "empty user", sasl: &minikafka.SASLConfig{Users: map[string]string{"alice": "secret"}}, grants: []minikafka.TopicGrant{{Topic: "orders", Action: minikafka.TopicRead}}},
		{name: "empty topic", sasl: &minikafka.SASLConfig{Users: map[string]string{"alice": "secret"}}, grants: []minikafka.TopicGrant{{User: "alice", Action: minikafka.TopicRead}}},
		{name: "unknown user", sasl: &minikafka.SASLConfig{Users: map[string]string{"alice": "secret"}}, grants: []minikafka.TopicGrant{{User: "bob", Topic: "orders", Action: minikafka.TopicRead}}},
		{name: "invalid action", sasl: &minikafka.SASLConfig{Users: map[string]string{"alice": "secret"}}, grants: []minikafka.TopicGrant{{User: "alice", Topic: "orders", Action: "create"}}},
		{name: "reserved wildcard user", sasl: &minikafka.SASLConfig{Users: map[string]string{"*": "secret"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := minikafka.Open(minikafka.Config{Store: memory.Open(), SASL: tc.sasl, Authorization: &minikafka.AuthorizationConfig{Grants: tc.grants}})
			if b != nil {
				_ = b.Close()
				t.Fatal("Open accepted invalid authorization")
			}
			if !errors.Is(err, minikafka.ErrInvalidAuthorizationConfig) {
				t.Fatalf("Open error = %v", err)
			}
		})
	}
}

func permissionConn(t *testing.T, b *minikafka.Broker, user string) net.Conn {
	t.Helper()
	conn := authTestConn(t, b.Addr())
	handshake := authTestExchange(t, conn, 1, &saslhandshake.Request{Mechanism: "PLAIN"}).(*saslhandshake.Response)
	if handshake.ErrorCode != 0 {
		t.Fatalf("handshake = %+v", handshake)
	}
	auth := authTestExchange(t, conn, 1, &saslauthenticate.Request{AuthBytes: []byte("\x00" + user + "\x00secret")}).(*saslauthenticate.Response)
	if auth.ErrorCode != 0 {
		t.Fatalf("authenticate = %+v", auth)
	}
	return conn
}

func permissionProduce(t *testing.T, conn net.Conn, topic string, acks int16) *produce.Response {
	t.Helper()
	req := &produce.Request{Acks: acks, Topics: []produce.RequestTopic{{
		Topic: topic, Partitions: []produce.RequestPartition{{Partition: 0, RecordSet: protocol.RecordSet{
			Records: kafka.NewRecordReader(kafka.Record{Value: kafka.NewBytes([]byte("value"))}),
		}}},
	}}}
	req.Prepare(8)
	if acks == 0 {
		if err := protocol.WriteRequest(conn, 8, 42, "permissions", req); err != nil {
			t.Fatal(err)
		}
		return nil
	}
	return authTestExchange(t, conn, 8, req).(*produce.Response)
}

func permissionListOffset(t *testing.T, conn net.Conn, topic string) int16 {
	t.Helper()
	res := authTestExchange(t, conn, 5, &listoffsets.Request{Topics: []listoffsets.RequestTopic{{
		Topic: topic, Partitions: []listoffsets.RequestPartition{{Partition: 0, Timestamp: -1}},
	}}}).(*listoffsets.Response)
	return res.Topics[0].Partitions[0].ErrorCode
}

func TestAuthorizationGrantMatching(t *testing.T) {
	for _, tc := range []struct {
		name        string
		grants      []minikafka.TopicGrant
		user, topic string
		read, write bool
	}{
		{"empty policy", nil, "alice", "orders", false, false},
		{"specific read", []minikafka.TopicGrant{{"alice", "orders", minikafka.TopicRead}}, "alice", "orders", true, false},
		{"specific write", []minikafka.TopicGrant{{"alice", "orders", minikafka.TopicWrite}}, "alice", "orders", false, true},
		{"all actions", []minikafka.TopicGrant{{"alice", "orders", minikafka.TopicAll}}, "alice", "orders", true, true},
		{"combined actions", []minikafka.TopicGrant{{"alice", "orders", minikafka.TopicRead}, {"alice", "orders", minikafka.TopicWrite}}, "alice", "orders", true, true},
		{"all users", []minikafka.TopicGrant{{"*", "orders", minikafka.TopicRead}}, "bob", "orders", true, false},
		{"all topics", []minikafka.TopicGrant{{"alice", "*", minikafka.TopicWrite}}, "alice", "orders", false, true},
		{"combined", []minikafka.TopicGrant{{"*", "orders", minikafka.TopicRead}, {"alice", "*", minikafka.TopicWrite}}, "alice", "orders", true, true},
		{"global all", []minikafka.TopicGrant{{"*", "*", minikafka.TopicAll}}, "bob", "orders", true, true},
		{"other user denied", []minikafka.TopicGrant{{"alice", "orders", minikafka.TopicAll}}, "bob", "orders", false, false},
		{"other topic denied", []minikafka.TopicGrant{{"alice", "audit", minikafka.TopicAll}}, "alice", "orders", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), SASL: &minikafka.SASLConfig{
				Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"alice": "secret", "bob": "secret"},
			}, Authorization: &minikafka.AuthorizationConfig{Grants: tc.grants}})
			if len(tc.grants) > 0 {
				// The broker must keep the permissions it read at Open.
				tc.grants[0] = minikafka.TopicGrant{User: "bob", Topic: "changed", Action: minikafka.TopicAll}
			}
			if err := b.CreateTopic(context.Background(), "orders", minikafka.TopicOptions{}); err != nil {
				t.Fatal(err)
			}
			conn := permissionConn(t, b, tc.user)
			readCode := int16(29)
			if tc.read {
				readCode = 0
			}
			if code := permissionListOffset(t, conn, tc.topic); code != readCode {
				t.Fatalf("read code = %d, want %d", code, readCode)
			}
			res := permissionProduce(t, conn, tc.topic, 1)
			writeCode := int16(29)
			if tc.write {
				writeCode = 0
			}
			if code := res.Topics[0].Partitions[0].ErrorCode; code != writeCode {
				t.Fatalf("write code = %d, want %d", code, writeCode)
			}
		})
	}
}

func TestAuthorizationMetadataAndAutoCreation(t *testing.T) {
	store := memory.Open()
	b := startBrokerWithConfig(t, minikafka.Config{Store: store, AutoCreateTopics: true,
		SASL: &minikafka.SASLConfig{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"alice": "secret"}},
		Authorization: &minikafka.AuthorizationConfig{Grants: []minikafka.TopicGrant{
			{"alice", "read", minikafka.TopicRead}, {"alice", "missing-read", minikafka.TopicRead},
			{"alice", "write", minikafka.TopicWrite}, {"alice", "new-write", minikafka.TopicWrite},
			{"alice", "legacy-write", minikafka.TopicWrite},
		}},
	})
	for _, name := range []string{"read", "write", "hidden"} {
		if err := b.CreateTopic(context.Background(), name, minikafka.TopicOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	conn := permissionConn(t, b, "alice")
	all := authTestExchange(t, conn, 8, &metadata.Request{}).(*metadata.Response)
	if len(all.Topics) != 2 {
		t.Fatalf("all topics = %+v", all.Topics)
	}
	for _, topic := range all.Topics {
		if topic.Name != "read" && topic.Name != "write" {
			t.Fatalf("unexpected topic in metadata: %+v", topic)
		}
	}
	named := authTestExchange(t, conn, 8, &metadata.Request{TopicNames: []string{"read", "write", "hidden", "denied"}, AllowAutoTopicCreation: true}).(*metadata.Response)
	for i, want := range []int16{0, 0, 29, 29} {
		if named.Topics[i].ErrorCode != want {
			t.Fatalf("named topic %s code = %d, want %d", named.Topics[i].Name, named.Topics[i].ErrorCode, want)
		}
	}
	for _, tc := range []struct {
		version        int16
		name           string
		allow, created bool
		code           int16
	}{
		{8, "missing-read", true, false, 3},
		{8, "new-write", false, false, 3},
		{8, "new-write", true, true, 0},
		{3, "legacy-write", false, true, 0},
		{8, "denied", true, false, 29},
	} {
		res := authTestExchange(t, conn, tc.version, &metadata.Request{TopicNames: []string{tc.name}, AllowAutoTopicCreation: tc.allow}).(*metadata.Response)
		if len(res.Topics) != 1 || res.Topics[0].ErrorCode != tc.code {
			t.Fatalf("metadata %q v%d = %+v", tc.name, tc.version, res.Topics)
		}
		_, err := store.Topic(context.Background(), tc.name)
		if (err == nil) != tc.created {
			t.Fatalf("topic %q created = %v, want %v", tc.name, err == nil, tc.created)
		}
	}
}

func TestAuthorizationOffsetsFetchAndNoSideEffects(t *testing.T) {
	store := memory.Open()
	b := startBrokerWithConfig(t, minikafka.Config{Store: store, AutoCreateTopics: true,
		SASL:          &minikafka.SASLConfig{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"alice": "secret"}},
		Authorization: &minikafka.AuthorizationConfig{Grants: []minikafka.TopicGrant{{"alice", "read", minikafka.TopicRead}, {"alice", "write", minikafka.TopicWrite}}},
	})
	ctx := context.Background()
	for _, topic := range []string{"read", "write", "hidden"} {
		if _, err := b.Publish(ctx, topic, nil, []byte("record")); err != nil {
			t.Fatal(err)
		}
	}
	conn := permissionConn(t, b, "alice")
	listed := authTestExchange(t, conn, 5, &listoffsets.Request{Topics: []listoffsets.RequestTopic{
		{Topic: "read", Partitions: []listoffsets.RequestPartition{{Partition: 0, Timestamp: -1}}},
		{Topic: "hidden", Partitions: []listoffsets.RequestPartition{{Partition: 0, Timestamp: -1}}},
	}}).(*listoffsets.Response)
	if listed.Topics[0].Partitions[0].ErrorCode != 0 || listed.Topics[1].Partitions[0].ErrorCode != 29 {
		t.Fatalf("ListOffsets = %+v", listed.Topics)
	}
	commit := authTestExchange(t, conn, 7, &offsetcommit.Request{GroupID: "group", Topics: []offsetcommit.RequestTopic{
		{Name: "read", Partitions: []offsetcommit.RequestPartition{{PartitionIndex: 0, CommittedOffset: 1}}},
		{Name: "hidden", Partitions: []offsetcommit.RequestPartition{{PartitionIndex: 0, CommittedOffset: 2}}},
	}}).(*offsetcommit.Response)
	if commit.Topics[0].Partitions[0].ErrorCode != 0 || commit.Topics[1].Partitions[0].ErrorCode != 29 {
		t.Fatalf("OffsetCommit = %+v", commit.Topics)
	}
	deniedOffset, err := store.FetchOffset(ctx, minikafka.FetchOffsetRequest{GroupID: "group", Topic: "hidden"})
	if err != nil || deniedOffset.Found {
		t.Fatalf("denied offset stored: %+v, %v", deniedOffset, err)
	}
	offsets := authTestExchange(t, conn, 5, &offsetfetch.Request{GroupID: "group", Topics: []offsetfetch.RequestTopic{
		{Name: "read", PartitionIndexes: []int32{0}}, {Name: "hidden", PartitionIndexes: []int32{0}},
	}}).(*offsetfetch.Response)
	if offsets.Topics[0].Partitions[0].CommittedOffset != 1 || offsets.Topics[0].Partitions[0].ErrorCode != 0 || offsets.Topics[1].Partitions[0].ErrorCode != 29 {
		t.Fatalf("OffsetFetch = %+v", offsets.Topics)
	}
	fetched := authTestExchange(t, conn, 11, &fetch.Request{MaxWaitTime: 0, Topics: []fetch.RequestTopic{
		{Topic: "read", Partitions: []fetch.RequestPartition{{Partition: 0, FetchOffset: 0, PartitionMaxBytes: 1024}}},
		{Topic: "hidden", Partitions: []fetch.RequestPartition{{Partition: 0, FetchOffset: 0, PartitionMaxBytes: 1024}}},
	}}).(*fetch.Response)
	if fetched.Topics[0].Partitions[0].ErrorCode != 0 || fetched.Topics[1].Partitions[0].ErrorCode != 29 || fetched.Topics[1].Partitions[0].RecordSet.Records != nil {
		t.Fatalf("Fetch = %+v", fetched.Topics)
	}
	produceReq := &produce.Request{Acks: 1, Topics: []produce.RequestTopic{
		{Topic: "write", Partitions: []produce.RequestPartition{{Partition: 0, RecordSet: protocol.RecordSet{Records: kafka.NewRecordReader(kafka.Record{Value: kafka.NewBytes([]byte("allowed"))})}}}},
		{Topic: "hidden", Partitions: []produce.RequestPartition{{Partition: 0, RecordSet: protocol.RecordSet{Records: kafka.NewRecordReader(kafka.Record{Value: kafka.NewBytes([]byte("denied"))})}}}},
	}}
	produceReq.Prepare(8)
	produced := authTestExchange(t, conn, 8, produceReq).(*produce.Response)
	if produced.Topics[0].Partitions[0].ErrorCode != 0 || produced.Topics[1].Partitions[0].ErrorCode != 29 {
		t.Fatalf("Produce = %+v", produced.Topics)
	}
	permissionProduce(t, conn, "unlisted", 0)
	assertAuthConnectionClosed(t, conn)
	if _, err := store.Topic(ctx, "unlisted"); !errors.Is(err, minikafka.ErrTopicNotFound) {
		t.Fatalf("acks=0 created topic: %v", err)
	}
	if latest, err := store.LatestOffset(ctx, "hidden", 0); err != nil || latest != 1 {
		t.Fatalf("denied Produce changed records: latest=%d, err=%v", latest, err)
	}
	if latest, err := store.LatestOffset(ctx, "write", 0); err != nil || latest != 2 {
		t.Fatalf("allowed Produce missing records: latest=%d, err=%v", latest, err)
	}
}

func TestAuthorizationProduceAcksZero(t *testing.T) {
	for _, tc := range []struct {
		name      string
		topic     string
		partition int32
		wantClose bool
	}{
		{"allowed", "write", 0, false},
		{"denied topic", "hidden", 0, true},
		{"invalid partition", "write", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memory.Open()
			b := startBrokerWithConfig(t, minikafka.Config{Store: store,
				SASL:          &minikafka.SASLConfig{Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain}, Users: map[string]string{"alice": "secret"}},
				Authorization: &minikafka.AuthorizationConfig{Grants: []minikafka.TopicGrant{{"alice", "write", minikafka.TopicWrite}}},
			})
			for _, topic := range []string{"write", "hidden"} {
				if err := b.CreateTopic(t.Context(), topic, minikafka.TopicOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			conn := permissionConn(t, b, "alice")
			req := &produce.Request{Acks: 0, Topics: []produce.RequestTopic{{
				Topic: tc.topic, Partitions: []produce.RequestPartition{{Partition: tc.partition, RecordSet: protocol.RecordSet{
					Records: kafka.NewRecordReader(kafka.Record{Value: kafka.NewBytes([]byte("value"))}),
				}}},
			}}}
			req.Prepare(8)
			if err := protocol.WriteRequest(conn, 8, 41, "permissions", req); err != nil {
				t.Fatal(err)
			}
			var wantOffset int64
			if tc.wantClose {
				assertAuthConnectionClosed(t, conn)
			} else {
				// A subsequent response confirms the write completed without a
				// Produce response or a closed connection.
				authTestExchange(t, conn, 8, &metadata.Request{TopicNames: []string{tc.topic}})
				wantOffset = 1
			}
			if offset, err := store.LatestOffset(t.Context(), tc.topic, 0); err != nil || offset != wantOffset {
				t.Fatalf("latest offset = %d, want %d, err=%v", offset, wantOffset, err)
			}
		})
	}
}

func TestAuthorizationIdentityAcrossHandshakes(t *testing.T) {
	// SCRAM removes the soft hyphen and escapes the comma and equals sign.
	const user = "a\u00adb,=z"
	for _, mechanism := range []minikafka.SASLMechanism{minikafka.SASLPlain, minikafka.SASLSCRAMSHA512} {
		for _, version := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/v%d", mechanism, version), func(t *testing.T) {
				b := startBrokerWithConfig(t, minikafka.Config{Store: memory.Open(), AutoCreateTopics: true,
					SASL: &minikafka.SASLConfig{Mechanisms: []minikafka.SASLMechanism{mechanism}, Users: map[string]string{user: "secret", "bob": "secret"}},
					Authorization: &minikafka.AuthorizationConfig{Grants: []minikafka.TopicGrant{
						{user, "allowed", minikafka.TopicWrite}, {"bob", "bob-only", minikafka.TopicAll},
					}},
				})
				conn := authTestConn(t, b.Addr())
				handshake := authTestExchange(t, conn, version, &saslhandshake.Request{Mechanism: string(mechanism)}).(*saslhandshake.Response)
				if handshake.ErrorCode != 0 {
					t.Fatalf("handshake = %+v", handshake)
				}
				session, token, err := segmentMechanism(t, mechanism, user, "secret").Start(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				for {
					var challenge []byte
					if version == 0 {
						writeRawToken(t, conn, token)
						challenge = readRawToken(t, conn)
					} else {
						res := authTestExchange(t, conn, 1, &saslauthenticate.Request{AuthBytes: token}).(*saslauthenticate.Response)
						if res.ErrorCode != 0 {
							t.Fatalf("authenticate = %+v", res)
						}
						challenge = res.AuthBytes
					}
					done, next, err := session.Next(t.Context(), challenge)
					if err != nil {
						t.Fatal(err)
					}
					if done {
						break
					}
					token = next
				}
				for range 2 {
					res := permissionProduce(t, conn, "allowed", 1)
					if code := res.Topics[0].Partitions[0].ErrorCode; code != 0 {
						t.Fatalf("configured identity lost: error code = %d", code)
					}
					res = permissionProduce(t, conn, "bob-only", 1)
					if code := res.Topics[0].Partitions[0].ErrorCode; code != 29 {
						t.Fatalf("other user's topic accessible: error code = %d", code)
					}
				}
			})
		}
	}
}

func TestAuthorizationSCRAMPrincipalAndDisabledMode(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[enabled], func(t *testing.T) {
			cfg := minikafka.Config{Store: memory.Open(), AutoCreateTopics: true, SASL: &minikafka.SASLConfig{
				Users: map[string]string{"alice": "secret", "bob": "secret"},
			}}
			if enabled {
				cfg.Authorization = &minikafka.AuthorizationConfig{Grants: []minikafka.TopicGrant{{"alice", "events", minikafka.TopicWrite}, {"bob", "events", minikafka.TopicRead}}}
			}
			b := startBrokerWithConfig(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			alice := &kafka.Dialer{Timeout: time.Second, SASLMechanism: segmentMechanism(t, minikafka.SASLSCRAMSHA512, "alice", "secret")}
			writer, err := alice.DialLeader(ctx, "tcp", b.Addr(), "events", 0)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			if _, err := writer.WriteMessages(kafka.Message{Value: []byte("message")}); err != nil {
				t.Fatal(err)
			}
			bob := &kafka.Dialer{Timeout: time.Second, SASLMechanism: segmentMechanism(t, minikafka.SASLSCRAMSHA512, "bob", "secret")}
			reader, err := bob.DialLeader(ctx, "tcp", b.Addr(), "events", 0)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if err := reader.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			message, err := reader.ReadMessage(1024)
			if err != nil || string(message.Value) != "message" {
				t.Fatalf("SCRAM read = %+v, %v", message, err)
			}
			if enabled {
				if _, err := alice.DialLeader(ctx, "tcp", b.Addr(), "other", 0); err == nil {
					t.Fatal("alice discovered unlisted topic")
				}
			} else {
				other, err := alice.DialLeader(ctx, "tcp", b.Addr(), "other", 0)
				if err != nil {
					t.Fatal(err)
				}
				_ = other.Close()
			}
		})
	}
}
