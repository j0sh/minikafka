package minikafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/fetch"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestEncodeFetchRecords(t *testing.T) {
	for _, offsets := range [][]int64{{0, 1, 2}, {42, 43, 44}, {math.MaxInt64 - 2, math.MaxInt64 - 1, math.MaxInt64}} {
		records := []Record{
			{Offset: offsets[0], Timestamp: time.UnixMilli(1200), Key: []byte("key"), Value: []byte("one"), Headers: []Header{{Key: "h", Value: []byte("v")}, {Key: "null"}, {Key: "empty", Value: []byte{}}}},
			{Offset: offsets[1], Timestamp: time.UnixMilli(900), Value: []byte("two")},
			{Offset: offsets[2], Timestamp: time.UnixMilli(1100), Value: bytes.Repeat([]byte("x"), 128)},
		}
		raw, err := encodeFetchRecords(records)
		if err != nil {
			t.Fatal(err)
		}
		var gotOffsets []int64
		batches := 0
		for remaining := raw; len(remaining) > 0; batches++ {
			length := int(binary.BigEndian.Uint32(remaining[8:12])) + 12
			wire := remaining[:length]
			remaining = remaining[length:]
			var batch kmsg.RecordBatch
			if err := batch.ReadFrom(wire); err != nil {
				t.Fatal(err)
			}
			if batch.Magic != 2 || batch.Attributes != 0 || batch.Length != int32(len(wire)-12) || uint32(batch.CRC) != crc32.Checksum(wire[21:], crc32.MakeTable(crc32.Castagnoli)) {
				t.Fatalf("invalid batch metadata: %+v", batch)
			}
			if batch.FirstOffset != offsets[0] || batch.NumRecords != int32(len(records)) || batch.PartitionLeaderEpoch != -1 || batch.ProducerID != -1 || batch.ProducerEpoch != -1 || batch.FirstSequence != -1 {
				t.Fatalf("invalid batch offsets, count, or producer metadata: %+v", batch)
			}
			data := batch.Records
			maxTimestamp := int64(math.MinInt64)
			var lastDelta int32
			for n := int32(0); n < batch.NumRecords; n++ {
				length, prefix := binary.Varint(data)
				if prefix <= 0 || length < 0 || length > int64(len(data)-prefix) {
					t.Fatal("invalid record length")
				}
				var rec kmsg.Record
				if err := rec.ReadFrom(data[:prefix+int(length)]); err != nil {
					t.Fatal(err)
				}
				data = data[prefix+int(length):]
				gotOffsets = append(gotOffsets, batch.FirstOffset+int64(rec.OffsetDelta))
				ts := batch.FirstTimestamp + rec.TimestampDelta64
				want := records[n]
				if ts != want.Timestamp.UnixMilli() || !reflect.DeepEqual(rec.Key, want.Key) || !reflect.DeepEqual(rec.Value, want.Value) || len(rec.Headers) != len(want.Headers) {
					t.Fatalf("kmsg decoded record mismatch: %+v, want %+v", rec, want)
				}
				for i, h := range rec.Headers {
					if h.Key != want.Headers[i].Key || !reflect.DeepEqual(h.Value, want.Headers[i].Value) {
						t.Fatal("kmsg decoded header mismatch")
					}
				}
				if ts > maxTimestamp {
					maxTimestamp = ts
				}
				lastDelta = rec.OffsetDelta
			}
			if len(data) != 0 || batch.MaxTimestamp != maxTimestamp || batch.LastOffsetDelta != lastDelta {
				t.Fatal("invalid record count, max timestamp, or last offset delta")
			}
		}
		if !reflect.DeepEqual(gotOffsets, offsets) {
			t.Fatalf("offsets = %v, want %v", gotOffsets, offsets)
		}
		if batches != 1 {
			t.Fatalf("batch count = %d, want 1", batches)
		}
		// Also verify the serialized records with kafka-go's decoder.
		var rs protocol.RecordSet
		framed := binary.BigEndian.AppendUint32(nil, uint32(len(raw)))
		if _, err := rs.ReadFrom(bytes.NewReader(append(framed, raw...))); err != nil {
			t.Fatal(err)
		}
		assertFetchRecords(t, rs.Records, records)
	}
}

func TestEncodeFetchRecordsTimestamps(t *testing.T) {
	for _, tc := range []struct {
		name  string
		times []time.Time
	}{
		{"epoch", []time.Time{time.UnixMilli(0)}},
		{"epoch_in_batch", []time.Time{time.UnixMilli(1000), time.UnixMilli(0), time.UnixMilli(2000)}},
		{"negative", []time.Time{time.UnixMilli(-1), time.UnixMilli(-3), time.UnixMilli(-2)}},
		{"submillisecond", []time.Time{time.Unix(0, -1), time.Unix(0, 1), time.Unix(0, 1999999)}},
		{"outside_unixnano_range", []time.Time{time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{"maximum", []time.Time{time.UnixMilli(math.MaxInt64 - 1), time.UnixMilli(math.MaxInt64)}},
		{"minimum", []time.Time{time.UnixMilli(math.MinInt64), time.UnixMilli(math.MinInt64 + 1)}},
		{"wide_deltas", []time.Time{time.UnixMilli(0), time.UnixMilli(math.MinInt64), time.UnixMilli(math.MaxInt64)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := make([]Record, len(tc.times))
			maxTimestamp := tc.times[0].UnixMilli()
			for i, ts := range tc.times {
				records[i] = Record{
					Offset: int64(42 + i), Timestamp: ts,
					// Growing the timestamp delta also grows this record's length prefix.
					Value: bytes.Repeat([]byte("x"), 56),
				}
				if i == 0 {
					records[i].Key = []byte{}
					records[i].Headers = []Header{{Key: "nil"}, {Key: "empty", Value: []byte{}}, {Key: "h", Value: []byte("v")}}
				}
				maxTimestamp = max(maxTimestamp, ts.UnixMilli())
			}
			raw, err := encodeFetchRecords(records)
			if err != nil {
				t.Fatal(err)
			}
			var batch kmsg.RecordBatch
			if err := batch.ReadFrom(raw); err != nil {
				t.Fatal(err)
			}
			if batch.Length != int32(len(raw)-12) || batch.NumRecords != int32(len(records)) || batch.FirstTimestamp != tc.times[0].UnixMilli() || batch.MaxTimestamp != maxTimestamp {
				t.Fatalf("incorrect batch metadata: %+v", batch)
			}
			if uint32(batch.CRC) != crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)) {
				t.Fatal("incorrect batch CRC")
			}
			data := batch.Records
			for _, want := range records {
				length, prefix := binary.Varint(data)
				if prefix <= 0 || length < 0 || length > int64(len(data)-prefix) {
					t.Fatal("incorrect record length")
				}
				var rec kmsg.Record
				if err := rec.ReadFrom(data[:prefix+int(length)]); err != nil {
					t.Fatal(err)
				}
				data = data[prefix+int(length):]
				if batch.FirstTimestamp+rec.TimestampDelta64 != want.Timestamp.UnixMilli() || batch.FirstOffset+int64(rec.OffsetDelta) != want.Offset || !bytes.Equal(rec.Value, want.Value) || !reflect.DeepEqual(rec.Key, want.Key) || len(rec.Headers) != len(want.Headers) {
					t.Fatalf("incorrect decoded record: %+v, want %+v", rec, want)
				}
				for i, h := range rec.Headers {
					if h.Key != want.Headers[i].Key || !reflect.DeepEqual(h.Value, want.Headers[i].Value) {
						t.Fatal("incorrect decoded header")
					}
				}
			}
			if len(data) != 0 {
				t.Fatal("unexpected trailing record bytes")
			}
			var rs protocol.RecordSet
			framed := binary.BigEndian.AppendUint32(nil, uint32(len(raw)))
			if _, err := rs.ReadFrom(bytes.NewReader(append(framed, raw...))); err != nil {
				t.Fatal(err)
			}
			for i := range records {
				records[i].Timestamp = time.UnixMilli(records[i].Timestamp.UnixMilli())
			}
			assertFetchRecords(t, rs.Records, records)
		})
	}
}

func TestEncodeFetchRecordsTimestampOverflow(t *testing.T) {
	for _, times := range [][2]int64{{math.MinInt64, math.MaxInt64}, {math.MaxInt64, math.MinInt64}} {
		records := []Record{
			{Offset: 42, Timestamp: time.UnixMilli(times[0])},
			{Offset: 43, Timestamp: time.UnixMilli(times[1])},
		}
		if raw, err := encodeFetchRecords(records); err == nil || raw != nil {
			t.Fatalf("timestamp delta overflow: raw=%v err=%v", raw, err)
		}
	}
}

func assertFetchRecords(t *testing.T, reader protocol.RecordReader, want []Record) {
	t.Helper()
	for _, expected := range want {
		r, err := reader.ReadRecord()
		if err != nil {
			t.Fatal(err)
		}
		key, err := protocol.ReadAll(r.Key)
		if err != nil {
			t.Fatal(err)
		}
		value, err := protocol.ReadAll(r.Value)
		if err != nil {
			t.Fatal(err)
		}
		if r.Key != nil {
			r.Key.Close()
		}
		if r.Value != nil {
			r.Value.Close()
		}
		if r.Offset != expected.Offset || !r.Time.Equal(expected.Timestamp) || !bytes.Equal(key, expected.Key) || !bytes.Equal(value, expected.Value) || len(r.Headers) != len(expected.Headers) {
			t.Fatalf("decoded record mismatch: %+v, want %+v", r, expected)
		}
		for i, h := range r.Headers {
			if h.Key != expected.Headers[i].Key || !bytes.Equal(h.Value, expected.Headers[i].Value) {
				t.Fatal("header mismatch")
			}
		}
	}
	if _, err := reader.ReadRecord(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected end of records, got %v", err)
	}
}

func TestEncodeFetchRecordsInvalidAndEmpty(t *testing.T) {
	for _, records := range [][]Record{
		{{Offset: -1}},
		{{Offset: 42}, {Offset: 42}},
		{{Offset: 42}, {Offset: 41}},
		{{Offset: 42}, {Offset: 44}},
		{{Offset: 42}, {Offset: 42 + math.MaxInt32}},
		{{Offset: math.MaxInt64}, {Offset: math.MinInt64}},
		{{Offset: math.MaxInt64}, {Offset: 0}},
	} {
		if _, err := encodeFetchRecords(records); err == nil {
			t.Fatalf("accepted invalid offsets: %v", records)
		}
	}
	for _, records := range [][]Record{nil, {}} {
		if raw, err := encodeFetchRecords(records); err != nil || raw != nil {
			t.Fatalf("empty result = %v, %v", raw, err)
		}
	}
}

func TestEncodeFetchRecordsNullablePayloads(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, []byte("payload")} {
		raw, err := encodeFetchRecords([]Record{{Offset: 42, Timestamp: time.UnixMilli(1234), Key: payload, Value: payload}})
		if err != nil {
			t.Fatal(err)
		}
		var batch kmsg.RecordBatch
		if err := batch.ReadFrom(raw); err != nil {
			t.Fatal(err)
		}
		var rec kmsg.Record
		if err := rec.ReadFrom(batch.Records); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rec.Key, payload) || !reflect.DeepEqual(rec.Value, payload) {
			t.Fatalf("payload = %#v, key = %#v, value = %#v", payload, rec.Key, rec.Value)
		}
	}
}

func TestWriteFetchResponse(t *testing.T) {
	for version := int16(0); version <= 11; version++ {
		records := []Record{{Offset: 42, Timestamp: time.UnixMilli(0), Value: []byte("answer")}}
		raw, err := encodeFetchRecords(records)
		if err != nil {
			t.Fatal(err)
		}
		res := kmsg.NewPtrFetchResponse()
		res.ThrottleMillis = 7
		res.Topics = []kmsg.FetchResponseTopic{{Topic: "events", Partitions: []kmsg.FetchResponseTopicPartition{
			{Partition: 0, HighWatermark: 50, LastStableOffset: 50, LogStartOffset: 40, PreferredReadReplica: -1, RecordBatches: raw},
			{Partition: 1, ErrorCode: kerrUnknownTopicOrPartition},
			{Partition: 0, HighWatermark: 50, LastStableOffset: 50, LogStartOffset: 40},
		}}}
		w := &shortFetchWriter{limit: 3}
		if err := writeFetchResponse(w, version, 123, res); err != nil {
			t.Fatal(err)
		}
		frame := w.Bytes()
		if int(binary.BigEndian.Uint32(frame[:4])) != len(frame)-4 {
			t.Fatal("incorrect response frame length")
		}
		decoded := kmsg.NewPtrFetchResponse()
		decoded.SetVersion(version)
		if err := decoded.ReadFrom(frame[8:]); err != nil || decoded.Topics[0].Partitions[2].RecordBatches != nil {
			t.Fatalf("empty partition must encode null records: %v", err)
		}
		id, msg, err := protocol.ReadResponse(bytes.NewReader(frame), protocol.Fetch, version)
		if err != nil || id != 123 {
			t.Fatalf("v%d: id=%d err=%v", version, id, err)
		}
		got := msg.(*fetch.Response)
		part := got.Topics[0].Partitions[0]
		if part.HighWatermark != 50 || (version >= 4 && part.LastStableOffset != 50) || (version >= 5 && part.LogStartOffset != 40) || (version >= 11 && part.PreferredReadReplica != -1) || got.Topics[0].Partitions[1].ErrorCode != kerrUnknownTopicOrPartition {
			t.Fatalf("v%d: response metadata mismatch: %+v", version, got)
		}
		assertFetchRecords(t, part.RecordSet.Records, records)
	}
}

type shortFetchWriter struct {
	bytes.Buffer
	limit int
}

func (w *shortFetchWriter) Write(p []byte) (int, error) {
	return w.Buffer.Write(p[:min(w.limit, len(p))])
}

func TestWriteFetchResponseErrors(t *testing.T) {
	res := kmsg.NewPtrFetchResponse()
	if err := writeFetchResponse(&shortFetchWriter{}, 11, 0, res); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero write: %v", err)
	}
	for _, version := range []int16{-1, 12} {
		if err := writeFetchResponse(io.Discard, version, 0, res); err == nil {
			t.Fatal("accepted invalid version")
		}
	}
	res.Topics = []kmsg.FetchResponseTopic{{Topic: strings.Repeat("x", math.MaxInt16+1)}}
	if err := writeFetchResponse(io.Discard, 11, 0, res); err == nil {
		t.Fatal("accepted oversized topic")
	}
	// Shared payloads exercise the 2 GiB frame bound without allocating 2 GiB.
	payload := make([]byte, 1<<20)
	partitions := make([]kmsg.FetchResponseTopicPartition, 2048)
	for i := range partitions {
		partitions[i].RecordBatches = payload
	}
	res.Topics = []kmsg.FetchResponseTopic{{Topic: "events", Partitions: partitions}}
	var dst bytes.Buffer
	if err := writeFetchResponse(&dst, 11, 0, res); err == nil || dst.Len() != 0 {
		t.Fatal("oversized frame was not rejected before writing")
	}
}
