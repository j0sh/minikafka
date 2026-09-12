package minikafka

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Encode one uncompressed v2 batch with the store's offsets and timestamps.
func encodeFetchRecords(records []Record) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if len(records) > math.MaxInt32 {
		return nil, fmt.Errorf("too many fetch records")
	}
	first := records[0]
	batch := kmsg.RecordBatch{
		FirstOffset: first.Offset, PartitionLeaderEpoch: -1, Magic: 2,
		FirstTimestamp: first.Timestamp.UnixMilli(), MaxTimestamp: first.Timestamp.UnixMilli(),
		ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1,
		NumRecords: int32(len(records)), LastOffsetDelta: int32(len(records) - 1),
	}
	var scratch []byte
	for i, r := range records {
		if r.Offset < 0 || (i > 0 && r.Offset-1 != records[i-1].Offset) {
			return nil, fmt.Errorf("invalid store offset %d at record %d", r.Offset, i)
		}
		ts := r.Timestamp.UnixMilli()
		delta := ts - batch.FirstTimestamp
		if (ts > batch.FirstTimestamp && delta < 0) || (ts < batch.FirstTimestamp && delta > 0) {
			return nil, fmt.Errorf("timestamp delta overflows at offset %d", r.Offset)
		}
		rec := kmsg.Record{
			OffsetDelta: int32(i), TimestampDelta64: delta, Key: r.Key, Value: r.Value,
		}
		for _, h := range r.Headers {
			rec.Headers = append(rec.Headers, kmsg.Header{Key: h.Key, Value: h.Value})
		}
		// kmsg writes Length verbatim. Serialize with its one-byte zero length,
		// then prepend the actual body length without duplicating field sizing.
		scratch = rec.AppendTo(scratch[:0])
		body := scratch[1:]
		if len(body) > math.MaxInt32 {
			return nil, fmt.Errorf("record at offset %d exceeds Kafka size limit", r.Offset)
		}
		batch.Records = binary.AppendVarint(batch.Records, int64(len(body)))
		batch.Records = append(batch.Records, body...)
		// The batch header contributes 61 bytes to the Fetch record set.
		if len(batch.Records) > math.MaxInt32-61 {
			return nil, fmt.Errorf("fetch record set exceeds Kafka size limit")
		}
		batch.MaxTimestamp = max(batch.MaxTimestamp, ts)
	}
	batch.Length = int32(49 + len(batch.Records)) // Excludes FirstOffset and Length.
	raw := batch.AppendTo(nil)
	// CRC covers Attributes through Records, following the CRC field itself.
	binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	return raw, nil
}

func writeFetchResponse(w io.Writer, version int16, correlationID int32, res *kmsg.FetchResponse) error {
	if version < 0 || version > 11 {
		return fmt.Errorf("unsupported Fetch version %d", version)
	}
	// Validate every variable-length field and the frame before kmsg narrows
	// lengths to int32. Versions 0–11 all use the non-flexible response header.
	size := int64(4 + 4) // correlation ID and topic array length
	if version >= 1 {
		size += 4
	}
	if version >= 7 {
		size += 6
	}
	if len(res.Topics) > math.MaxInt32 {
		return fmt.Errorf("too many Fetch topics")
	}
	for _, topic := range res.Topics {
		if len(topic.Topic) > math.MaxInt16 || len(topic.Partitions) > math.MaxInt32 {
			return fmt.Errorf("Fetch topic exceeds Kafka size limit")
		}
		size += 2 + int64(len(topic.Topic)) + 4
		for _, p := range topic.Partitions {
			if len(p.RecordBatches) > math.MaxInt32 || len(p.AbortedTransactions) > math.MaxInt32 {
				return fmt.Errorf("Fetch partition exceeds Kafka size limit")
			}
			size += 4 + 2 + 8 + 4 + int64(len(p.RecordBatches))
			if version >= 4 {
				size += 8 + 4 + 16*int64(len(p.AbortedTransactions))
			}
			if version >= 5 {
				size += 8
			}
			if version >= 11 {
				size += 4
			}
			if size > math.MaxInt32 {
				return fmt.Errorf("Fetch response exceeds Kafka frame size")
			}
		}
	}
	if size > math.MaxInt32 {
		return fmt.Errorf("Fetch response exceeds Kafka frame size")
	}
	res.SetVersion(version)
	frame := binary.BigEndian.AppendUint32(nil, uint32(size))
	frame = binary.BigEndian.AppendUint32(frame, uint32(correlationID))
	frame = res.AppendTo(frame)
	for len(frame) > 0 {
		n, err := w.Write(frame)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(frame) {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}
