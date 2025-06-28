package golite

import (
	"encoding/binary"
	"fmt"
	"math"
)

// NullType is a sentinel type used to represent a SQL NULL value.
type NullType struct{}

// SQLNull is the singleton instance of NullType, representing a NULL value.
var SQLNull = NullType{}

// Record represents a single row of data, as a slice of Values.
type Record []any

// ParseRecord parses a raw byte slice from a cell's payload into a Record.
func ParseRecord(data []byte) (Record, error) {
	headerSize, n := readVarint(data)
	if int(headerSize) > len(data) {
		return nil, fmt.Errorf("invalid record: header size %d is larger than payload size %d", headerSize, len(data))
	}

	header := data[n:headerSize]
	body := data[headerSize:]

	var serialTypes []int64
	bytesRead := 0
	for bytesRead < len(header) {
		st, m := readVarint(header[bytesRead:])
		serialTypes = append(serialTypes, st)
		bytesRead += m
	}

	record := make(Record, 0, len(serialTypes))
	bodyOffset := 0
	for i, st := range serialTypes {
		value, bytesConsumed, err := serialTypeToValue(st, body[bodyOffset:])
		if err != nil {
			return nil, fmt.Errorf("invalid record: column %d: %w", i, err)
		}
		if bodyOffset+bytesConsumed > len(body) {
			return nil, fmt.Errorf("invalid record: data for column %d extends beyond body", i)
		}
		record = append(record, value)
		bodyOffset += bytesConsumed
	}

	return record, nil
}

// serialTypeToValue decodes a single value from the record body based on its serial type.
// It returns the parsed Value and the number of bytes consumed from the body.
func serialTypeToValue(serialType int64, body []byte) (any, int, error) {
	switch {
	case serialType == 0: // NULL
		return SQLNull, 0, nil
	case serialType == 1: // 8-bit signed int
		if len(body) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for 8-bit integer")
		}
		return int64(int8(body[0])), 1, nil
	case serialType == 2: // 16-bit signed int
		if len(body) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for 16-bit integer")
		}
		return int64(int16(binary.BigEndian.Uint16(body[:2]))), 2, nil
	case serialType == 3: // 24-bit signed int
		if len(body) < 3 {
			return nil, 0, fmt.Errorf("insufficient data for 24-bit integer")
		}
		b := make([]byte, 4)
		if body[0]&0x80 != 0 {
			b[0] = 0xff
		}
		copy(b[1:], body[:3])
		return int64(int32(binary.BigEndian.Uint32(b))), 3, nil
	case serialType == 4: // 32-bit signed int
		if len(body) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for 32-bit integer")
		}
		return int64(int32(binary.BigEndian.Uint32(body[:4]))), 4, nil
	case serialType == 5: // 48-bit signed int
		if len(body) < 6 {
			return nil, 0, fmt.Errorf("insufficient data for 48-bit integer")
		}
		b := make([]byte, 8)
		if body[0]&0x80 != 0 {
			b[0], b[1] = 0xff, 0xff
		}
		copy(b[2:], body[:6])
		return int64(binary.BigEndian.Uint64(b)), 6, nil
	case serialType == 6: // 64-bit signed int
		if len(body) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for 64-bit integer")
		}
		return int64(binary.BigEndian.Uint64(body[:8])), 8, nil
	case serialType == 7: // 64-bit float
		if len(body) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for 64-bit float")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(body[:8])), 8, nil
	case serialType == 8: // Constant 0
		return int64(0), 0, nil
	case serialType == 9: // Constant 1
		return int64(1), 0, nil
	case serialType >= 12 && serialType%2 == 0: // BLOB
		length := int((serialType - 12) / 2)
		if len(body) < length {
			return nil, 0, fmt.Errorf("insufficient data for BLOB of length %d", length)
		}
		return body[:length], length, nil
	case serialType >= 13 && serialType%2 == 1: // TEXT
		length := int((serialType - 13) / 2)
		if len(body) < length {
			return nil, 0, fmt.Errorf("insufficient data for TEXT of length %d", length)
		}
		return string(body[:length]), length, nil
	default: // Reserved or unused
		return nil, 0, fmt.Errorf("unsupported serial type %d", serialType)
	}
}
