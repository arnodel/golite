package golite

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
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

// CompareRecords compares two records according to SQLite's sorting rules.
// It returns -1 if a < b, 0 if a == b, and 1 if a > b.
// This is essential for searching index B-Trees.
func CompareRecords(a, b Record) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		cmp := compareValues(a[i], b[i])
		if cmp != 0 {
			return cmp
		}
	}

	// If we've exhausted one record, the shorter one is smaller.
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}

	return 0 // Records are identical.
}

// getTypeRank returns an integer representing the type's precedence for comparison.
// Lower ranks are considered "less than" higher ranks.
func getTypeRank(v any) int {
	switch v.(type) {
	case NullType:
		return 0
	case int64, float64:
		return 1 // Numeric types
	case string:
		return 2
	case []byte:
		return 3
	default:
		// This should not be reached with valid records.
		return 4
	}
}

// toFloat64 converts a numeric value (int64 or float64) to a float64 for comparison.
func toFloat64(v any) float64 {
	if i, ok := v.(int64); ok {
		return float64(i)
	}
	return v.(float64)
}

// compareValues compares two individual values based on SQLite's type ordering rules.
func compareValues(a, b any) int {
	rankA := getTypeRank(a)
	rankB := getTypeRank(b)

	if rankA != rankB {
		if rankA < rankB {
			return -1
		}
		return 1
	}

	// Ranks are the same, so we can compare the values directly.
	switch rankA {
	case 0: // NULL
		return 0 // All NULLs are equal.
	case 1: // Numeric
		fA := toFloat64(a)
		fB := toFloat64(b)
		if fA < fB {
			return -1
		} else if fA > fB {
			return 1
		}
		return 0
	case 2: // Text
		return strings.Compare(a.(string), b.(string))
	case 3: // Blob
		return bytes.Compare(a.([]byte), b.([]byte))
	}

	return 0 // Should not be reached.
}

// serialTypeToValue decodes a single value from the record body based on its serial type.
// It returns the parsed Value and the number of bytes consumed from the body.
func serialTypeToValue(serialType int64, body []byte) (any, int, error) {
	switch {
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
	}

	switch serialType {
	case 0: // NULL
		return SQLNull, 0, nil
	case 1: // 8-bit signed int
		if len(body) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for 8-bit integer")
		}
		return int64(int8(body[0])), 1, nil
	case 2: // 16-bit signed int
		if len(body) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for 16-bit integer")
		}
		return int64(int16(binary.BigEndian.Uint16(body[:2]))), 2, nil
	case 3: // 24-bit signed int
		if len(body) < 3 {
			return nil, 0, fmt.Errorf("insufficient data for 24-bit integer")
		}
		b := make([]byte, 4)
		if body[0]&0x80 != 0 {
			b[0] = 0xff
		}
		copy(b[1:], body[:3])
		return int64(int32(binary.BigEndian.Uint32(b))), 3, nil
	case 4: // 32-bit signed int
		if len(body) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for 32-bit integer")
		}
		return int64(int32(binary.BigEndian.Uint32(body[:4]))), 4, nil
	case 5: // 48-bit signed int
		if len(body) < 6 {
			return nil, 0, fmt.Errorf("insufficient data for 48-bit integer")
		}
		b := make([]byte, 8)
		if body[0]&0x80 != 0 {
			b[0], b[1] = 0xff, 0xff
		}
		copy(b[2:], body[:6])
		return int64(binary.BigEndian.Uint64(b)), 6, nil
	case 6: // 64-bit signed int
		if len(body) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for 64-bit integer")
		}
		return int64(binary.BigEndian.Uint64(body[:8])), 8, nil
	case 7: // 64-bit float
		if len(body) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for 64-bit float")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(body[:8])), 8, nil
	case 8: // Constant 0
		return int64(0), 0, nil
	case 9: // Constant 1
		return int64(1), 0, nil
	default: // Reserved or unused
		return nil, 0, fmt.Errorf("unsupported serial type %d", serialType)
	}
}
