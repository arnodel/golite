package golite

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"
)

func TestParseRecord_Errors(t *testing.T) {
	testCases := []struct {
		name  string
		input []byte
		err   string
	}{
		{
			name:  "header size larger than payload",
			input: []byte{0x05, 0x01, 0x01, 0x01}, // Header says 5 bytes, but total is 4
			err:   "invalid record: header size 5 is larger than payload size 4",
		},
		{
			name:  "body data extends beyond body",
			input: []byte{0x03, 0x17, 0x68, 0x65, 0x6c, 0x6c}, // Header for 5-byte string, but only 4 bytes in body
			err:   "invalid record: column 0: insufficient data for TEXT of length 5",
		},
		{
			name:  "unsupported serial type",
			input: []byte{0x02, 0x0b}, // Serial type 11 is reserved
			err:   "invalid record: column 0: unsupported serial type 11",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseRecord(tc.input)
			if err == nil {
				t.Fatalf("ParseRecord() expected an error, but got nil")
			}
			if err.Error() != tc.err {
				t.Errorf("ParseRecord() error = %q, want %q", err.Error(), tc.err)
			}
		})
	}
}

func TestSerialTypeToValue(t *testing.T) {
	floatVal := 3.14159
	floatBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(floatBytes, math.Float64bits(floatVal))

	testCases := []struct {
		name       string
		serialType int64
		body       []byte
		wantValue  any
		wantBytes  int
		wantErr    bool
	}{
		// Happy paths
		{name: "NULL", serialType: 0, body: []byte{}, wantValue: SQLNull, wantBytes: 0},
		{name: "INT 8-bit", serialType: 1, body: []byte{0xfa}, wantValue: int64(-6), wantBytes: 1},
		{name: "INT 16-bit", serialType: 2, body: []byte{0xff, 0xfa}, wantValue: int64(-6), wantBytes: 2},
		{name: "INT 24-bit", serialType: 3, body: []byte{0xff, 0xff, 0xfa}, wantValue: int64(-6), wantBytes: 3},
		{name: "INT 32-bit", serialType: 4, body: []byte{0xff, 0xff, 0xff, 0xfa}, wantValue: int64(-6), wantBytes: 4},
		{name: "INT 48-bit", serialType: 5, body: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xfa}, wantValue: int64(-6), wantBytes: 6},
		{name: "INT 64-bit", serialType: 6, body: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfa}, wantValue: int64(-6), wantBytes: 8},
		{name: "FLOAT 64-bit", serialType: 7, body: floatBytes, wantValue: floatVal, wantBytes: 8},
		{name: "Constant 0", serialType: 8, body: []byte{}, wantValue: int64(0), wantBytes: 0},
		{name: "Constant 1", serialType: 9, body: []byte{}, wantValue: int64(1), wantBytes: 0},
		{name: "BLOB 5 bytes", serialType: 22, body: []byte("hello"), wantValue: []byte("hello"), wantBytes: 5},
		{name: "TEXT 5 bytes", serialType: 23, body: []byte("hello"), wantValue: "hello", wantBytes: 5},
		{name: "BLOB 0 bytes", serialType: 12, body: []byte{}, wantValue: []byte{}, wantBytes: 0},
		{name: "TEXT 0 bytes", serialType: 13, body: []byte{}, wantValue: "", wantBytes: 0},

		// Error paths - insufficient data
		{name: "INT 8-bit short", serialType: 1, body: []byte{}, wantErr: true},
		{name: "INT 16-bit short", serialType: 2, body: []byte{0x01}, wantErr: true},
		{name: "INT 24-bit short", serialType: 3, body: []byte{0x01, 0x02}, wantErr: true},
		{name: "INT 32-bit short", serialType: 4, body: []byte{0x01, 0x02, 0x03}, wantErr: true},
		{name: "INT 48-bit short", serialType: 5, body: []byte{0x01, 0x02, 0x03, 0x04, 0x05}, wantErr: true},
		{name: "INT 64-bit short", serialType: 6, body: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, wantErr: true},
		{name: "FLOAT 64-bit short", serialType: 7, body: []byte{0x01, 0x02, 0x03}, wantErr: true},
		{name: "BLOB short", serialType: 22, body: []byte("hell"), wantErr: true},
		{name: "TEXT short", serialType: 23, body: []byte("hell"), wantErr: true},

		// Error paths - unsupported types
		{name: "Reserved type 10", serialType: 10, body: []byte{}, wantErr: true},
		{name: "Reserved type 11", serialType: 11, body: []byte{}, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotValue, gotBytes, err := serialTypeToValue(tc.serialType, tc.body)

			if (err != nil) != tc.wantErr {
				t.Fatalf("serialTypeToValue() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			if !reflect.DeepEqual(gotValue, tc.wantValue) {
				t.Errorf("serialTypeToValue() gotValue = %v, want %v", gotValue, tc.wantValue)
			}

			if gotBytes != tc.wantBytes {
				t.Errorf("serialTypeToValue() gotBytes = %v, want %v", gotBytes, tc.wantBytes)
			}
		})
	}
}

func TestCompareRecords(t *testing.T) {
	testCases := []struct {
		name string
		a    Record
		b    Record
		want int // -1 for a < b, 0 for a == b, 1 for a > b
	}{
		// Basic equality
		{name: "equal ints", a: Record{int64(1)}, b: Record{int64(1)}, want: 0},
		{name: "equal floats", a: Record{3.14}, b: Record{3.14}, want: 0},
		{name: "equal strings", a: Record{"hello"}, b: Record{"hello"}, want: 0},
		{name: "equal blobs", a: Record{[]byte{1, 2}}, b: Record{[]byte{1, 2}}, want: 0},
		{name: "equal nulls", a: Record{SQLNull}, b: Record{SQLNull}, want: 0},
		{name: "equal mixed records", a: Record{int64(1), "a", SQLNull}, b: Record{int64(1), "a", SQLNull}, want: 0},
		{name: "equal int and float", a: Record{int64(5)}, b: Record{5.0}, want: 0},

		// Basic inequality
		{name: "lesser int", a: Record{int64(1)}, b: Record{int64(2)}, want: -1},
		{name: "greater int", a: Record{int64(3)}, b: Record{int64(2)}, want: 1},
		{name: "lesser float", a: Record{1.0}, b: Record{2.0}, want: -1},
		{name: "greater float", a: Record{3.0}, b: Record{2.0}, want: 1},
		{name: "lesser string", a: Record{"a"}, b: Record{"b"}, want: -1},
		{name: "greater string", a: Record{"c"}, b: Record{"b"}, want: 1},
		{name: "lesser blob", a: Record{[]byte{1}}, b: Record{[]byte{2}}, want: -1},
		{name: "greater blob", a: Record{[]byte{3}}, b: Record{[]byte{2}}, want: 1},
		{name: "lesser int vs float", a: Record{int64(4)}, b: Record{4.1}, want: -1},
		{name: "greater int vs float", a: Record{int64(5)}, b: Record{4.9}, want: 1},

		// Type precedence
		{name: "null vs int", a: Record{SQLNull}, b: Record{int64(1)}, want: -1},
		{name: "int vs null", a: Record{int64(1)}, b: Record{SQLNull}, want: 1},
		{name: "int vs string", a: Record{int64(123)}, b: Record{"abc"}, want: -1},
		{name: "string vs int", a: Record{"abc"}, b: Record{int64(123)}, want: 1},
		{name: "string vs blob", a: Record{"abc"}, b: Record{[]byte("abc")}, want: -1},
		{name: "blob vs string", a: Record{[]byte("abc")}, b: Record{"abc"}, want: 1},

		// Multi-column records
		{name: "multi-col first diff", a: Record{int64(1), "b"}, b: Record{int64(1), "c"}, want: -1},
		{name: "multi-col second diff", a: Record{int64(2), "a"}, b: Record{int64(1), "z"}, want: 1},

		// Record length differences
		{name: "prefix is lesser", a: Record{int64(1)}, b: Record{int64(1), int64(2)}, want: -1},
		{name: "prefix is greater", a: Record{int64(1), int64(2)}, b: Record{int64(1)}, want: 1},
		{name: "empty vs non-empty", a: Record{}, b: Record{int64(1)}, want: -1},
		{name: "equal empty", a: Record{}, b: Record{}, want: 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := CompareRecords(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("CompareRecords(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}

			// Test the reverse comparison as well
			reverseWant := -tc.want
			reverseGot := CompareRecords(tc.b, tc.a)
			if reverseGot != reverseWant {
				t.Errorf("Reverse CompareRecords(%v, %v) = %d, want %d", tc.b, tc.a, reverseGot, reverseWant)
			}
		})
	}
}

func TestReadVarint_Errors(t *testing.T) {
	// The current implementation of readVarint can panic if it reads past the
	// end of the slice. This test is here to catch that if the implementation
	// changes in a way that introduces this bug.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("readVarint panicked on short input, which it should not")
		}
	}()

	// This varint indicates it has more bytes, but the slice is truncated.
	// A robust implementation should not panic.
	shortInput := []byte{0x81}
	readVarint(shortInput)
}
