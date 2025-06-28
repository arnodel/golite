package golite

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// HeaderString is the required 16-byte string at the beginning of a valid SQLite file.
	HeaderString = "SQLite format 3\x00"
	// HeaderSize is the size of the SQLite database header in bytes.
	HeaderSize = 100
)

// Header represents the parsed 100-byte header of an SQLite database file.
// It contains key metadata about the database structure.
type Header struct {
	// PageSize is the database page size in bytes. Must be a power of two
	// between 512 and 65536 inclusive.
	PageSize uint16
	// ChangeCounter is the file change counter.
	ChangeCounter uint32
	// DatabaseSize is the size of the database file in pages.
	DatabaseSize uint32
	// FreelistTrunk is the page number of the first freelist trunk page.
	FreelistTrunk uint32
	// FreelistPages is the total number of freelist pages.
	FreelistPages uint32
	// SchemaCookie is used to detect schema changes.
	SchemaCookie uint32
	// SchemaFormat is the schema format number. 1, 2, 3, and 4 are supported.
	SchemaFormat uint32
	// DefaultCacheSize is the suggested default page cache size in bytes.
	DefaultCacheSize uint32
	// TextEncoding defines the text encoding used by the database.
	// 1: UTF-8, 2: UTF-16le, 3: UTF-16be.
	TextEncoding uint32
	// UserVersion is the "user version" number, read and set by the user_version pragma.
	UserVersion uint32
}

// ParseHeader reads the 100-byte header data and returns a parsed Header struct.
// It returns an error if the data is not a valid SQLite header.
func ParseHeader(data []byte) (*Header, error) {
	if len(data) != HeaderSize {
		return nil, fmt.Errorf("invalid header size: expected %d bytes, got %d", HeaderSize, len(data))
	}

	if string(data[0:16]) != HeaderString {
		return nil, errors.New("invalid SQLite header string")
	}

	h := &Header{
		PageSize:         binary.BigEndian.Uint16(data[16:18]),
		ChangeCounter:    binary.BigEndian.Uint32(data[24:28]),
		DatabaseSize:     binary.BigEndian.Uint32(data[28:32]),
		FreelistTrunk:    binary.BigEndian.Uint32(data[32:36]),
		FreelistPages:    binary.BigEndian.Uint32(data[36:40]),
		SchemaCookie:     binary.BigEndian.Uint32(data[40:44]),
		SchemaFormat:     binary.BigEndian.Uint32(data[44:48]),
		DefaultCacheSize: binary.BigEndian.Uint32(data[48:52]),
		TextEncoding:     binary.BigEndian.Uint32(data[56:60]),
		UserVersion:      binary.BigEndian.Uint32(data[60:64]),
	}

	return h, nil
}
