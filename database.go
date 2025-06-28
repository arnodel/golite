package golite

import (
	"fmt"
	"os"
)

// Database represents an open SQLite database file.
// It holds the file handle and the parsed database header.
type Database struct {
	file   *os.File
	Header *Header
}

// Open opens an SQLite database file from the given path.
func Open(path string) (*Database, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	headerBytes := make([]byte, HeaderSize)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read database header: %w", err)
	}

	header, err := ParseHeader(headerBytes)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to parse database header: %w", err)
	}

	return &Database{file: file, Header: header}, nil
}

// Close closes the underlying database file.
func (db *Database) Close() error {
	return db.file.Close()
}

// ReadPage reads a single page from the database file.
func (db *Database) ReadPage(pageNum int) (*Page, error) {
	pageData := make([]byte, db.Header.PageSize)
	offset := int64(pageNum-1) * int64(db.Header.PageSize)
	_, err := db.file.ReadAt(pageData, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageNum, err)
	}
	return ParsePage(pageData, pageNum)
}
