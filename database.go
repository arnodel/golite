package golite

import (
	"errors"
	"fmt"
	"iter"
	"os"
)

// Database represents an open SQLite database file.
// It holds the file handle and the parsed database header.
type Database struct {
	file   *os.File
	Header *Header
}

// ErrNotFound is returned by Find when a record with the specified rowID cannot be found.
var ErrNotFound = errors.New("record not found")

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

// Find searches for a record with a specific rowID within a table's B-Tree.
// It starts the search from the given rootPageNum.
func (db *Database) Find(rootPageNum int, rowID int64) (Record, error) {
	pageNum := rootPageNum
	for {
		page, err := db.ReadPage(pageNum)
		if err != nil {
			return nil, err
		}

		switch page.Type {
		case PageTypeLeafTable:
			// We've reached a leaf page, search for the rowID here.
			for _, cell := range page.LeafCells {
				if cell.RowID == rowID {
					return cell.Record, nil
				}
			}
			// If we've searched the whole leaf page and not found it.
			return nil, ErrNotFound

		case PageTypeInteriorTable:
			// It's an interior page, find the next page to visit.
			foundChild := false
			for _, cell := range page.InteriorCells {
				if rowID <= cell.Key {
					pageNum = int(cell.LeftChildPageNum)
					foundChild = true
					break
				}
			}
			if !foundChild {
				pageNum = int(page.RightMostPtr)
			}
		default:
			return nil, fmt.Errorf("unexpected page type %02x encountered during search", page.Type)
		}
	}
}

// Scan returns an iterator over all records in a table.
// The scan starts from the given rootPageNum. The iterator can be used with a for...range loop.
// Note: This API requires Go 1.22+ with GOEXPERIMENT=rangefunc, or Go 1.23+.
func (db *Database) Scan(rootPageNum int) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		db.scanPage(rootPageNum, yield)
	}
}

// scanPage is the recursive helper for Scan. It traverses the B-Tree in-order.
// It returns true to continue scanning, or false to stop.
func (db *Database) scanPage(pageNum int, yield func(Record, error) bool) bool {
	page, err := db.ReadPage(pageNum)
	if err != nil {
		return yield(nil, err)
	}

	switch page.Type {
	case PageTypeLeafTable:
		for _, cell := range page.LeafCells {
			if !yield(cell.Record, nil) {
				return false // Stop scan
			}
		}
		return true // Continue scan

	case PageTypeInteriorTable:
		for _, cell := range page.InteriorCells {
			if !db.scanPage(int(cell.LeftChildPageNum), yield) {
				return false // Stop scan
			}
		}
		return db.scanPage(int(page.RightMostPtr), yield)
	default:
		return yield(nil, fmt.Errorf("unexpected page type %02x encountered during scan", page.Type))
	}
}
