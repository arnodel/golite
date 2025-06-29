package golite

import (
	"errors"
	"fmt"
	"os"
	"sort"
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

// TableSeek searches for a record with a specific rowID within a table's B-Tree.
// It returns a RecordIterator that will yield at most one record. If the record
// is not found, the iterator will be empty.
func (db *Database) TableSeek(table TableInfo, rowID int64) RecordIterator {
	return func(yield func(Record, error) bool) {
		pageNum := table.RootPage
		for {
			page, err := db.ReadPage(pageNum)
			if err != nil {
				yield(nil, err)
				return
			}

			switch page.Type {
			case PageTypeLeafTable:
				// We've reached a leaf page. The cells are sorted by rowID, so we can binary search.
				i := sort.Search(len(page.LeafCells), func(i int) bool {
					return page.LeafCells[i].RowID >= rowID
				})

				if i < len(page.LeafCells) && page.LeafCells[i].RowID == rowID {
					// Found it.
					cell := page.LeafCells[i]
					record := cell.Record
					if table.RowIDColumnIndex != -1 {
						record[table.RowIDColumnIndex] = cell.RowID
						yield(record, nil)
					} else {
						yield(append(Record{cell.RowID}, record...), nil)
					}
				}
				return // We are done, whether we found it or not.

			case PageTypeInteriorTable:
				// It's an interior page. The cells are sorted by key, so we can binary search
				// to find the correct child page to descend into.
				i := sort.Search(len(page.InteriorCells), func(i int) bool {
					return rowID <= page.InteriorCells[i].Key
				})

				if i < len(page.InteriorCells) {
					pageNum = int(page.InteriorCells[i].LeftChildPageNum)
				} else {
					// The rowID is greater than all keys in the cells, so go to the right-most child.
					pageNum = int(page.RightMostPtr)
				}
			default:
				yield(nil, fmt.Errorf("unexpected page type %02x encountered during search", page.Type))
				return
			}
		}
	}
}

// IndexSeek searches for a key within an index's B-Tree. It returns a RecordIterator
// that yields all matching index records. For a unique index, this will be at most one record.
// The key is a Record containing the values of the indexed columns.
func (db *Database) IndexSeek(index IndexInfo, key Record) RecordIterator {
	return func(yield func(Record, error) bool) {
		pageNum := index.RootPage
		for {
			page, err := db.ReadPage(pageNum)
			if err != nil {
				yield(nil, err)
				return
			}

			switch page.Type {
			case PageTypeLeafIndex:
				// We've reached a leaf page. The cells are sorted by payload, so we can binary search.
				i := sort.Search(len(page.LeafIndexCells), func(i int) bool {
					// We are looking for the first record that is >= our key.
					// The cell payload is (key_values..., rowid). We only compare the key part.
					// This is safe because a valid index payload will always be longer than the key.
					return CompareRecords(page.LeafIndexCells[i].Payload[:len(key)], key) >= 0
				})

				// Now, iterate from the found position as long as the keys match.
				for ; i < len(page.LeafIndexCells); i++ {
					cell := page.LeafIndexCells[i]
					if len(cell.Payload) < len(key) {
						continue
					}
					if CompareRecords(cell.Payload[:len(key)], key) == 0 {
						if !yield(cell.Payload, nil) {
							return // Consumer requested stop
						}
					} else {
						// Since the cells are sorted, we can stop as soon as we find a non-match.
						break
					}
				}
				return // We are done searching this branch.

			case PageTypeInteriorIndex:
				// It's an interior page. Find the correct child page to descend into.
				i := sort.Search(len(page.InteriorIndexCells), func(i int) bool {
					return CompareRecords(key, page.InteriorIndexCells[i].Payload) <= 0
				})

				if i < len(page.InteriorIndexCells) {
					pageNum = int(page.InteriorIndexCells[i].LeftChildPageNum)
				} else {
					pageNum = int(page.RightMostPtr)
				}
			default:
				yield(nil, fmt.Errorf("unexpected page type %02x encountered during index search", page.Type))
				return
			}
		}
	}
}

// TableScan returns an iterator over all records in a table.
// The iterator can be used with a for...range loop.
// Note: This API requires Go 1.22+ with GOEXPERIMENT=rangefunc, or Go 1.23+.
func (db *Database) TableScan(table TableInfo) RecordIterator {
	return func(yield func(Record, error) bool) {
		db.tableScanPage(table.RootPage, table, yield)
	}
}

// tableScanPage is the recursive helper for TableScan. It traverses the B-Tree in-order.
// It returns true to continue scanning, or false to stop.
func (db *Database) tableScanPage(pageNum int, table TableInfo, yield func(Record, error) bool) bool {
	page, err := db.ReadPage(pageNum)
	if err != nil {
		return yield(nil, err)
	}

	switch page.Type {
	case PageTypeLeafTable:
		for _, cell := range page.LeafCells {
			record := cell.Record
			var finalRecord Record
			if table.RowIDColumnIndex != -1 {
				record[table.RowIDColumnIndex] = cell.RowID
				finalRecord = record
			} else {
				finalRecord = append(Record{cell.RowID}, record...)
			}
			if !yield(finalRecord, nil) {
				return false // Stop scan
			}
		}
		return true // Continue scan

	case PageTypeInteriorTable:
		for _, cell := range page.InteriorCells {
			if !db.tableScanPage(int(cell.LeftChildPageNum), table, yield) {
				return false // Stop scan
			}
		}
		return db.tableScanPage(int(page.RightMostPtr), table, yield)
	default:
		return yield(nil, fmt.Errorf("unexpected page type %02x encountered during scan", page.Type))
	}
}

// GetSchema reads and parses the entire database schema from the sqlite_schema table.
func (db *Database) GetSchema() (*Schema, error) {
	schema := &Schema{
		Tables:  make(map[string]TableInfo),
		Indexes: make(map[string]IndexInfo),
	}

	// The schema table is always rooted at page 1.
	// We create a "bootstrap" TableInfo for the schema table itself to use the TableScan method.
	// The schema table has no INTEGER PRIMARY KEY, so its RowIDColumnIndex is -1.
	schemaTableInfo := TableInfo{
		Name:             "sqlite_schema",
		RootPage:         1,
		SQL:              "CREATE TABLE sqlite_schema(type text, name text, tbl_name text, rootpage integer, sql text)",
		RowIDColumnIndex: -1,
	}
	schema.Tables[schemaTableInfo.Name] = schemaTableInfo

	for record, err := range db.TableScan(schemaTableInfo) {
		if err != nil {
			return nil, fmt.Errorf("failed to scan schema table: %w", err)
		}

		// Schema table format: type, name, tbl_name, rootpage, sql
		// After prepending the implicit rowid, we expect 6 columns.
		if len(record) < 6 {
			return nil, fmt.Errorf("malformed schema record: expected at least 6 columns, got %d", len(record))
		}

		itemType, ok := record[1].(string)
		if !ok {
			return nil, fmt.Errorf("malformed schema record: column 1 (type) is not a string")
		}
		switch itemType {
		case "table":
			name, okName := record[2].(string)
			rootPage, okRootPage := record[4].(int64)
			sql, okSQL := record[5].(string)
			if !okName || !okRootPage || !okSQL {
				return nil, fmt.Errorf("malformed schema record for table %q: one or more columns have an unexpected type", name)
			}

			columns, rowIndex, err := ParseTableSQL(sql)
			if err != nil {
				return nil, fmt.Errorf("failed to parse schema for table %q: %w", name, err)
			}
			schema.Tables[name] = TableInfo{
				Name:             name,
				RootPage:         int(rootPage),
				SQL:              sql,
				Columns:          columns,
				RowIDColumnIndex: rowIndex,
			}
		case "index":
			name, okName := record[2].(string)
			tableName, okTableName := record[3].(string)
			rootPage, okRootPage := record[4].(int64)
			sql, okSQL := record[5].(string)
			if !okName || !okTableName || !okRootPage || !okSQL {
				return nil, fmt.Errorf("malformed schema record for index %q: one or more columns have an unexpected type", name)
			}
			schema.Indexes[name] = IndexInfo{
				Name:      name,
				TableName: tableName,
				RootPage:  int(rootPage),
				SQL:       sql,
			}
		}
	}
	return schema, nil
}
