package golite

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"sort"
	"strings"
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
func (db *Database) Find(table TableInfo, rowID int64) (Row, error) {
	pageNum := table.RootPage
	for {
		page, err := db.ReadPage(pageNum)
		if err != nil {
			return Row{}, err
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
				if table.RowIDColumnIndex != -1 && len(record) > table.RowIDColumnIndex {
					record[table.RowIDColumnIndex] = cell.RowID
				}
				return Row{
					RowID:  cell.RowID,
					Record: record,
				}, nil
			}
			return Row{}, ErrNotFound // Not found on this leaf page.

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
			return Row{}, fmt.Errorf("unexpected page type %02x encountered during search", page.Type)
		}
	}
}

// FindInIndex searches for a key within an index's B-Tree and returns the corresponding rowID.
// The key is a Record containing the values of the indexed columns.
func (db *Database) FindInIndex(index IndexInfo, key Record) (int64, error) {
	pageNum := index.RootPage
	for {
		page, err := db.ReadPage(pageNum)
		if err != nil {
			return 0, err
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

			if i < len(page.LeafIndexCells) {
				cell := page.LeafIndexCells[i]
				// Check if it's an exact match.
				if len(cell.Payload) == len(key)+1 && CompareRecords(cell.Payload[:len(key)], key) == 0 {
					rowid, ok := cell.Payload[len(cell.Payload)-1].(int64)
					if !ok {
						return 0, fmt.Errorf("malformed index record: rowid is not an integer")
					}
					return rowid, nil
				}
			}
			return 0, ErrNotFound // Not found on this leaf page.

		case PageTypeInteriorIndex:
			// It's an interior page. The cells are sorted by payload, so we can binary search
			// to find the correct child page to descend into.
			i := sort.Search(len(page.InteriorIndexCells), func(i int) bool {
				// Find the first cell whose key is >= our search key.
				return CompareRecords(key, page.InteriorIndexCells[i].Payload) <= 0
			})

			if i < len(page.InteriorIndexCells) {
				pageNum = int(page.InteriorIndexCells[i].LeftChildPageNum)
			} else {
				// If key is greater than all keys in the cells, go to the right-most child.
				pageNum = int(page.RightMostPtr)
			}
		default:
			return 0, fmt.Errorf("unexpected page type %02x encountered during index search", page.Type)
		}
	}
}

// Scan returns an iterator over all records in a table.
// The iterator can be used with a for...range loop.
// Note: This API requires Go 1.22+ with GOEXPERIMENT=rangefunc, or Go 1.23+.
func (db *Database) Scan(table TableInfo) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		db.scanPage(table.RootPage, table, yield)
	}
}

// scanPage is the recursive helper for Scan. It traverses the B-Tree in-order.
// It returns true to continue scanning, or false to stop.
func (db *Database) scanPage(pageNum int, table TableInfo, yield func(Row, error) bool) bool {
	page, err := db.ReadPage(pageNum)
	if err != nil {
		return yield(Row{}, err)
	}

	switch page.Type {
	case PageTypeLeafTable:
		for _, cell := range page.LeafCells {
			record := cell.Record
			if table.RowIDColumnIndex != -1 && len(record) > table.RowIDColumnIndex {
				record[table.RowIDColumnIndex] = cell.RowID
			}
			row := Row{
				RowID:  cell.RowID,
				Record: record,
			}
			if !yield(row, nil) {
				return false // Stop scan
			}
		}
		return true // Continue scan

	case PageTypeInteriorTable:
		for _, cell := range page.InteriorCells {
			if !db.scanPage(int(cell.LeftChildPageNum), table, yield) {
				return false // Stop scan
			}
		}
		return db.scanPage(int(page.RightMostPtr), table, yield)
	default:
		return yield(Row{}, fmt.Errorf("unexpected page type %02x encountered during scan", page.Type))
	}
}

// GetSchema reads and parses the entire database schema from the sqlite_schema table.
func (db *Database) GetSchema() (*Schema, error) {
	schema := &Schema{
		Tables:  make(map[string]TableInfo),
		Indexes: make(map[string]IndexInfo),
	}

	// The schema table is always rooted at page 1.
	// We create a "bootstrap" TableInfo for the schema table itself to use the Scan method.
	// The schema table has no INTEGER PRIMARY KEY, so its RowIDColumnIndex is -1.
	schemaTableInfo := TableInfo{
		Name:             "sqlite_schema",
		RootPage:         1,
		SQL:              "CREATE TABLE sqlite_schema(type text, name text, tbl_name text, rootpage integer, sql text)",
		RowIDColumnIndex: -1,
	}
	schema.Tables[schemaTableInfo.Name] = schemaTableInfo

	for row, err := range db.Scan(schemaTableInfo) {
		if err != nil {
			return nil, fmt.Errorf("failed to scan schema table: %w", err)
		}

		record := row.Record
		// Schema table format: type, name, tbl_name, rootpage, sql
		if len(record) < 5 {
			return nil, fmt.Errorf("malformed schema record: expected at least 5 columns, got %d", len(record))
		}

		itemType, ok := record[0].(string)
		if !ok {
			return nil, fmt.Errorf("malformed schema record: column 0 (type) is not a string")
		}
		switch itemType {
		case "table":
			name, okName := record[1].(string)
			rootPage, okRootPage := record[3].(int64)
			sql, okSQL := record[4].(string)
			if !okName || !okRootPage || !okSQL {
				return nil, fmt.Errorf("malformed schema record for table %q: one or more columns have an unexpected type", name)
			}

			rowIndex := findRowIDColumnIndex(sql)
			schema.Tables[name] = TableInfo{
				Name:             name,
				RootPage:         int(rootPage),
				SQL:              sql,
				RowIDColumnIndex: rowIndex,
			}
		case "index":
			name, okName := record[1].(string)
			tableName, okTableName := record[2].(string)
			rootPage, okRootPage := record[3].(int64)
			sql, okSQL := record[4].(string)
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

// findRowIDColumnIndex performs a simple parse of a CREATE TABLE statement
// to find the index of the INTEGER PRIMARY KEY column.
// It returns -1 if no such column is found.
// NOTE: This is a simplified parser and may not handle all valid SQL syntax,
// especially complex constraints with nested parentheses.
func findRowIDColumnIndex(sql string) int {
	start := strings.Index(sql, "(")
	if start == -1 {
		return -1
	}
	// We assume the column definitions end at the last parenthesis.
	// This is fragile but works for simple CREATE TABLE statements.
	end := strings.LastIndex(sql, ")")
	if end <= start {
		return -1
	}

	defs := strings.Split(sql[start+1:end], ",")
	for i, def := range defs {
		if strings.Contains(strings.ToUpper(strings.TrimSpace(def)), "INTEGER PRIMARY KEY") {
			return i
		}
	}
	return -1
}
