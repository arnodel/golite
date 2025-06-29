package golite

import (
	"testing"
)

func TestParsePage(t *testing.T) {
	t.Run("parse page 1 header and record", func(t *testing.T) {
		dbPath := createTestDB(t, "page_test.sqlite")
		db, err := Open(dbPath)
		if err != nil {
			t.Fatalf("Open() failed with error: %v", err)
		}
		defer db.Close()

		page, err := db.ReadPage(1)
		if err != nil {
			t.Fatalf("ReadPage(1) failed with error: %v", err)
		}

		if page.Type != PageTypeLeafTable {
			t.Errorf("expected page 1 to be a leaf table page (0x0d), but got 0x%02x", page.Type)
		}

		// The schema table is on page 1 and should have one entry for our 'test' table.
		// It now has two entries: one for the table, one for the index.
		if page.CellCount != 2 {
			t.Errorf("expected page 1 to have 2 cells, but got %d", page.CellCount)
		}

		if len(page.CellPointers) != 2 {
			t.Errorf("expected to parse 2 cell pointers, but got %d", len(page.CellPointers))
		}

		if len(page.LeafCells) != 2 {
			t.Fatalf("expected to parse 2 leaf cells, but got %d", len(page.LeafCells))
		}

		// Find the cell for the table definition. The order is not guaranteed.
		var tableCell LeafTableCell
		var found bool
		for _, c := range page.LeafCells {
			if len(c.Record) > 0 {
				if t, ok := c.Record[0].(string); ok && t == "table" {
					tableCell = c
					found = true
					break
				}
			}
		}
		if !found {
			t.Fatal("could not find the 'table' record in the schema page")
		}

		// The sqlite_schema table has 5 columns: type, name, tbl_name, rootpage, sql
		// We can verify the contents of the record for our 'test' table.
		if len(tableCell.Record) != 5 {
			t.Fatalf("expected schema record to have 5 columns, got %d", len(tableCell.Record))
		}

		// Column 1: name (TEXT)
		if val, ok := tableCell.Record[1].(string); !ok || val != "test" {
			t.Errorf("expected schema col 1 (name) to be 'test', got %v", tableCell.Record[1])
		}

		// Column 3: rootpage (INTEGER). The new table is on page 2.
		if val, ok := tableCell.Record[3].(int64); !ok || val == 0 {
			t.Errorf("expected schema col 3 (rootpage) to be a valid page number, got %v", tableCell.Record[3])
		}

		// Column 4: sql (TEXT)
		expectedSQL := "CREATE TABLE test(id INTEGER PRIMARY KEY, name TEXT)"
		if val, ok := tableCell.Record[4].(string); !ok || val != expectedSQL {
			t.Errorf("expected schema col 4 (sql) to be %q, got %q", expectedSQL, val)
		}
	})
}

func TestParseInteriorPage(t *testing.T) {
	dbPath := createTestDB(t, "interior_page_test.sqlite")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() failed with error: %v", err)
	}
	defer db.Close()

	// Read the schema to find the root page of our 'test' table.
	schemaPage, err := db.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1) for schema failed: %v", err)
	}
	// Find the cell for the table definition. The order is not guaranteed.
	var rootPageNum int64
	var found bool
	for _, c := range schemaPage.LeafCells {
		if len(c.Record) > 0 {
			if t, ok := c.Record[0].(string); ok && t == "table" {
				rootPageNum, found = c.Record[3].(int64)
				break
			}
		}
	}
	if !found {
		t.Fatal("could not find the root page number for the 'test' table in the schema page")
	}

	// Read the root page of the 'test' table, which should be an interior page.
	rootPage, err := db.ReadPage(int(rootPageNum))
	if err != nil {
		t.Fatalf("ReadPage(%d) for root failed: %v", rootPageNum, err)
	}

	if rootPage.Type != PageTypeInteriorTable {
		t.Errorf("expected page %d to be an interior table page (0x05), but got 0x%02x", rootPageNum, rootPage.Type)
	}

	if rootPage.CellCount == 0 {
		t.Error("expected interior page to have cells, but it was empty")
	}

	if len(rootPage.InteriorCells) != int(rootPage.CellCount) {
		t.Errorf("mismatched cell count: expected %d, got %d", rootPage.CellCount, len(rootPage.InteriorCells))
	}
}

func TestReadVarint(t *testing.T) {
	testCases := []struct {
		name    string
		input   []byte
		wantVal int64
		wantLen int
	}{
		{"zero", []byte{0x00}, 0, 1},
		{"one", []byte{0x01}, 1, 1},
		{"127", []byte{0x7f}, 127, 1},
		{"128", []byte{0x81, 0x00}, 128, 2},
		{"240", []byte{0x81, 0x70}, 240, 2},
		{"2024", []byte{0x8f, 0x68}, 2024, 2},
		{"16383", []byte{0xff, 0x7f}, 16383, 2},
		{"16384", []byte{0x81, 0x80, 0x00}, 16384, 3},
		{"2097151", []byte{0xff, 0xff, 0x7f}, 2097151, 3},
		{"max 9-byte", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, -1, 9},
		{"zero in 9-bytes", []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}, 0, 9},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val, length := readVarint(tc.input)
			if val != tc.wantVal {
				t.Errorf("readVarint() got value = %v, want %v", val, tc.wantVal)
			}
			if length != tc.wantLen {
				t.Errorf("readVarint() got length = %v, want %v", length, tc.wantLen)
			}
		})
	}
}
