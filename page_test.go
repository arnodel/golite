package golite

import (
	"os"
	"testing"
)

func TestParsePage(t *testing.T) {
	t.Run("parse page 1 header and cells", func(t *testing.T) {
		dbPath := createTestDB(t, "page_test.sqlite")
		data, err := os.ReadFile(dbPath)
		if err != nil {
			t.Fatalf("failed to read test database file: %v", err)
		}

		// For this test, we only need the file header to get the page size.
		fileHeader, err := ParseHeader(data[:HeaderSize])
		if err != nil {
			t.Fatalf("failed to parse file header: %v", err)
		}

		page1Data := data[:fileHeader.PageSize]
		page, err := ParsePage(page1Data, 1)
		if err != nil {
			t.Fatalf("ParsePage() failed with error: %v", err)
		}

		if page.Type != PageTypeLeafTable {
			t.Errorf("expected page 1 to be a leaf table page (0x0d), but got 0x%02x", page.Type)
		}

		// The schema table is on page 1 and should have one entry for our 'test' table.
		if page.CellCount != 1 {
			t.Errorf("expected page 1 to have 1 cell, but got %d", page.CellCount)
		}

		if len(page.CellPointers) != 1 {
			t.Errorf("expected to parse 1 cell pointer, but got %d", len(page.CellPointers))
		}

		if len(page.Cells) != 1 {
			t.Fatalf("expected to parse 1 cell, but got %d", len(page.Cells))
		}

		cell := page.Cells[0]
		// The sqlite_schema table has one row for our 'test' table, and its rowid should be 1.
		if cell.RowID != 1 {
			t.Errorf("expected cell rowID to be 1, but got %d", cell.RowID)
		}

		if cell.PayloadSize <= 0 {
			t.Errorf("expected cell payload size to be positive, but got %d", cell.PayloadSize)
		}
	})
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
