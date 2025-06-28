package golite

import (
	"errors"
	"testing"
)

func TestDatabase_Find(t *testing.T) {
	dbPath := createTestDB(t, "find_test.sqlite")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() failed with error: %v", err)
	}
	defer db.Close()

	// Get the root page of the 'test' table from the schema.
	schemaPage, err := db.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1) for schema failed: %v", err)
	}
	schemaRecord := schemaPage.LeafCells[0].Record
	rootPageNum, ok := schemaRecord[3].(int64)
	if !ok {
		t.Fatalf("could not get root page number from schema")
	}

	t.Run("find existing record", func(t *testing.T) {
		// We inserted 500 rows, let's find one in the middle.
		targetRowID := int64(250)
		record, err := db.Find(int(rootPageNum), targetRowID)
		if err != nil {
			t.Fatalf("Find() returned an unexpected error: %v", err)
		}

		if len(record) != 2 {
			t.Fatalf("expected record to have 2 columns, got %d", len(record))
		}

		// Column 0: id (INTEGER)
		if id, ok := record[0].(int64); !ok || id != targetRowID {
			t.Errorf("expected record col 0 (id) to be %d, got %v", targetRowID, record[0])
		}

		// Column 1: name (TEXT)
		expectedName := "name250"
		if name, ok := record[1].(string); !ok || name != expectedName {
			t.Errorf("expected record col 1 (name) to be %q, got %v", expectedName, record[1])
		}
	})

	t.Run("find non-existent record", func(t *testing.T) {
		targetRowID := int64(9999) // This rowID does not exist.
		_, err := db.Find(int(rootPageNum), targetRowID)
		if err == nil {
			t.Fatal("Find() expected an error for non-existent row, but got nil")
		}
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Find() returned error %v, want %v", err, ErrNotFound)
		}
	})
}

func TestDatabase_Scan(t *testing.T) {
	dbPath := createTestDB(t, "scan_test.sqlite")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() failed with error: %v", err)
	}
	defer db.Close()

	// Get the root page of the 'test' table from the schema.
	schemaPage, err := db.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1) for schema failed: %v", err)
	}
	rootPageNum, ok := schemaPage.LeafCells[0].Record[3].(int64)
	if !ok {
		t.Fatalf("could not get root page number from schema")
	}

	t.Run("full table scan", func(t *testing.T) {
		var count int
		for record, err := range db.Scan(int(rootPageNum)) {
			if err != nil {
				t.Fatalf("Scan returned an unexpected error: %v", err)
			}
			if record == nil {
				t.Fatal("Scan returned nil record and nil error")
			}
			count++
		}
		if count != 500 {
			t.Errorf("expected to scan 500 records, but got %d", count)
		}
	})

	t.Run("stopped table scan", func(t *testing.T) {
		var count int
		for _, err := range db.Scan(int(rootPageNum)) {
			if err != nil {
				t.Fatalf("Scan returned an unexpected error: %v", err)
			}
			count++
			if count >= 10 {
				break
			}
		}
		if count != 10 {
			t.Errorf("expected to scan 10 records, but got %d", count)
		}
	})
}
