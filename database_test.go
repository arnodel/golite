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
	schema, err := db.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema() failed: %v", err)
	}
	testTable, ok := schema.Tables["test"]
	if !ok {
		t.Fatalf("schema did not contain 'test' table")
	}

	t.Run("find existing record", func(t *testing.T) {
		// We inserted 500 rows, let's find one in the middle.
		targetRowID := int64(250)
		record, err := db.Find(testTable, targetRowID)
		if err != nil {
			t.Fatalf("Find() returned an unexpected error: %v", err)
		}

		if len(record) != 2 {
			t.Fatalf("expected record to have 2 columns, got %d", len(record))
		}

		// Column 0: id (INTEGER)
		// This is an alias for the rowid, so it should match.
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
		_, err := db.Find(testTable, targetRowID)
		if err == nil {
			t.Fatal("Find() expected an error for non-existent row, but got nil")
		}
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Find() returned error %v, want %v", err, ErrNotFound)
		}
	})
}

func TestDatabase_FindInIndex(t *testing.T) {
	dbPath := createTestDB(t, "find_index_test.sqlite")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() failed with error: %v", err)
	}
	defer db.Close()

	schema, err := db.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema() failed: %v", err)
	}
	indexInfo, ok := schema.Indexes["idx_name"]
	if !ok {
		t.Fatalf("schema did not contain 'idx_name' index")
	}

	t.Run("find existing key in index", func(t *testing.T) {
		// The index is on the 'name' column. Let's find 'name300'.
		// The corresponding rowid should be 300.
		key := Record{"name300"}
		expectedRowID := int64(300)

		rowid, err := db.FindInIndex(indexInfo, key)
		if err != nil {
			t.Fatalf("FindInIndex() returned an unexpected error: %v", err)
		}

		if rowid != expectedRowID {
			t.Errorf("expected rowid %d, got %d", expectedRowID, rowid)
		}
	})

	t.Run("find non-existent key in index", func(t *testing.T) {
		key := Record{"non_existent_name"}
		_, err := db.FindInIndex(indexInfo, key)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("FindInIndex() returned error %v, want %v", err, ErrNotFound)
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
	schema, err := db.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema() failed: %v", err)
	}
	testTable, ok := schema.Tables["test"]
	if !ok {
		t.Fatalf("schema did not contain 'test' table")
	}

	t.Run("full table scan", func(t *testing.T) {
		var count int
		for record, err := range db.Scan(testTable) {
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
		for _, err := range db.Scan(testTable) {
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
