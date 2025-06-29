package golite

import (
	"testing"
)

func TestDatabase_TableSeek(t *testing.T) {
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
		iterator := db.TableSeek(testTable, targetRowID)

		count := 0
		for record, err := range iterator {
			if err != nil {
				t.Fatalf("TableSeek() iterator returned an unexpected error: %v", err)
			}
			count++

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
		}

		if count != 1 {
			t.Errorf("expected iterator to yield 1 record, but it yielded %d", count)
		}
	})

	t.Run("find non-existent record", func(t *testing.T) {
		targetRowID := int64(9999) // This rowID does not exist.
		iterator := db.TableSeek(testTable, targetRowID)
		count := 0
		for range iterator {
			count++
		}
		if count != 0 {
			t.Errorf("expected an empty iterator for non-existent row, but it yielded %d records", count)
		}
	})
}

func TestDatabase_IndexSeek(t *testing.T) {
	dbPath := createTestDB(t, "index_seek_test.sqlite")
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

		iterator := db.IndexSeek(indexInfo, key)
		count := 0
		for record, err := range iterator {
			if err != nil {
				t.Fatalf("IndexSeek() iterator returned an unexpected error: %v", err)
			}
			count++

			// The yielded record is the index record: (key, rowid)
			if len(record) != 2 {
				t.Fatalf("expected index record to have 2 columns, got %d", len(record))
			}
			if name, ok := record[0].(string); !ok || name != "name300" {
				t.Errorf("expected key 'name300', got %v", record[0])
			}
			if rowid, ok := record[1].(int64); !ok || rowid != expectedRowID {
				t.Errorf("expected rowid %d, got %v", expectedRowID, record[1])
			}
		}
		if count != 1 {
			t.Errorf("expected iterator to yield 1 record, but it yielded %d", count)
		}
	})

	t.Run("index seek non-existent key", func(t *testing.T) {
		key := Record{"non_existent_name"}
		iterator := db.IndexSeek(indexInfo, key)
		count := 0
		for range iterator {
			count++
		}
		if count != 0 {
			t.Errorf("expected an empty iterator for non-existent key, but it yielded %d records", count)
		}
	})
}

func TestDatabase_IndexScan(t *testing.T) {
	dbPath := createTestDB(t, "index_scan_test.sqlite")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() failed with error: %v", err)
	}
	defer db.Close()
	// We'll use the 'index_scan_test.sqlite' database, which should already
	// contain a table 'test' with an index 'idx_name' on the 'name' column,
	// populated with 500 rows. The create_db.sh script handles this setup.

	schema, err := db.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema() failed: %v", err)
	}

	// Get the index info.
	indexInfo, ok := schema.Indexes["idx_name"] // We're now looking for "idx_name"
	if !ok {
		t.Fatalf("schema did not contain 'idx_name' index")
	}

	// Perform an IndexScan
	iterator := db.IndexScan(indexInfo)
	count := 0
	var prevRecord Record

	for record, err := range iterator {
		if err != nil {
			t.Fatalf("IndexScan iterator returned an unexpected error: %v", err)
		}
		count++

		// Check that records are sorted.
		if prevRecord != nil {
			if CompareRecords(prevRecord, record) > 0 {
				t.Errorf("IndexScan yielded unsorted records: %v came after %v", record, prevRecord)
			}
		}
		prevRecord = record
	}

	if count != 500 {
		t.Errorf("expected to scan 500 index records, but got %d", count)
	}

	// Get the root page of the 'test' table from the schema.
	schema, err = db.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema() failed: %v", err)
	}
	testTable, ok := schema.Tables["test"]
	if !ok {
		t.Fatalf("schema did not contain 'test' table")
	}

	t.Run("full table scan", func(t *testing.T) {
		var count int
		for record, err := range db.TableScan(testTable) {
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
		for _, err := range db.TableScan(testTable) {
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
