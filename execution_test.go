package golite

import (
	"fmt"
	"testing"
)

func TestFilter(t *testing.T) {
	dbPath := createTestDB(t, "filter_test.sqlite")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() failed with error: %v", err)
	}
	defer db.Close()

	schema, err := db.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema() failed: %v", err)
	}
	testTable, ok := schema.Tables["test"]
	if !ok {
		t.Fatalf("schema did not contain 'test' table")
	}

	// The full table scan is our source iterator.
	var sourceIterator RecordIterator = db.TableScan(testTable)

	t.Run("filter by rowid", func(t *testing.T) {
		// Create a predicate that filters for rows with rowid > 450.
		predicate := func(record Record) (bool, error) {
			return record[0].(int64) > 450, nil
		}

		filteredIterator := Filter(sourceIterator, predicate)

		count := 0
		for _, err := range filteredIterator {
			if err != nil {
				t.Fatalf("iterator returned an error: %v", err)
			}
			count++
		}

		// We inserted 500 rows, so 50 should match (451 to 500).
		if count != 50 {
			t.Errorf("expected 50 rows, got %d", count)
		}
	})

	t.Run("filter by column value", func(t *testing.T) {
		// Create a predicate that finds a specific name.
		predicate := func(record Record) (bool, error) {
			// In a real scenario, we'd look up the column index from the schema.
			// For this test, we know 'name' is at index 1 of the original schema.
			name, ok := record[1].(string)
			if !ok {
				return false, fmt.Errorf("column 1 is not a string")
			}
			return name == "name123", nil
		}

		filteredIterator := Filter(sourceIterator, predicate)

		// We expect exactly one row to match.
		for record, err := range filteredIterator {
			if record[0].(int64) != 123 {
				t.Errorf("expected rowid 123, got %d", record[0])
			}
			if err != nil {
				t.Fatalf("iterator returned an error: %v", err)
			}
		}
	})
}
