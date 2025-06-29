package golite

import (
	"testing"
)

func TestDatabase_GetSchema(t *testing.T) {
	dbPath := createTestDB(t, "schema_test.sqlite")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() failed with error: %v", err)
	}
	defer db.Close()

	schema, err := db.GetSchema()
	if err != nil {
		t.Fatalf("GetSchema() failed with error: %v", err)
	}

	if schema == nil {
		t.Fatal("GetSchema() returned nil schema")
	}
	if schema.Tables == nil {
		t.Fatal("schema.Tables is nil")
	}

	if len(schema.Tables) != 2 {
		t.Fatalf("expected 1 table in schema, got %d", len(schema.Tables))
	}

	testTable, ok := schema.Tables["test"]
	if !ok {
		t.Fatal("schema did not contain 'test' table")
	}

	if testTable.Name != "test" {
		t.Errorf("expected table name 'test', got %q", testTable.Name)
	}
	if testTable.RootPage == 0 || testTable.RootPage == 1 {
		t.Errorf("expected table root page to be a valid page number, got %d", testTable.RootPage)
	}
	expectedSQL := "CREATE TABLE test(id INTEGER PRIMARY KEY, name TEXT)"
	if testTable.SQL != expectedSQL {
		t.Errorf("expected table SQL %q, got %q", expectedSQL, testTable.SQL)
	}

	schemaTable, ok := schema.Tables["sqlite_schema"]
	if !ok {
		t.Fatal("schema did not contain 'sqlite_schema' table")
	}
	if schemaTable.Name != "sqlite_schema" {
		t.Errorf("expected schema table name 'sqlite_schema', got %q", schemaTable.Name)
	}
	if schemaTable.RootPage != 1 {
		t.Errorf("expected schema table root page to be 1, got %d", schemaTable.RootPage)
	}

	if len(schema.Indexes) != 1 {
		t.Fatalf("expected 1 index in schema, got %d", len(schema.Indexes))
	}

	testIndex, ok := schema.Indexes["idx_name"]
	if !ok {
		t.Fatal("schema did not contain 'idx_name' index")
	}
	if testIndex.TableName != "test" {
		t.Errorf("expected index table name 'test', got %q", testIndex.TableName)
	}
}
