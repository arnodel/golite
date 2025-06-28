package golite

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// createTestDB is a helper function that runs a shell script to create a
// fresh SQLite database file for testing. It returns the path to the created file.
func createTestDB(t *testing.T, filename string) string {
	t.Helper()
	dir := t.TempDir()
	// The testdata directory must be in the same directory as the test files.
	scriptPath := filepath.Join("testdata", "create_db.sh")
	dbPath := filepath.Join(dir, filename)

	// Ensure the script is executable, as git doesn't always preserve permissions.
	if err := os.Chmod(scriptPath, 0755); err != nil {
		t.Fatalf("could not make script executable: %v", err)
	}

	cmd := exec.Command(scriptPath, dbPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to create test database: %v\nOutput: %s", err, string(output))
	}
	return dbPath
}

func TestParseHeader(t *testing.T) {
	t.Run("valid header from generated file", func(t *testing.T) {
		dbPath := createTestDB(t, "valid.sqlite")

		// Set a specific user version to test that field.
		userVersionCmd := exec.Command("sqlite3", dbPath, "PRAGMA user_version = 12345;")
		if output, err := userVersionCmd.CombinedOutput(); err != nil {
			t.Fatalf("failed to set user_version: %v\nOutput: %s", err, string(output))
		}

		data, err := os.ReadFile(dbPath)
		if err != nil {
			t.Fatalf("failed to read test database file: %v", err)
		}

		headerBytes := data[:HeaderSize]
		header, err := ParseHeader(headerBytes)
		if err != nil {
			t.Fatalf("ParseHeader() failed with error: %v", err)
		}

		// The default page size created by the sqlite3 CLI is 4096.
		if header.PageSize != 4096 {
			t.Errorf("expected PageSize 4096, got %d", header.PageSize)
		}

		// The text encoding for a database created by a modern SQLite version
		// is always UTF-8, which is represented by the integer 1.
		if header.TextEncoding != 1 {
			t.Errorf("expected TextEncoding 1 (UTF-8), got %d", header.TextEncoding)
		}

		// Check the user version we just set.
		if header.UserVersion != 12345 {
			t.Errorf("expected UserVersion 12345, got %d", header.UserVersion)
		}

		// Check the schema format, which should be 4 for modern databases.
		if header.SchemaFormat != 4 {
			t.Errorf("expected SchemaFormat 4, got %d", header.SchemaFormat)
		}

		// The database should have at least one page (the schema table).
		if header.DatabaseSize == 0 {
			t.Error("expected DatabaseSize to be > 0, got 0")
		}
	})

	t.Run("invalid header size", func(t *testing.T) {
		_, err := ParseHeader(make([]byte, 50))
		if err == nil {
			t.Error("expected an error for header with invalid size, but got nil")
		}
	})

	t.Run("invalid header string", func(t *testing.T) {
		invalidData := make([]byte, HeaderSize)
		copy(invalidData, []byte("This is not SQLite"))
		_, err := ParseHeader(invalidData)
		if err == nil {
			t.Error("expected an error for header with invalid magic string, but got nil")
		}
	})
}
