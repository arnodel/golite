package golite

import (
	"fmt"
	"strings"
)

// ParseTableSQL parses a CREATE TABLE statement to extract column information.
// It returns a slice of ColumnInfo and the index of the rowid alias column (-1 if none).
// NOTE: This is a simplified parser and may not handle all valid SQL syntax,
// especially complex constraints or types with parentheses.
func ParseTableSQL(sql string) ([]ColumnInfo, int, error) {
	start := strings.Index(sql, "(")
	if start == -1 {
		return nil, -1, fmt.Errorf("invalid CREATE TABLE statement: missing opening parenthesis")
	}
	// We assume the column definitions end at the last parenthesis.
	// This is fragile but works for simple CREATE TABLE statements.
	end := strings.LastIndex(sql, ")")
	if end <= start {
		return nil, -1, fmt.Errorf("invalid CREATE TABLE statement: missing closing parenthesis")
	}

	defsStr := sql[start+1 : end]
	defs := strings.Split(defsStr, ",")

	var columns []ColumnInfo
	rowIDColumnIndex := -1

	for i, def := range defs {
		def = strings.TrimSpace(def)
		parts := strings.Fields(def)
		if len(parts) < 2 {
			return nil, -1, fmt.Errorf("malformed column definition: %q", def)
		}

		columns = append(columns, ColumnInfo{Name: strings.Trim(parts[0], "\"`"), Type: parts[1]})

		if strings.Contains(strings.ToUpper(def), "INTEGER PRIMARY KEY") {
			rowIDColumnIndex = i
		}
	}

	return columns, rowIDColumnIndex, nil
}
