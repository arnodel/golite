package golite

import (
	"reflect"
	"testing"
)

func TestParseTableSQL(t *testing.T) {
	testCases := []struct {
		name         string
		sql          string
		wantCols     []ColumnInfo
		wantRowIDIdx int
		wantErr      bool
	}{
		{
			name: "simple table",
			sql:  "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)",
			wantCols: []ColumnInfo{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "TEXT"},
				{Name: "email", Type: "TEXT"},
			},
			wantRowIDIdx: -1,
		},
		{
			name: "with integer primary key",
			sql:  "CREATE TABLE products (product_id INTEGER PRIMARY KEY, name TEXT, price REAL)",
			wantCols: []ColumnInfo{
				{Name: "product_id", Type: "INTEGER"},
				{Name: "name", Type: "TEXT"},
				{Name: "price", Type: "REAL"},
			},
			wantRowIDIdx: 0,
		},
		{
			name: "with quoted identifiers",
			sql:  "CREATE TABLE `orders` (`order_id` INTEGER, `user_id` INTEGER, `order_date` TEXT)",
			wantCols: []ColumnInfo{
				{Name: "order_id", Type: "INTEGER"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "order_date", Type: "TEXT"},
			},
			wantRowIDIdx: -1,
		},
		{
			name:    "malformed sql no parens",
			sql:     "CREATE TABLE no_parens",
			wantErr: true,
		},
		{
			name:    "malformed column def",
			sql:     "CREATE TABLE bad_col (id, name TEXT)",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cols, rowIDIdx, err := ParseTableSQL(tc.sql)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseTableSQL() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return // Don't check return values if an error was expected.
			}

			if !reflect.DeepEqual(cols, tc.wantCols) {
				t.Errorf("ParseTableSQL() cols = %v, want %v", cols, tc.wantCols)
			}
			if rowIDIdx != tc.wantRowIDIdx {
				t.Errorf("ParseTableSQL() rowIDIdx = %d, want %d", rowIDIdx, tc.wantRowIDIdx)
			}
		})
	}
}
