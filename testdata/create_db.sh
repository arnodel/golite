#!/bin/bash
# This script creates a fresh SQLite database file for testing.
set -e
DB_FILE=${1:-"test.sqlite"}
rm -f "$DB_FILE"
# Create a simple table to ensure the database is initialized properly.
sqlite3 "$DB_FILE" "
CREATE TABLE test(id INTEGER PRIMARY KEY, name TEXT);
-- Insert enough rows to force the creation of interior pages.
-- A page size of 4096 bytes can hold a few hundred small rows.
WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt LIMIT 500)
INSERT INTO test (name) SELECT 'name' || x FROM cnt;
" > /dev/null 2>&1