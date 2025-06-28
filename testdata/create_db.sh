#!/bin/bash
# This script creates a fresh SQLite database file for testing.
set -e
DB_FILE=${1:-"test.sqlite"}
rm -f "$DB_FILE"
# Create a simple table to ensure the database is initialized properly.
sqlite3 "$DB_FILE" "CREATE TABLE test(id INTEGER, name TEXT);" > /dev/null 2>&1