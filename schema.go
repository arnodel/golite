package golite

// TableInfo holds schema information about a single table.
type TableInfo struct {
	Name             string
	RootPage         int
	SQL              string
	RowIDColumnIndex int // The index of the column that is an alias for the rowid. -1 if none.
}

// IndexInfo holds schema information about a single index.
type IndexInfo struct {
	Name      string
	TableName string
	RootPage  int
	SQL       string
}

// Schema holds the parsed schema for the entire database.
type Schema struct {
	Tables  map[string]TableInfo
	Indexes map[string]IndexInfo
}
