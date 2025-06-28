# golite

[![Go Reference](https://pkg.go.dev/badge/github.com/arnodel/golite.svg)](https://pkg.go.dev/github.com/arnodel/golite)
[![Go Report Card](https://goreportcard.com/badge/github.com/arnodel/golite)](https://goreportcard.com/report/github.com/arnodel/golite)

*(Note: Badges will become active as the project is populated and versioned.)*

A lightweight, pure Go, read-only library for SQLite database files.

## Aim of the Project

The primary goal of `golite` is to provide a Go native library to read and query data from SQLite files **without using CGo**.

Many existing Go libraries for SQLite are wrappers around the official C library. While powerful, this introduces a C dependency, which complicates cross-compilation and breaks the simplicity of a standard Go build environment.

`golite` aims to solve this by parsing the SQLite file format directly. The key principles are:
- **Pure Go:** No CGo, no external system dependencies.
- **Read-Only:** The library will focus exclusively on data extraction, which simplifies the scope and avoids the complexities of writing, locking, and journaling.
- **Low-Level Primitives:** Instead of a full SQL interpreter, `golite` will provide a set of Go-native query primitives for filtering and selecting data. This can serve as a foundation for higher-level tools or be used directly by applications.

## Current Status

⚠️ **This project is in the very early stages of development and is not yet functional.** ⚠️

The repository has just been created, and work is beginning based on the roadmap below.

## Roadmap

This roadmap outlines the planned development steps to reach version 1.0.

-   [x] **1. Parse File Header:** Read and parse the 100-byte database header to identify the file as a valid SQLite database and retrieve key metadata.
-   [x] **2. Navigate B-Tree Pages:** Implement the core logic for reading pages and navigating the B-tree data structure that SQLite uses for tables and indexes.
-   [ ] **3. Read Schema Table:** Use the B-tree logic to find and parse the `sqlite_schema` table, which contains the definitions for all other tables, indexes, and views.
-   [ ] **4. Read Table Records:** Implement logic to parse raw record data from table pages into structured Go types.
-   [ ] **5. Implement Query Primitives:** Develop a simple, programmatic API for filtering records (e.g., `WHERE`-like functionality).
-   [ ] **Future: Joins:** Explore implementing logic for joining data between tables.

## Installation

Once the library is functional, it will be installable via `go get`:
```sh
go get [github.com/arnodel/golite](https://github.com/arnodel/golite)
```

## Usage (Future Vision)

The following is a speculative example of what the API might look like:

```go
package main

import (
    "fmt"
    "log"

    "[github.com/arnodel/golite](https://github.com/arnodel/golite)"
)

func main() {
    db, err := golite.Open("my_database.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Example 1: List all tables in the database
    tables, err := db.Tables()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Tables:", tables)

    // Example 2: A potential future query API
    //
    // results, err := db.Table("users").
    //     Filter(golite.Field("status").Equals("active")).
    //     Select("name", "email")
    //
    // if err != nil {
    //     log.Fatal(err)
    // }
    //
    // for _, row := range results {
    //     fmt.Println(row["name"], row["email"])
    // }
}
```

## Contributing

Contributions and ideas are welcome! As the project is just starting, please feel free to open an issue to discuss the roadmap or implementation details.
