package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/julio77it/database/columns"

	_ "github.com/mattn/go-sqlite3"
)

const (
	// for a simple example, I'v chosen sqlite3
	driver string = "sqlite3"
	// SQL : query the table
	queryStmt string = "SELECT * FROM quotes LIMIT 1"
)

func main() {
	// create new data database
	db, err := sql.Open(driver, "columns/sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err := db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}

	// query the table
	rows, err := db.Query(queryStmt)
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer rows.Close()

	// the core of the example starts here
	// database/columns/RowsWithColumns(sql.SQLRows) : get the fields info from the resultset
	sqlRows, err := columns.New(rows)

	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}

	for ri := 0; sqlRows.Next(); ri++ {
		fmt.Printf("ROW[%2d]\n", ri)

		for fi := 0; fi < sqlRows.Length(); fi++ {
			name, value, err := sqlRows.GetFieldByIndex(fi)
			if err != nil {
				fmt.Println(err)
				os.Exit(0)
			}
			fmt.Printf("\tFIELD[%2d] %s[%t] = %v\n", fi, name, value, value)
		}

	}
	if err := sqlRows.Err(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
}
