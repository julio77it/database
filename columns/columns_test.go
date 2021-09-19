package columns

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const (
	// SQL : query the table
	queryStmt string = "SELECT * FROM quotes LIMIT 50"
)

func TestNew(t *testing.T) {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err = db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}

	rows, err := db.Query(queryStmt)
	if err != nil {
		t.Errorf("db.Query failed : %v", err)
	}

	// OK
	_, err = New(rows)
	if err != nil {
		t.Errorf("New failed : %v", err)
	}
	rows.Close()

	// KO
	_, err = New(rows)
	if err == nil {
		t.Errorf("New error expected, got %v", err)
	}
}

func TestLength(t *testing.T) {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err = db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	rows, err := db.Query(queryStmt)
	if err != nil {
		t.Errorf("db.Query failed : %v", err)
	}
	defer rows.Close()
	// OK
	rh, err := New(rows)
	if err != nil {
		t.Errorf("New failed : %v", err)
	}
	length := rh.Length()
	if length != 4 {
		t.Errorf("RowsWithColumns.Length expected 3, got %d", length)
	}
}

func TestNext(t *testing.T) {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err = db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	rows, err := db.Query(queryStmt)
	if err != nil {
		t.Errorf("db.Query failed : got %v", err)
	}
	rh, err := New(rows)
	if err != nil {
		t.Errorf("New failed : got %v", err)
	}
	if !rh.Next() {
		t.Errorf("RowsWithColumns.Next failed : got %v", err)
	}
	rows.Close()

	if rh.Next() {
		t.Errorf("RowsWithColumns.Next not failed : error expected")
	}
}

func TestGetFieldByIndex(t *testing.T) {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err = db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	rows, err := db.Query(queryStmt)
	if err != nil {
		t.Errorf("db.Query failed : got %v", err)
	}
	defer rows.Close()
	rh, err := New(rows)
	if err != nil {
		t.Errorf("New failed : got %v", err)
	}
	if !rh.Next() {
		t.Errorf("RowsWithColumns.Next failed : got %v", err)
	}
	if _, _, err := rh.GetFieldByIndex(-1); err == nil {
		t.Errorf("RowsWithColumns.GetFieldByIndex not failed : error expected")
	}
	if _, _, err := rh.GetFieldByIndex(0); err != nil {
		t.Errorf("RowsWithColumns.GetFieldByIndex failed : got %v", err)
	}
	if _, _, err := rh.GetFieldByIndex(1); err != nil {
		t.Errorf("RowsWithColumns.GetFieldByIndex failed : got %v", err)
	}
	if _, _, err := rh.GetFieldByIndex(2); err != nil {
		t.Errorf("RowsWithColumns.GetFieldByIndex failed : got %v", err)
	}
	if _, _, err := rh.GetFieldByIndex(3); err != nil {
		t.Errorf("RowsWithColumns.GetFieldByIndex failed : got %v", err)
	}
	if _, _, err := rh.GetStringFieldByIndex(-1); err == nil {
		t.Errorf("RowsWithColumns.GetStringFieldByIndex not failed : error expected")
	}
	if _, _, err := rh.GetStringFieldByIndex(1); err != nil {
		t.Errorf("RowsWithColumns.GetStringFieldByIndex failed : got %v", err)
	}
}

func TestGetFieldByName(t *testing.T) {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err = db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	rows, err := db.Query(queryStmt)
	if err != nil {
		t.Errorf("db.Query failed : got %v", err)
	}
	defer rows.Close()
	rh, err := New(rows)
	if err != nil {
		t.Errorf("New failed : got %v", err)
	}
	if !rows.Next() {
		t.Errorf("RowsWithColumns.Next failed : got %v", err)
	}
	if _, _, err := rh.GetFieldByName("BOH"); err == nil {
		t.Errorf("RowsWithColumns.GetFieldByName not failed : error expected")
	}
	if _, _, err := rh.GetFieldByName("author"); err != nil {
		t.Errorf("RowsWithColumns.GetFieldByName failed : got %v", err)
	}
}

// BenchmarkRows : test database/sql.Rows
func BenchmarkRows(b *testing.B) {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err = db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	b.ResetTimer()

	var author, quote string
	for n := 0; n < b.N; n++ {
		rows, err := db.Query(queryStmt)
		if err != nil {
			b.Errorf("db.Query failed : got %v", err)
		}
		for rows.Next() {
			rows.Scan(author, quote)
		}
		rows.Close()
	}
}

// BenchmarkRowsWithColumnsGetByIndex : test columns.RowsWithColumns.GetFieldByIndex
func BenchmarkRowsWithColumnsGetByIndex(b *testing.B) {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err = db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows, err := db.Query(queryStmt)
		if err != nil {
			b.Errorf("db.Query failed : got %v", err)
		}
		rh, err := New(rows)
		if err != nil {
			b.Errorf("New failed : got %v", err)
		}
		for rh.Next() {
			rh.GetFieldByIndex(0)
			rh.GetFieldByIndex(1)
		}
		rows.Close()
	}
}

// BenchmarkRowsWithColumnsGetByName : test columns.RowsWithColumns.GetFieldByName
func BenchmarkRowsWithColumnsGetByName(b *testing.B) {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err = db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows, err := db.Query(queryStmt)
		if err != nil {
			b.Errorf("db.Query failed : got %v", err)
		}
		rh, err := New(rows)
		if err != nil {
			b.Errorf("New failed : got %v", err)
		}
		for rh.Next() {
			rh.GetFieldByName("author")
			rh.GetFieldByName("quoteText")
		}
		rows.Close()
	}
}

// BenchmarkRowsWithColumnsGetAllByIndex : test columns.RowsWithColumns.GetFieldByIndex
func BenchmarkRowsWithColumnsGetAllByIndex(b *testing.B) {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	defer db.Close()

	// check the connection
	if err = db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows, err := db.Query(queryStmt)
		if err != nil {
			b.Errorf("db.Query failed : got %v", err)
		}
		rh, err := New(rows)
		if err != nil {
			b.Errorf("New failed : got %v", err)
		}
		for rh.Next() {
			for i := 0; i < rh.Length(); i++ {
				rh.GetFieldByIndex(i)
			}
		}
		rows.Close()
	}
}

func ExampleRowsWithColumns() {
	// open database
	db, err := sql.Open("sqlite3", "sql_test.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	// query
	rows, err := db.Query("SELECT * FROM quotes LIMIT 1")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	// promote sql.Rows in columns.RowsWithColumns
	rh, err := New(rows)
	if err != nil {
		fmt.Println(err)
		return
	}
	// 1st row
	if rh.Next() {
		fmt.Println(rh.Length())
	}
	// Output:
	// 4
}
