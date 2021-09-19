package columns

import (
	"database/sql"
	"errors"
	"strconv"
)

// New : build a RowsWithColumns struct from a database/sql.Rows
func New(rows *sql.Rows) (*RowsWithColumns, error) {
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	return &RowsWithColumns{rows, ct, make([]interface{}, len(ct))}, nil
}

// RowsWithColumns : holds the info about sql.Row fields
type RowsWithColumns struct {
	*sql.Rows
	columnTypes []*sql.ColumnType
	columnBytes []interface{}
}

// Length : number of fields of the result set
func (rh RowsWithColumns) Length() int {
	return len(rh.columnTypes)
}

// Next : read the bytes of the current row
func (rh *RowsWithColumns) Next() bool {
	b := rh.Rows.Next()
	if !b {
		return b
	}
	// reserve memory space
	for i := 0; i < len(rh.columnBytes); i++ {
		rh.columnBytes[i] = new(sql.RawBytes)
	}
	// read as variadic parameters
	if err := rh.Scan(rh.columnBytes...); err != nil {
		panic(err)
	}
	return true
}

func scanValue(colType *sql.ColumnType, bytes []byte) interface{} {
	switch colType.DatabaseTypeName() {
	case "INTEGER":
		integer, _ := strconv.ParseInt(string(bytes), 10, 64)
		return integer

	case "TEXT", "CHAR", "VARCHAR":
		return string(bytes)

	case "NUMERIC", "REAL", "FLOAT", "DOUBLE":
		float, _ := strconv.ParseFloat(string(bytes), 64)
		return float

	default:
		return string(bytes)
	}
}

// GetFieldByIndex : find a field By index. Return name, value and error
func (rh RowsWithColumns) GetFieldByIndex(index int) (string, interface{}, error) {
	// Check the input parameters
	if index < 0 || index >= len(rh.columnTypes) {
		// return zerov, zerov, error
		return "", nil, errors.New("index out of bound")
	}
	return rh.columnTypes[index].Name(), scanValue(rh.columnTypes[index], *rh.columnBytes[index].(*sql.RawBytes)), nil
}

// GetStringFieldByIndex : find a field By index. Return name, value as string and error
func (rh RowsWithColumns) GetStringFieldByIndex(index int) (string, string, error) {
	// Check the input parameters
	if index < 0 || index >= len(rh.columnTypes) {
		// return zerov, zerov, error
		return "", "", errors.New("index out of bound")
	}
	return rh.columnTypes[index].Name(), string(*rh.columnBytes[index].(*sql.RawBytes)), nil
}

// GetFieldByName : find a field By name. Returns index, value and error
func (rh RowsWithColumns) GetFieldByName(name string) (int, interface{}, error) {
	err := errors.New("field not found")

	for i, v := range rh.columnTypes {
		// Find the index of the field name
		if name == v.Name() {
			// Get the field by index
			_, value, err := rh.GetFieldByIndex(i)
			if err != nil {
				break
			}
			// return index, value, zerov
			return i, value, nil
		}
	}
	// return zerov, zerov, nil
	return 0, nil, err
}
