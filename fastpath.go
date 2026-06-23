// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// fastpath.go provides ReadBatch — a method on *Rows that bypasses
// database/sql.Scan and convertAssign, eliminating per-row reflection
// and interface boxing overhead.
//
// Access via sql.Conn.Raw():
//
//	conn.Raw(func(driverConn interface{}) error {
//	    c := driverConn.(*goibmdb.Conn)
//	    dr, _ := c.Query("SELECT a, b, c FROM t", nil)
//	    defer dr.Close()
//	    gr := dr.(*goibmdb.Rows)
//
//	    ids   := make([]int64,  200)
//	    names := make([]string, 200)
//	    ts    := make([]time.Time, 200)
//	    nulls := [][]bool{make([]bool, 200), make([]bool, 200), make([]bool, 200)}
//	    for {
//	        n, err := gr.ReadBatch([]interface{}{&ids, &names, &ts}, nulls)
//	        if err == io.EOF { break }
//	        if err != nil { return err }
//	        // nulls[c][i] is true when column c, row i is SQL NULL.
//	        // process ids[:n], names[:n], ts[:n] with null awareness
//	    }
//	    return nil
//	})

package go_ibm_db

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"time"
	"unsafe"

	"github.com/ibmdb/go_ibm_db/api"
	trc "github.com/ibmdb/go_ibm_db/log2"
)

// ReadBatch calls SQLFetch once and decodes all fetched rows into pre-allocated
// destination slices, returning the number of rows decoded.
//
// dst must hold one *[]T pointer per column; accepted element types:
// *[]int64, *[]int32, *[]float64, *[]bool, *[]string, *[][]byte, *[]time.Time.
// Each slice must be pre-allocated to at least FetchSize capacity.
//
// nulls must hold one []bool per column, each with length >= FetchSize.
// On return, nulls[c][i] is true when column c, row i is SQL NULL.
// The typed destination slices store a zero-value for NULL cells; callers
// must check nulls[c][i] to distinguish NULL from a real zero-value.
func (r *Rows) ReadBatch(dst []interface{}, nulls [][]bool) (n int, err error) {
	trc.Trace1(fmt.Sprintf("fastpath.go: ReadBatch() cols=%d - ENTRY", len(dst)))

	os := r.os

	// Validate column counts before consuming a rowset from the cursor.
	if len(dst) != len(os.Cols) {
		return 0, fmt.Errorf("ReadBatch: %d destinations for %d columns", len(dst), len(os.Cols))
	}
	if len(nulls) != len(os.Cols) {
		return 0, fmt.Errorf("ReadBatch: %d null masks for %d columns", len(nulls), len(os.Cols))
	}

	ret := api.SQLFetch(os.h)
	if ret != api.SQL_NO_DATA && IsError(ret) {
		return 0, NewError("SQLFetch", os.h)
	}

	// Read RowsFetched regardless of isEOF: DB2 multi-row FETCH can return
	// SQL_NO_DATA (SQLCODE 100) with RowsFetched > 0 for the last partial batch
	// https://ftpdocs.broadcom.com/cadocs/0/CA%20Chorus%2003%200%2000-ENU/Bookshelf_Files/HTML/Performance%20Handbook%20for%20DB2/Multi_Row_FETCH.html
	if os.RowsFetched != nil {
		n = int(*os.RowsFetched)
	}

	if ret == api.SQL_NO_DATA && n == 0 {
		return 0, io.EOF
	}

	// Validate null mask lengths now that we know how many rows were fetched.
	for colIdx, nm := range nulls {
		if len(nm) < n {
			return 0, fmt.Errorf("ReadBatch: nulls[%d] len=%d < rows fetched=%d; pre-allocate to at least FetchSize", colIdx, len(nm), n)
		}
	}

	// Mark the full rowset consumed so Next() calls SQLFetch on the next call.
	r.rowIdx = n

	for colIdx, col := range os.Cols {
		bc, isBound := col.(*BindableColumn)
		if !isBound || !bc.IsBound {
			if err := fillNonBindableSlice(os, colIdx, col, dst[colIdx], n, nulls[colIdx]); err != nil {
				return 0, err
			}
			continue
		}
		if err := fillBindableSlice(bc, colIdx, dst[colIdx], n, nulls[colIdx]); err != nil {
			return 0, err
		}
	}

	trc.Trace1(fmt.Sprintf("fastpath.go: ReadBatch() n=%d err=%v - EXIT", n, err))

	if ret == api.SQL_NO_DATA {
		// Last partial batch: n rows are ready, and the cursor is exhausted.
		return n, io.EOF
	}
	return n, nil
}

// ─── helpers ────────────────────────────────────────────────────────────────

// fillBindableSlice decodes all n rows for one bound column into a slice.
// nullMask[i] is set to true when row i is SQL NULL; the typed slice stores
// a zero-value for those rows. Callers must check nullMask to distinguish
// NULL from a real zero-value.
func fillBindableSlice(bc *BindableColumn, colIdx int, dst interface{}, n int, nullMask []bool) error {
	switch d := dst.(type) {
	case *[]int64:
		fillBoundRows(bc, d, n, nullMask, int64(0), func(i int) int64 {
			return *(*int64)(unsafe.Pointer(&bc.Buffer[i*bc.Size]))
		})
	case *[]int32:
		fillBoundRows(bc, d, n, nullMask, int32(0), func(i int) int32 {
			return *(*int32)(unsafe.Pointer(&bc.Buffer[i*bc.Size]))
		})
	case *[]float64:
		fillBoundRows(bc, d, n, nullMask, float64(0), func(i int) float64 {
			return *(*float64)(unsafe.Pointer(&bc.Buffer[i*bc.Size]))
		})
	case *[]bool:
		fillBoundRows(bc, d, n, nullMask, false, func(i int) bool {
			return bc.Buffer[i*bc.Size] != 0
		})
	case *[]string:
		switch bc.CType {
		case api.SQL_C_CHAR:
			fillBoundRows(bc, d, n, nullMask, "", func(i int) string {
				dataLen := boundDataLen(bc, i)
				start := i * bc.Size
				buf := bc.Buffer[start : start+dataLen]
				if bc.SType == api.SQL_DECIMAL || bc.SType == api.SQL_DECFLOAT {
					// "45,234" → "45.234" so the value parses as a float (see column.go Value).
					buf = bytes.Replace(buf, []byte(","), []byte("."), 1)
				}
				return string(buf)
			})
		case api.SQL_C_WCHAR:
			fillBoundRows(bc, d, n, nullMask, "", func(i int) string {
				dataLen := boundDataLen(bc, i)
				if dataLen == 0 {
					return ""
				}
				start := i * bc.Size
				raw := bc.Buffer[start : start+dataLen]
				s := (*[1 << 20]uint16)(unsafe.Pointer(&raw[0]))[: len(raw)/2 : len(raw)/2]
				return string(utf16toutf8(s))
			})
		case api.SQL_C_DBCHAR:
			fillBoundRows(bc, d, n, nullMask, "", func(i int) string {
				dataLen := boundDataLen(bc, i)
				if dataLen == 0 {
					return ""
				}
				start := i * bc.Size
				return string(dbclobToUTF8(bc.Buffer[start : start+dataLen]))
			})
		default:
			fillBoundRows(bc, d, n, nullMask, "", func(i int) string {
				dataLen := boundDataLen(bc, i)
				start := i * bc.Size
				return string(bc.Buffer[start : start+dataLen])
			})
		}
	case *[][]byte:
		fillBoundRows(bc, d, n, nullMask, []byte(nil), func(i int) []byte {
			dataLen := boundDataLen(bc, i)
			start := i * bc.Size
			cp := make([]byte, dataLen)
			copy(cp, bc.Buffer[start:start+dataLen])
			if bc.SType == api.SQL_DECIMAL || bc.SType == api.SQL_DECFLOAT {
				// "45,234" → "45.234" in place (same fix as column.go Value).
				for k := 0; k < dataLen; k++ {
					if cp[k] == ',' {
						cp[k] = '.'
						break
					}
				}
			}
			return cp
		})
	case *[]time.Time:
		fillBoundRows(bc, d, n, nullMask, time.Time{}, func(i int) time.Time {
			start := i * bc.Size
			p := unsafe.Pointer(&bc.Buffer[start])
			switch bc.CType {
			case api.SQL_C_TYPE_TIMESTAMP:
				t := (*api.SQL_TIMESTAMP_STRUCT)(p)
				return time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
					int(t.Hour), int(t.Minute), int(t.Second), int(t.Fraction), time.UTC)
			case api.SQL_C_TYPE_TIMESTAMP_EXT_TZ:
				t := (*api.SQL_TIMESTAMP_STRUCT_EXT_TZ)(p)
				offset := int(t.TimezoneHour)*3600 + int(t.TimezoneMinute)*60
				return time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
					int(t.Hour), int(t.Minute), int(t.Second), int(t.Fraction),
					time.FixedZone("", offset))
			case api.SQL_C_TYPE_DATE:
				t := (*api.SQL_DATE_STRUCT)(p)
				return time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
					0, 0, 0, 0, time.Local)
			case api.SQL_C_TYPE_TIME:
				t := (*api.SQL_TIME_STRUCT)(p)
				return time.Date(1, 1, 1, int(t.Hour), int(t.Minute), int(t.Second), 0, time.Local)
			default:
				return time.Time{}
			}
		})
	default:
		return fmt.Errorf("ReadBatch: unsupported destination type %T for column %d", dst, colIdx)
	}
	return nil
}

// fillNonBindableSlice fills n rows from a non-bindable column using SQLGetData
// row by row. Truly non-bindable types (CLOB/BLOB/XML) land here by design.
// Bindable-by-type columns that follow a non-bindable column in the result set
// also land here because binding stops at the first non-bindable column (ODBC
// restriction: SQLGetData may only be called on columns after the last bound
// column). Their col.Value() returns the native Go type, so we handle every
// type that buildColBuffers may allocate.
func fillNonBindableSlice(os *ODBCStmt, colIdx int, col Column, dst interface{}, n int, nullMask []bool) error {
	switch d := dst.(type) {
	case *[]string:
		// BaseColumn.Value returns []byte for SQL_C_CHAR (CLOB) and string for
		// SQL_C_DBCHAR (DBCLOB) / SQL_C_WCHAR. Coerce both to string here.
		return fillNonBindableRows(os, colIdx, col, d, n, nullMask, "", func(v driver.Value) (string, error) {
			switch s := v.(type) {
			case string:
				return s, nil
			case []byte:
				return string(s), nil
			default:
				return "", fmt.Errorf("ReadBatch: non-bindable column %d: expected string or []byte, got %T", colIdx, v)
			}
		})
	case *[][]byte:
		// BaseColumn.Value returns string for SQL_C_DBCHAR (DBCLOB) and []byte for
		// SQL_C_BINARY (XML) / SQL_C_CHAR (CLOB). Coerce both to []byte here.
		return fillNonBindableRows(os, colIdx, col, d, n, nullMask, []byte(nil), func(v driver.Value) ([]byte, error) {
			switch b := v.(type) {
			case []byte:
				return b, nil
			case string:
				return []byte(b), nil
			default:
				return nil, fmt.Errorf("ReadBatch: non-bindable column %d: expected []byte or string, got %T", colIdx, v)
			}
		})
	case *[]int32:
		return fillNonBindableRows(os, colIdx, col, d, n, nullMask, int32(0), func(v driver.Value) (int32, error) {
			if i, ok := v.(int32); ok {
				return i, nil
			}
			return 0, fmt.Errorf("ReadBatch: non-bindable column %d: expected int32, got %T", colIdx, v)
		})
	case *[]int64:
		return fillNonBindableRows(os, colIdx, col, d, n, nullMask, int64(0), func(v driver.Value) (int64, error) {
			if i, ok := v.(int64); ok {
				return i, nil
			}
			return 0, fmt.Errorf("ReadBatch: non-bindable column %d: expected int64, got %T", colIdx, v)
		})
	case *[]float64:
		return fillNonBindableRows(os, colIdx, col, d, n, nullMask, float64(0), func(v driver.Value) (float64, error) {
			if f, ok := v.(float64); ok {
				return f, nil
			}
			return 0, fmt.Errorf("ReadBatch: non-bindable column %d: expected float64, got %T", colIdx, v)
		})
	case *[]bool:
		return fillNonBindableRows(os, colIdx, col, d, n, nullMask, false, func(v driver.Value) (bool, error) {
			if b, ok := v.(bool); ok {
				return b, nil
			}
			return false, fmt.Errorf("ReadBatch: non-bindable column %d: expected bool, got %T", colIdx, v)
		})
	case *[]time.Time:
		return fillNonBindableRows(os, colIdx, col, d, n, nullMask, time.Time{}, func(v driver.Value) (time.Time, error) {
			if t, ok := v.(time.Time); ok {
				return t, nil
			}
			return time.Time{}, fmt.Errorf("ReadBatch: non-bindable column %d: expected time.Time, got %T", colIdx, v)
		})
	default:
		return fmt.Errorf("ReadBatch: non-bindable column %d: unsupported destination type %T", colIdx, dst)
	}
}

// fillNonBindableRows decodes n rows via SQLGetData into dst.
func fillNonBindableRows[T any](os *ODBCStmt, colIdx int, col Column, dst *[]T, n int, nullMask []bool, zero T, decode func(driver.Value) (T, error)) error {
	ensureSliceLen(dst, n)
	for i := 0; i < n; i++ {
		v, err := col.Value(os.h, colIdx, i)
		if err != nil {
			return err
		}
		isNull := v == nil
		if nullMask != nil {
			nullMask[i] = isNull
		}
		if isNull {
			(*dst)[i] = zero
			continue
		}
		val, err := decode(v)
		if err != nil {
			return err
		}
		(*dst)[i] = val
	}
	return nil
}

// fillBoundRows decodes n rows from a bound ODBC column buffer into dst.
// nullMask[i] is set when row i is SQL NULL; NULL rows store zeroVal in dst.
func fillBoundRows[T any](bc *BindableColumn, dst *[]T, n int, nullMask []bool, zeroVal T, decode func(i int) T) {
	ensureSliceLen(dst, n)
	for i := 0; i < n; i++ {
		isNull := bc.LenBuffer[i].IsNull()
		if nullMask != nil {
			nullMask[i] = isNull
		}
		if isNull {
			(*dst)[i] = zeroVal
			continue
		}
		(*dst)[i] = decode(i)
	}
}

// boundDataLen returns how many bytes in bc.Buffer[row i] are valid for decoding.
// LenBuffer[i] is usually < bc.Size (short VARCHAR/BINARY values), may equal bc.Size
// when the value fills the bind buffer, and can exceed bc.Size when ODBC reports
// the full untruncated length after truncation (SQLSTATE 01004); cap to bc.Size.
func boundDataLen(bc *BindableColumn, i int) int {
	dataLen := int(bc.LenBuffer[i])
	return min(dataLen, bc.Size)
}

// ensureSliceLen grows the slice to length n if needed.
func ensureSliceLen[T any](s *[]T, n int) {
	if len(*s) < n {
		*s = make([]T, n)
	}
}
