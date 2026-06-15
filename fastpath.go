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
	"fmt"
	"io"
	"time"
	"unsafe"

	"github.com/ibmdb/go_ibm_db/api"
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
//
// Returns (0, io.EOF) at end of result set.
func (r *Rows) ReadBatch(dst []interface{}, nulls [][]bool) (int, error) {
	os := r.os

	// Validate column counts before consuming a rowset from the cursor.
	if len(dst) != len(os.Cols) {
		return 0, fmt.Errorf("ReadBatch: %d destinations for %d columns", len(dst), len(os.Cols))
	}
	if len(nulls) != len(os.Cols) {
		return 0, fmt.Errorf("ReadBatch: %d null masks for %d columns", len(nulls), len(os.Cols))
	}

	ret := api.SQLFetch(os.h)
	if ret == api.SQL_NO_DATA {
		return 0, io.EOF
	}
	if IsError(ret) {
		return 0, NewError("SQLFetch", os.h)
	}

	n := 1
	if os.RowsFetched != nil {
		n = int(*os.RowsFetched)
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
		ensureCapInt64(d, n)
		for i := 0; i < n; i++ {
			isNull := bc.Lens[i].IsNull()
			if nullMask != nil {
				nullMask[i] = isNull
			}
			if isNull {
				(*d)[i] = 0
				continue
			}
			(*d)[i] = *(*int64)(unsafe.Pointer(&bc.Buffer[i*bc.Size]))
		}
	case *[]int32:
		ensureCapInt32(d, n)
		for i := 0; i < n; i++ {
			isNull := bc.Lens[i].IsNull()
			if nullMask != nil {
				nullMask[i] = isNull
			}
			if isNull {
				(*d)[i] = 0
				continue
			}
			(*d)[i] = *(*int32)(unsafe.Pointer(&bc.Buffer[i*bc.Size]))
		}
	case *[]float64:
		ensureCapFloat64(d, n)
		for i := 0; i < n; i++ {
			isNull := bc.Lens[i].IsNull()
			if nullMask != nil {
				nullMask[i] = isNull
			}
			if isNull {
				(*d)[i] = 0
				continue
			}
			(*d)[i] = *(*float64)(unsafe.Pointer(&bc.Buffer[i*bc.Size]))
		}
	case *[]bool:
		ensureCapBool(d, n)
		for i := 0; i < n; i++ {
			isNull := bc.Lens[i].IsNull()
			if nullMask != nil {
				nullMask[i] = isNull
			}
			if isNull {
				(*d)[i] = false
				continue
			}
			(*d)[i] = bc.Buffer[i*bc.Size] != 0
		}
	case *[]string:
		ensureCapString(d, n)
		switch bc.CType {
		case api.SQL_C_CHAR:
			for i := 0; i < n; i++ {
				isNull := bc.Lens[i].IsNull()
				if nullMask != nil {
					nullMask[i] = isNull
				}
				if isNull {
					(*d)[i] = ""
					continue
				}
				dataLen := int(bc.Lens[i])
				if dataLen > bc.Size {
					dataLen = bc.Size
				}
				start := i * bc.Size
				buf := bc.Buffer[start : start+dataLen]
				if bc.SType == api.SQL_DECIMAL || bc.SType == api.SQL_DECFLOAT {
					buf = bytes.Replace(buf, []byte(","), []byte("."), 1)
				}
				(*d)[i] = string(buf)
			}
		case api.SQL_C_WCHAR:
			for i := 0; i < n; i++ {
				isNull := bc.Lens[i].IsNull()
				if nullMask != nil {
					nullMask[i] = isNull
				}
				if isNull {
					(*d)[i] = ""
					continue
				}
				dataLen := int(bc.Lens[i])
				if dataLen > bc.Size {
					dataLen = bc.Size
				}
				if dataLen == 0 {
					(*d)[i] = ""
					continue
				}
				start := i * bc.Size
				raw := bc.Buffer[start : start+dataLen]
				s := (*[1 << 20]uint16)(unsafe.Pointer(&raw[0]))[: len(raw)/2 : len(raw)/2]
				(*d)[i] = string(utf16toutf8(s))
			}
		case api.SQL_C_DBCHAR:
			// DBCLOB: UTF-16BE encoded double-byte characters. Decode via
			// dbclobToUTF8 to produce a valid UTF-8 Go string.
			for i := 0; i < n; i++ {
				isNull := bc.Lens[i].IsNull()
				if nullMask != nil {
					nullMask[i] = isNull
				}
				if isNull {
					(*d)[i] = ""
					continue
				}
				dataLen := int(bc.Lens[i])
				if dataLen > bc.Size {
					dataLen = bc.Size
				}
				if dataLen == 0 {
					(*d)[i] = ""
					continue
				}
				start := i * bc.Size
				(*d)[i] = string(dbclobToUTF8(bc.Buffer[start : start+dataLen]))
			}
		default:
			for i := 0; i < n; i++ {
				isNull := bc.Lens[i].IsNull()
				if nullMask != nil {
					nullMask[i] = isNull
				}
				if isNull {
					(*d)[i] = ""
					continue
				}
				dataLen := int(bc.Lens[i])
				if dataLen > bc.Size {
					dataLen = bc.Size
				}
				start := i * bc.Size
				(*d)[i] = string(bc.Buffer[start : start+dataLen])
			}
		}
	case *[][]byte:
		ensureCapBytes(d, n)
		for i := 0; i < n; i++ {
			isNull := bc.Lens[i].IsNull()
			if nullMask != nil {
				nullMask[i] = isNull
			}
			if isNull {
				(*d)[i] = nil
				continue
			}
			dataLen := int(bc.Lens[i])
			if dataLen > bc.Size {
				dataLen = bc.Size
			}
			start := i * bc.Size
			cp := make([]byte, dataLen)
			copy(cp, bc.Buffer[start:start+dataLen])
			if bc.SType == api.SQL_DECIMAL || bc.SType == api.SQL_DECFLOAT {
				// Locale comma decimal separator -> dot. cp is freshly
				// allocated and owned here, so replace in place instead of
				// bytes.Replace (which always allocates a second copy). A DB2
				// DECIMAL/DECFLOAT has at most one separator, so stop at first.
				for k := 0; k < dataLen; k++ {
					if cp[k] == ',' {
						cp[k] = '.'
						break
					}
				}
			}
			(*d)[i] = cp
		}
	case *[]time.Time:
		ensureCapTime(d, n)
		for i := 0; i < n; i++ {
			isNull := bc.Lens[i].IsNull()
			if nullMask != nil {
				nullMask[i] = isNull
			}
			if isNull {
				(*d)[i] = time.Time{}
				continue
			}
			start := i * bc.Size
			p := unsafe.Pointer(&bc.Buffer[start])
			switch bc.CType {
			case api.SQL_C_TYPE_TIMESTAMP:
				t := (*api.SQL_TIMESTAMP_STRUCT)(p)
				(*d)[i] = time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
					int(t.Hour), int(t.Minute), int(t.Second), int(t.Fraction), time.UTC)
			case api.SQL_C_TYPE_TIMESTAMP_EXT_TZ:
				t := (*api.SQL_TIMESTAMP_STRUCT_EXT_TZ)(p)
				offset := int(t.TimezoneHour)*3600 + int(t.TimezoneMinute)*60
				(*d)[i] = time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
					int(t.Hour), int(t.Minute), int(t.Second), int(t.Fraction),
					time.FixedZone("", offset))
			case api.SQL_C_TYPE_DATE:
				t := (*api.SQL_DATE_STRUCT)(p)
				(*d)[i] = time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
					0, 0, 0, 0, time.Local)
			case api.SQL_C_TYPE_TIME:
				t := (*api.SQL_TIME_STRUCT)(p)
				(*d)[i] = time.Date(1, 1, 1, int(t.Hour), int(t.Minute), int(t.Second), 0, time.Local)
			default:
				(*d)[i] = time.Time{}
			}
		}
	default:
		return fmt.Errorf("ReadBatch: unsupported destination type %T for column %d", dst, colIdx)
	}
	return nil
}

// fillNonBindableSlice fills n rows from a non-bindable (LOB/XML) column using
// SQLGetData row by row. This is slower but correct for unbounded-length types.
func fillNonBindableSlice(os *ODBCStmt, colIdx int, col Column, dst interface{}, n int, nullMask []bool) error {
	switch d := dst.(type) {
	case *[]string:
		ensureCapString(d, n)
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
				(*d)[i] = ""
				continue
			}
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("ReadBatch: non-bindable column %d: expected string, got %T", colIdx, v)
			}
			(*d)[i] = s
		}
	case *[][]byte:
		ensureCapBytes(d, n)
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
				(*d)[i] = nil
				continue
			}
			b, ok := v.([]byte)
			if !ok {
				return fmt.Errorf("ReadBatch: non-bindable column %d: expected []byte, got %T", colIdx, v)
			}
			(*d)[i] = b
		}
	default:
		return fmt.Errorf("ReadBatch: non-bindable column %d requires *[]string or *[][]byte destination, got %T", colIdx, dst)
	}
	return nil
}

// ensureCap* helpers grow the slice to length n if needed.
func ensureCapInt64(s *[]int64, n int) {
	if len(*s) < n {
		*s = make([]int64, n)
	}
}
func ensureCapInt32(s *[]int32, n int) {
	if len(*s) < n {
		*s = make([]int32, n)
	}
}
func ensureCapFloat64(s *[]float64, n int) {
	if len(*s) < n {
		*s = make([]float64, n)
	}
}
func ensureCapBool(s *[]bool, n int) {
	if len(*s) < n {
		*s = make([]bool, n)
	}
}
func ensureCapString(s *[]string, n int) {
	if len(*s) < n {
		*s = make([]string, n)
	}
}
func ensureCapBytes(s *[][]byte, n int) {
	if len(*s) < n {
		*s = make([][]byte, n)
	}
}
func ensureCapTime(s *[]time.Time, n int) {
	if len(*s) < n {
		*s = make([]time.Time, n)
	}
}
