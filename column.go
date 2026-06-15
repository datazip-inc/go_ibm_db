// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package go_ibm_db

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/ibmdb/go_ibm_db/api"
	trc "github.com/ibmdb/go_ibm_db/log2"
)

type BufferLen api.SQLLEN

func (l *BufferLen) IsNull() bool {
	trc.Trace1("column.go: IsNull()")

	return int16(*l) == api.SQL_NULL_DATA
}

func (l *BufferLen) GetData(h api.SQLHSTMT, idx int, ctype api.SQLSMALLINT, buf []byte) api.SQLRETURN {
	trc.Trace1("column.go: GetData()")

	return api.SQLGetData(h, api.SQLUSMALLINT(idx+1), ctype,
		api.SQLPOINTER(unsafe.Pointer(&buf[0])), api.SQLLEN(len(buf)),
		(*api.SQLLEN)(l))
}

// Column provides access to row columns.
type Column interface {
	Name() string
	TypeScan() reflect.Type
	// fetchSize is the number of rows per SQLFetch call (rowset size).
	// For array fetch, Bind allocates fetchSize × elementSize bytes and
	// fetchSize length indicators so the driver can fill all rows at once.
	Bind(h api.SQLHSTMT, idx int, fetchSize int) (bool, error)
	Value(h api.SQLHSTMT, idx int, rowIdx int) (driver.Value, error)
}

func describeColumn(h api.SQLHSTMT, idx int, namebuf []uint16) (namelen int, sqltype api.SQLSMALLINT, size api.SQLULEN, ret api.SQLRETURN) {
	trc.Trace1("column.go: describeColumn() - ENTRY")

	var l, decimal, nullable api.SQLSMALLINT
	ret = api.SQLDescribeCol(h, api.SQLUSMALLINT(idx+1),
		(*api.SQLWCHAR)(unsafe.Pointer(&namebuf[0])),
		api.SQLSMALLINT(len(namebuf)), &l,
		&sqltype, &size, &decimal, &nullable)

	trc.Trace1("column.go: describeColumn() - EXIT")
	return int(l), sqltype, size, ret
}

func NewColumn(h api.SQLHSTMT, idx int) (Column, error) {
	trc.Trace1("column.go: NewColumn() - ENTRY")

	namebuf := make([]uint16, 150)
	namelen, sqltype, size, ret := describeColumn(h, idx, namebuf)
	if ret == api.SQL_SUCCESS_WITH_INFO && namelen > len(namebuf) {
		// try again with bigger buffer
		namebuf = make([]uint16, namelen)
		namelen, sqltype, size, ret = describeColumn(h, idx, namebuf)
	}
	if IsError(ret) {
		return nil, NewError("SQLDescribeCol", h)
	}
	if namelen > len(namebuf) {
		// still complaining about buffer size
		return nil, errors.New("failed to allocate column name buffer")
	}
	b := &BaseColumn{
		name:  api.UTF16ToString(namebuf[:namelen]),
		SType: sqltype,
	}

	trc.Trace1("column.go: NewColumn() - EXIT")

	switch sqltype {
	case api.SQL_BIT, api.SQL_BOOLEAN:
		return NewBindableColumn(b, api.SQL_C_BIT, 1), nil
	case api.SQL_TINYINT, api.SQL_SMALLINT, api.SQL_INTEGER:
		return NewBindableColumn(b, api.SQL_C_LONG, 4), nil
	case api.SQL_BIGINT:
		return NewBindableColumn(b, api.SQL_C_SBIGINT, 8), nil
	case api.SQL_NUMERIC, api.SQL_FLOAT, api.SQL_REAL, api.SQL_DOUBLE:
		return NewBindableColumn(b, api.SQL_C_DOUBLE, 8), nil
	case api.SQL_TYPE_TIMESTAMP:
		var v api.SQL_TIMESTAMP_STRUCT
		return NewBindableColumn(b, api.SQL_C_TYPE_TIMESTAMP, int(unsafe.Sizeof(v))), nil
	case api.SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
		var v api.SQL_TIMESTAMP_STRUCT_EXT_TZ
		return NewBindableColumn(b, api.SQL_C_TYPE_TIMESTAMP_EXT_TZ, int(unsafe.Sizeof(v))), nil
	case api.SQL_TYPE_DATE:
		var v api.SQL_DATE_STRUCT
		return NewBindableColumn(b, api.SQL_C_TYPE_DATE, int(unsafe.Sizeof(v))), nil
	case api.SQL_TYPE_TIME:
		var v api.SQL_TIME_STRUCT
		return NewBindableColumn(b, api.SQL_C_TYPE_TIME, int(unsafe.Sizeof(v))), nil
	case api.SQL_CHAR, api.SQL_VARCHAR, api.SQL_CLOB, api.SQL_DECFLOAT, api.SQL_DECIMAL:
		return NewVariableWidthColumn(b, api.SQL_C_CHAR, size), nil
	case api.SQL_WCHAR, api.SQL_WVARCHAR:
		return NewVariableWidthColumn(b, api.SQL_C_WCHAR, size), nil
	case api.SQL_BINARY, api.SQL_VARBINARY, api.SQL_BLOB:
		return NewVariableWidthColumn(b, api.SQL_C_BINARY, size), nil
	case api.SQL_LONGVARCHAR:
		return NewVariableWidthColumn(b, api.SQL_C_CHAR, size), nil
	case api.SQL_WLONGVARCHAR, api.SQL_SS_XML:
		return NewVariableWidthColumn(b, api.SQL_C_WCHAR, size), nil
	case api.SQL_LONGVARBINARY:
		return NewVariableWidthColumn(b, api.SQL_C_BINARY, 0), nil
	case api.SQL_DBCLOB:
		return NewVariableWidthColumn(b, api.SQL_C_DBCHAR, size), nil
	case api.SQL_XML:
		// XML has no bounded width. Binding a fixed 30 MB buffer per row would
		// allocate 30 MB * FetchSize per fetch (multiple GB) and risk OOM, so
		// fetch it row-by-row via SQLGetData (colWidth 0 -> NonBindableColumn),
		// which streams arbitrary-length values without a giant pre-allocation.
		return NewVariableWidthColumn(b, api.SQL_C_BINARY, 0), nil
	default:
		return nil, fmt.Errorf("unsupported column type %d", sqltype)
	}
}

// BaseColumn implements common column functionality.
type BaseColumn struct {
	name  string
	CType api.SQLSMALLINT
	SType api.SQLSMALLINT
}

func (c *BaseColumn) Name() string {
	return c.name
}

func (c *BaseColumn) TypeScan() reflect.Type {
	trc.Trace1("column.go: TypeScan()")

	//TODO(Akhil):This will return the golang type of a variable
	switch c.CType {
	case api.SQL_C_BIT:
		return reflect.TypeOf(false)
	case api.SQL_C_LONG:
		return reflect.TypeOf(int32(0))
	case api.SQL_C_SBIGINT:
		return reflect.TypeOf(int64(0))
	case api.SQL_C_DOUBLE:
		return reflect.TypeOf(float64(0.0))
	case api.SQL_C_CHAR, api.SQL_C_WCHAR, api.SQL_C_DBCHAR:
		// DECIMAL/DECFLOAT are fetched as SQL_C_CHAR text; Value() returns []byte, not float64.
		if c.SType == api.SQL_DECIMAL || c.SType == api.SQL_DECFLOAT {
			return reflect.TypeOf([]byte(nil))
		}
		return reflect.TypeOf(string(""))
	case api.SQL_C_TYPE_DATE, api.SQL_C_TYPE_TIME, api.SQL_C_TYPE_TIMESTAMP, api.SQL_C_TYPE_TIMESTAMP_EXT_TZ:
		return reflect.TypeOf(time.Time{})
	case api.SQL_C_BINARY:
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf(new(interface{}))
	}
}

func (c *BaseColumn) Value(buf []byte) (driver.Value, error) {
	trc.Trace1("column.go: Value()")

	var p unsafe.Pointer
	if len(buf) > 0 {
		p = unsafe.Pointer(&buf[0])
	}
	switch c.CType {
	case api.SQL_C_BIT:
		return buf[0] != 0, nil
	case api.SQL_C_LONG:
		return *((*int32)(p)), nil
	case api.SQL_C_SBIGINT:
		return *((*int64)(p)), nil
	case api.SQL_C_DOUBLE:
		return *((*float64)(p)), nil
	case api.SQL_C_CHAR:
		if c.SType == api.SQL_DECIMAL || c.SType == api.SQL_DECFLOAT {
			return bytes.Replace(buf, []byte(","), []byte("."), 1), nil
		}
		return buf, nil
	case api.SQL_C_WCHAR:
		if p == nil {
			return nil, nil
		}
		s := (*[1 << 20]uint16)(p)[:len(buf)/2]
		return utf16toutf8(s), nil
	case api.SQL_C_DBCHAR:
		if p == nil {
			return nil, nil
		}
		s := (*[1 << 20]uint8)(p)[:len(buf)]
		return dbclobToUTF8(s), nil
	case api.SQL_C_TYPE_TIMESTAMP:
		t := (*api.SQL_TIMESTAMP_STRUCT)(p)
		r := time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
			int(t.Hour), int(t.Minute), int(t.Second), int(t.Fraction),
			time.UTC)
		return r, nil
	case api.SQL_C_TYPE_TIMESTAMP_EXT_TZ:
		t := (*api.SQL_TIMESTAMP_STRUCT_EXT_TZ)(p)
		offset := int(t.TimezoneHour)*3600 + int(t.TimezoneMinute)*60
		r := time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
			int(t.Hour), int(t.Minute), int(t.Second), int(t.Fraction),
			time.FixedZone("", offset))
		return r, nil
	case api.SQL_C_TYPE_DATE:
		t := (*api.SQL_DATE_STRUCT)(p)
		r := time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
			0, 0, 0, 0, time.Local)
		return r, nil
	case api.SQL_C_TYPE_TIME:
		t := (*api.SQL_TIME_STRUCT)(p)
		r := time.Date(1, 1, 1,
			int(t.Hour),
			int(t.Minute),
			int(t.Second),
			0,
			time.Local)
		return r, nil
	case api.SQL_C_BINARY:
		return buf, nil
	}
	return nil, fmt.Errorf("unsupported column ctype %d", c.CType)
}

// BindableColumn allows access to columns that can have their buffers bound.
// When FetchSize > 1 (array fetch), Buffer holds FetchSize×Size bytes and
// Lens holds one length/indicator per row in the rowset. The driver fills
// all rows in one SQLFetch call; Value(h, idx, rowIdx) reads the correct slot.
type BindableColumn struct {
	*BaseColumn
	IsBound         bool
	IsVariableWidth bool
	Size            int
	FetchSize       int
	Lens            []BufferLen // one per row in the rowset
	Buffer          []byte      // FetchSize * Size bytes
	smallBuf        [8]byte     // inline buffer used for single-row unbound reads
}

func NewBindableColumn(b *BaseColumn, ctype api.SQLSMALLINT, bufSize int) *BindableColumn {
	trc.Trace1("column.go: NewBindableColumn() - ENTRY")
	trc.Trace1(fmt.Sprintf("bufSize = %d", bufSize))

	b.CType = ctype
	c := &BindableColumn{BaseColumn: b, Size: bufSize}
	// Allocate a single-row buffer for the unbound/pre-bind fallback path.
	if c.Size <= len(c.smallBuf) {
		c.Buffer = c.smallBuf[:c.Size]
	} else {
		c.Buffer = make([]byte, c.Size)
	}
	c.Lens = make([]BufferLen, 1)
	trc.Trace1("column.go: NewBindableColumn() - EXIT")
	return c
}

func NewVariableWidthColumn(b *BaseColumn, ctype api.SQLSMALLINT, colWidth api.SQLULEN) Column {
	trc.Trace1("column.go: NewVariableWidthColumn() - ENTRY")

	if colWidth == 0 {
		b.CType = ctype
		return &NonBindableColumn{b}
	}
	// TODO: large declared widths (e.g. CLOB(1M+)) allocate fetchSize×width bytes
	// per bound column and can OOM under block fetch; consider capping bind size
	// and falling back to NonBindableColumn for oversized LOBs.
	l := int(colWidth)
	switch ctype {
	case api.SQL_C_WCHAR, api.SQL_C_DBCHAR:
		l++    // room for null-termination character
		l *= 2 // wchars take 2 bytes each
	case api.SQL_C_CHAR:
		if b.SType == api.SQL_DECIMAL {
			l = l + 4 // adding 4 as decimal has '.' which takes 1 byte
		} else {
			l++    // room for null-termination character
			l *= 2 //chars take 2 bytes each
		}
	case api.SQL_C_BINARY:
		// nothing to do
	default:
		panic(fmt.Errorf("do not know how wide column of ctype %d is", ctype))
	}
	c := NewBindableColumn(b, ctype, l)
	c.IsVariableWidth = true
	trc.Trace1("column.go: NewVariableWidthColumn() - EXIT")
	return c
}

// Bind registers column buffers with the driver for array fetch.
// fetchSize is the rowset size; the driver writes fetchSize rows per SQLFetch.
func (c *BindableColumn) Bind(h api.SQLHSTMT, idx int, fetchSize int) (bool, error) {
	trc.Trace1("column.go: Bind() - ENTRY")
	trc.Trace1(fmt.Sprintf("idx = %d, fetchSize = %d", idx, fetchSize))

	if fetchSize <= 0 {
		fetchSize = 1
	}
	c.FetchSize = fetchSize

	// Allocate rowset-sized buffers.
	c.Lens = make([]BufferLen, fetchSize)
	if fetchSize == 1 && c.Size <= len(c.smallBuf) {
		c.Buffer = c.smallBuf[:c.Size]
	} else {
		c.Buffer = make([]byte, fetchSize*c.Size)
	}

	// Register the start of the whole array. The driver uses c.Size as the
	// per-row stride and writes into Lens[0..fetchSize-1] automatically.
	bufLen := api.SQLLEN(c.Size)
	ret := api.SQLBindCol(h, api.SQLUSMALLINT(idx+1), c.CType,
		c.Buffer, bufLen,
		(*api.SQLLEN)(&c.Lens[0]))
	if IsError(ret) {
		return false, NewError("SQLBindCol", h)
	}
	c.IsBound = true
	trc.Trace1("column.go: Bind() - EXIT")
	return true, nil
}

// Value returns the value for the given rowIdx within the current rowset.
// rowIdx must be 0 for single-row (FetchSize=1) fetches.
func (c *BindableColumn) Value(h api.SQLHSTMT, idx int, rowIdx int) (driver.Value, error) {
	trc.Trace1("column.go: Value() - ENTRY")
	trc.Trace1(fmt.Sprintf("idx = %d, rowIdx = %d", idx, rowIdx))

	if !c.IsBound {
		// Under block fetch, this column was never bound (it appears after a
		// NonBindableColumn that stopped the binding chain). The ODBC cursor
		// may have been advanced to an arbitrary row by a previous column's
		// SQLGetData / SQLSetPos calls, so we must always re-position before
		// calling SQLGetData — even for rowIdx 0.
		//
		// We use getDataChunked (same helper as NonBindableColumn) so that
		// values longer than c.Buffer are not silently truncated.
		ret := api.SQLSetPos(h, api.SQLSETPOSIROW(rowIdx+1), api.SQL_POSITION, api.SQL_LOCK_NO_CHANGE)
		if IsError(ret) {
			return nil, NewError("SQLSetPos", h)
		}
		total, err := getDataChunked(h, idx, c.CType)
		if err != nil {
			return nil, err
		}
		if total == nil {
			return nil, nil // SQL NULL
		}
		trc.Trace1("column.go: Value() - EXIT (unbound)")
		return c.BaseColumn.Value(total)
	}

	l := c.Lens[rowIdx]
	if l.IsNull() {
		return nil, nil
	}
	if !c.IsVariableWidth && int(l) != c.Size {
		panic(fmt.Errorf("wrong column #%d length %d returned, %d expected", idx, l, c.Size))
	}
	start := rowIdx * c.Size
	bufLen := int(l)
	if bufLen > c.Size {
		bufLen = c.Size
	}
	trc.Trace1("column.go: Value() - EXIT")
	return c.BaseColumn.Value(c.Buffer[start : start+bufLen])
}

// getDataChunked reads an arbitrary-length value for column idx from the
// current ODBC row using SQLGetData, handling SQL_SUCCESS_WITH_INFO (01004)
// by appending in chunks until the full value is accumulated.
//
// Returns (nil, nil) when the value is SQL NULL.
// Used by both NonBindableColumn and the BindableColumn unbound path so that
// both paths handle values larger than a single buffer without truncation.
func getDataChunked(h api.SQLHSTMT, idx int, ctype api.SQLSMALLINT) ([]byte, error) {
	var l BufferLen
	var total []byte
	b := make([]byte, 1024)
	for {
		ret := l.GetData(h, idx, ctype, b)
		switch ret {
		case api.SQL_SUCCESS:
			if l.IsNull() {
				return nil, nil
			}
			return append(total, b[:l]...), nil
		case api.SQL_SUCCESS_WITH_INFO:
			err := NewError("SQLGetData", h).(*Error)
			if len(err.Diag) > 0 && err.Diag[0].State != "01004" {
				return nil, err
			}
			i := len(b)
			switch ctype {
			case api.SQL_C_WCHAR, api.SQL_C_DBCHAR:
				i -= 2 // exclude wchar (2-byte) null terminator
			case api.SQL_C_CHAR:
				i-- // exclude single-byte null terminator
			}
			total = append(total, b[:i]...)
			if l != api.SQL_NO_TOTAL {
				// Driver told us the total remaining length; read it in one shot.
				n := int(l) // remaining bytes
				n -= i
				n += 2 // headroom for largest (wchar) null terminator
				if len(b) < n {
					b = make([]byte, n)
				}
			}
		default:
			return nil, NewError("SQLGetData", h)
		}
	}
}

// NonBindableColumn provide access to columns, that can't be bound.
// These are of character or binary type, and, usually, there is no
// limit for their width.
type NonBindableColumn struct {
	*BaseColumn
}

func (c *NonBindableColumn) Bind(h api.SQLHSTMT, idx int, fetchSize int) (bool, error) {
	return false, nil
}

func (c *NonBindableColumn) Value(h api.SQLHSTMT, idx int, rowIdx int) (driver.Value, error) {
	trc.Trace1("column.go: Value() - ENTRY")
	// ReadBatch processes columns in column-major order (all rows of col N, then
	// col N+1). A prior column's SQLGetData/SQLSetPos calls may leave the cursor
	// on an arbitrary row, so always reposition before SQLGetData — even rowIdx 0.
	ret := api.SQLSetPos(h, api.SQLSETPOSIROW(rowIdx+1), api.SQL_POSITION, api.SQL_LOCK_NO_CHANGE)
	if IsError(ret) {
		return nil, NewError("SQLSetPos", h)
	}
	total, err := getDataChunked(h, idx, c.CType)
	if err != nil {
		return nil, err
	}
	if total == nil {
		return nil, nil // SQL NULL
	}
	trc.Trace1("column.go: Value() - EXIT")
	return c.BaseColumn.Value(total)
}
