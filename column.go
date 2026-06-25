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
	// rowIdx is the 0-based index within the current rowset (0 for single-row fetch).
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
	case api.SQL_CHAR, api.SQL_VARCHAR, api.SQL_DECFLOAT, api.SQL_DECIMAL:
		return NewVariableWidthColumn(b, api.SQL_C_CHAR, size), nil
	case api.SQL_CLOB:
		// CLOB columns can have declared widths of 1 GB or more. Binding
		// fetchSize×width bytes per column would OOM under block fetch.
		return NewVariableWidthColumn(b, api.SQL_C_CHAR, 0), nil
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
		// DBCLOB (double-byte CLOB) can be arbitrarily large; force
		// NonBindableColumn to avoid fetchSize×width OOM
		return NewVariableWidthColumn(b, api.SQL_C_DBCHAR, 0), nil
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
		// DECIMAL/DECFLOAT come from DB2 as text (e.g. "45.234"), not as a binary float.
		// Report []byte so ColumnTypeScanType matches what Value/ReadBatch return.
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
		// Some locales send "45,234" (comma); Go expects "45.234" (dot) before parsing.
		if c.SType == api.SQL_DECIMAL || c.SType == api.SQL_DECFLOAT {
			return bytes.Replace(buf, []byte(","), []byte("."), 1), nil
		}
		return buf, nil
	case api.SQL_C_WCHAR:
		// p == nil means buf is empty: the DB value is an empty string, not NULL.
		// SQL NULL is handled by callers before BaseColumn.Value is ever reached
		// (getDataChunked returns nil for NULL; BindableColumn.Value checks LenBuffer).
		if p == nil {
			return "", nil
		}
		s := (*[1 << 20]uint16)(p)[:len(buf)/2]
		return utf16toutf8(s), nil
	case api.SQL_C_DBCHAR:
		if p == nil {
			return "", nil
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
		// DB2 DATE carries no timezone. We pin to UTC (not time.Local) so that
		// the value is machine-independent and consistent with how TIMESTAMP is
		// handled above. OLake's dataTypeConverter does not post-process dates,
		// so whatever location we choose here is what callers see.
		r := time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
			0, 0, 0, 0, time.UTC)
		return r, nil
	case api.SQL_C_TYPE_TIME:
		t := (*api.SQL_TIME_STRUCT)(p)
		// Same reasoning as DATE above: pin to UTC for consistency.
		r := time.Date(1, 1, 1,
			int(t.Hour),
			int(t.Minute),
			int(t.Second),
			0,
			time.UTC)
		return r, nil
	case api.SQL_C_BINARY:
		return buf, nil
	}
	return nil, fmt.Errorf("unsupported column ctype %d", c.CType)
}

// BindableColumn allows access to columns that can have their buffers bound.
// When FetchSize > 1 (array fetch), Buffer holds FetchSize×Size bytes and
// LenBuffer holds one length/indicator per row in the rowset. The driver fills
// all rows in one SQLFetch call; Value(h, idx, rowIdx) reads the correct slot.
type BindableColumn struct {
	*BaseColumn
	IsBound         bool
	IsVariableWidth bool
	Size            int
	LenBuffer       []BufferLen // one per row in the rowset
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
	// ODBC stores each fetched row's byte length (or a NULL marker) in LenBuffer[i],
	// alongside Buffer[i]. Pre-allocate one slot here so the slice is never nil;
	// Bind() reallocates to FetchSize and passes &LenBuffer[0] to SQLBindCol.
	c.LenBuffer = make([]BufferLen, 1)
	trc.Trace1("column.go: NewBindableColumn() - EXIT")
	return c
}

func NewVariableWidthColumn(b *BaseColumn, ctype api.SQLSMALLINT, colWidth api.SQLULEN) Column {
	trc.Trace1("column.go: NewVariableWidthColumn() - ENTRY")

	if colWidth == 0 {
		b.CType = ctype
		return &NonBindableColumn{b}
	}
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

	// Allocate rowset-sized buffers.
	c.LenBuffer = make([]BufferLen, fetchSize)
	if fetchSize == 1 && c.Size <= len(c.smallBuf) {
		c.Buffer = c.smallBuf[:c.Size]
	} else {
		c.Buffer = make([]byte, fetchSize*c.Size)
	}

	// Register the start of the whole array. The driver uses c.Size as the
	// per-row stride and writes into LenBuffer[0..fetchSize-1] automatically.
	bufLen := api.SQLLEN(c.Size)
	ret := api.SQLBindCol(h, api.SQLUSMALLINT(idx+1), c.CType,
		c.Buffer, bufLen,
		(*api.SQLLEN)(&c.LenBuffer[0]))
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
		// This column was never bound (it appears after a NonBindableColumn
		// that stopped the binding chain). BindColumns forces FetchSize=1 when
		// any non-bindable column is present, so rowIdx is always 0 and the
		// cursor is already on the correct row after SQLFetch.
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

	l := c.LenBuffer[rowIdx]
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
	var indicator BufferLen
	var total []byte

	buf := make([]byte, 1024) // start with 1 KB
	for {
		ret := indicator.GetData(h, idx, ctype, buf)
		switch ret {

		case api.SQL_SUCCESS:
			// The entire value (or the last chunk) fit in buf.
			// indicator holds the actual number of bytes written by the driver.
			if indicator.IsNull() {
				return nil, nil
			}
			if total == nil {
				// Common case: value fit on the very first call
				return buf[:indicator], nil
			}
			return append(total, buf[:indicator]...), nil

		case api.SQL_SUCCESS_WITH_INFO:
			// ODBC fills buf completely and returns SQL_SUCCESS_WITH_INFO when
			// the value is longer than buf. State "01004" (string data right-
			// truncated) is the expected signal to keep reading. Any other state
			// is an unexpected warning and should be treated as an error.
			if s := diagState(h); s != "01004" {
				return nil, NewError("SQLGetData", h)
			}

			// ODBC always appends a null terminator to the data it writes into
			// buf. Strip it before saving this chunk so it doesn't appear in
			// the middle of the assembled value.
			chunkLen := len(buf)
			switch ctype {
			case api.SQL_C_WCHAR, api.SQL_C_DBCHAR:
				chunkLen -= 2 // UTF-16 null terminator is 2 bytes
			case api.SQL_C_CHAR:
				chunkLen-- // single-byte null terminator
				// SQL_C_BINARY has no null terminator; chunkLen stays at len(buf)
			}
			total = append(total, buf[:chunkLen]...)

			if indicator != api.SQL_NO_TOTAL {
				// Driver reported how many bytes remain after this chunk.
				// Resize buf to read the rest in one shot, avoiding more loop iterations.
				// +2 gives headroom for the widest possible null terminator (wchar).
				nextBufSize := int(indicator) - chunkLen + 2
				if len(buf) < nextBufSize {
					buf = make([]byte, nextBufSize)
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
	// BindColumns forces FetchSize=1 when any non-bindable column is present,
	// so rowIdx is always 0 and the cursor is already on the correct row after
	// SQLFetch.
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
