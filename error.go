// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package go_ibm_db

import (
	"database/sql/driver"
	"fmt"
	"runtime"
	"strings"
	"unsafe"

	"github.com/ibmdb/go_ibm_db/api"
	trc "github.com/ibmdb/go_ibm_db/log2"
)

func IsError(ret api.SQLRETURN) bool {
	trc.Trace1("error.go: IsError() - ENTRY")
	switch ret {
	case api.SQL_SUCCESS:
		trc.Trace1("api.SQL_SUCCESS")
	case api.SQL_SUCCESS_WITH_INFO:
		trc.Trace1("api.SQL_SUCCESS_WITH_INFO")
	}
	trc.Trace1("error.go: IsError() - EXIT")
	return !(ret == api.SQL_SUCCESS || ret == api.SQL_SUCCESS_WITH_INFO)
}

type DiagRecord struct {
	State       string
	NativeError int
	Message     string
}

func (r *DiagRecord) String() string {
	return fmt.Sprintf("{%s} %s", r.State, r.Message)
}

type Error struct {
	APIName string
	Diag    []DiagRecord
}

func (e *Error) Error() string {
	trc.Trace1("error.go: Error() - ENTRY")
	ss := make([]string, len(e.Diag))
	for i, r := range e.Diag {
		ss[i] = r.String()
	}
	trc.Trace1(fmt.Sprintf("%s : %s", e.APIName, ss))
	trc.Trace1("error.go: Error() - EXIT")
	return e.APIName + ": " + strings.Join(ss, "\n")
}

func NewError(apiName string, handle interface{}) error {
	trc.Trace1("error.go: NewError() - ENTRY")
	trc.Trace1(fmt.Sprintf("apiName=%s", apiName))

	var ret api.SQLRETURN
	h, ht := ToHandleAndType(handle)
	err := &Error{APIName: apiName}
	var ne api.SQLINTEGER
	state := make([]uint16, 6)
	msg := make([]uint16, api.SQL_MAX_MESSAGE_LENGTH)
	for i := 1; ; i++ {
		if runtime.GOOS == "zos" {
			ret = api.SQLGetDiagRec(ht, h, api.SQLSMALLINT(i),
				(*api.SQLWCHAR)(unsafe.Pointer(&state[0])), &ne,
				(*api.SQLWCHAR)(unsafe.Pointer(&msg[0])),
				api.SQLSMALLINT(2*len(msg)), nil) // odbc api on zos doesn't handle null terminated strings, the exact size is passed
		} else {
			ret = api.SQLGetDiagRec(ht, h, api.SQLSMALLINT(i),
				(*api.SQLWCHAR)(unsafe.Pointer(&state[0])), &ne,
				(*api.SQLWCHAR)(unsafe.Pointer(&msg[0])),
				api.SQLSMALLINT(len(msg)), nil)
		}
		if ret == api.SQL_NO_DATA {
			break
		}
		if IsError(ret) {
			trc.Trace1(fmt.Sprintf("SQLGetDiagRec failed: ret=%d", ret))
			panic(fmt.Errorf("SQLGetDiagRec failed: ret=%d", ret))
		}
		r := DiagRecord{
			State:       api.UTF16ToString(state),
			NativeError: int(ne),
			Message:     api.UTF16ToString(msg),
		}
		if strings.Contains(r.Message, "CLI0106E") ||
			strings.Contains(r.Message, "CLI0107E") ||
			strings.Contains(r.Message, "CLI0108E") {
			return driver.ErrBadConn
		}
		err.Diag = append(err.Diag, r)
	}
	trc.Trace1(fmt.Sprintf("Error: %s", err))
	trc.Trace1("error.go: NewError() - EXIT")
	return err
}

// diagState reads only the SQLSTATE of the first diagnostic record for a
// statement handle. It avoids allocating the full message buffer that NewError
// uses, making it cheap to call inside the chunked-read loop where we only
// need to distinguish state "01004" (data truncated, keep reading) from other
// warnings. Returns "" if there are no diagnostic records.
func diagState(h api.SQLHSTMT) string {
	var ne api.SQLINTEGER
	state := make([]uint16, 6) // 5-char SQLSTATE + null terminator
	var msg [2]uint16          // minimal buffer; we only need the state, not the message
	msgLen := api.SQLSMALLINT(len(msg))
	if runtime.GOOS == "zos" {
		// zos requires the exact byte count rather than element count
		msgLen = api.SQLSMALLINT(2 * len(msg))
	}
	ret := api.SQLGetDiagRec(
		api.SQL_HANDLE_STMT, api.SQLHANDLE(h), 1,
		(*api.SQLWCHAR)(unsafe.Pointer(&state[0])), &ne,
		(*api.SQLWCHAR)(unsafe.Pointer(&msg[0])),
		msgLen, nil,
	)
	if ret == api.SQL_NO_DATA || IsError(ret) {
		return ""
	}
	return api.UTF16ToString(state)
}
