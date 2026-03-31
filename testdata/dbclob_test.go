package main

import (
	"fmt"
	"testing"
)

func TestDBClobDoubleByteChars(t *testing.T) {
	if DBClobDoubleByteChars() != nil {
		t.Error("Error at DBClobDoubleByteChars")
	}
}

// TestDBClobDoubleByteChars verifies that double-byte (multi-byte) characters
// stored in DBCLOB and VARGRAPHIC columns are retrieved correctly.
// This is a regression test for https://github.com/ibmdb/go_ibm_db/issues/274
func DBClobDoubleByteChars() error {
	db := Createconnection()
	defer db.Close()

	db.Exec("DROP TABLE dbclob_test")
	_, err := db.Exec("CREATE TABLE dbclob_test (id INTEGER, vargraphic_col VARGRAPHIC(100), dbclob_col DBCLOB)")
	if err != nil {
		fmt.Println("Exec error: ", err)
		return err
	}
	defer db.Exec("DROP TABLE dbclob_test")

	testval := "Hello世界"
	_, err = db.Exec(fmt.Sprintf("INSERT INTO dbclob_test VALUES (1, G'%s', G'%s')", testval, testval))
	if err != nil {
		fmt.Println("Insert error: ", err)
		return err
	}

	var vargraphic, dbclob string
	err = db.QueryRow("SELECT vargraphic_col, dbclob_col FROM dbclob_test WHERE id = 1").Scan(&vargraphic, &dbclob)
	if err != nil {
		fmt.Println("Select error: ", err)
		return err
	}

	if vargraphic != testval {
		return fmt.Errorf("VARGRAPHIC mismatch: expected %q, got %q", testval, vargraphic)
	}

	if dbclob != testval {
		return fmt.Errorf("DBCLOB mismatch: expected %q, got %q", testval, dbclob)
	}

	fmt.Println("DBCLOB double-byte character test passed")
	return nil
}
