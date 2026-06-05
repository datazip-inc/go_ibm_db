package main

import (
	"fmt"
	"strings"
	"testing"
)

// TestZosUTF16Boundary tests issue #278: SQLExecDirect/SQLPrepare passed SQL_NTS
// to non-null-terminated UTF-16 buffer, causing intermittent SQLCODE -104/-199 on z/OS.
// This test creates queries of various lengths to test boundary conditions.
func TestZosUTF16Boundary(t *testing.T) {
	if ZosUTF16Boundary() != nil {
		t.Error("Error in ZosUTF16Boundary test")
	}
}

// TestZosUTF16BoundaryPrepare tests the same issue but for prepared statements.
func TestZosUTF16BoundaryPrepare(t *testing.T) {
	if ZosUTF16BoundaryPrepare() != nil {
		t.Error("Error in ZosUTF16BoundaryPrepare test")
	}
}

// ZosUTF16Boundary tests Query (SQLExecDirect) with various query lengths
// that might hit memory allocation boundaries (e.g., 2048 bytes = 1024 UTF-16 code units).
func ZosUTF16Boundary() error {
	db := Createconnection()
	defer db.Close()

	// Test various query lengths including boundary conditions
	// UTF-16 on z/OS uses 2 bytes per character for BMP characters
	// Memory allocator size classes often include: 512, 1024, 2048, 4096 bytes
	// These translate to: 256, 512, 1024, 2048 UTF-16 code units

	testLengths := []int{
		255,  // Just under 512 bytes
		256,  // Exactly 512 bytes
		257,  // Just over 512 bytes
		511,  // Just under 1024 bytes
		512,  // Exactly 1024 bytes
		513,  // Just over 1024 bytes
		1023, // Just under 2048 bytes
		1024, // Exactly 2048 bytes - the boundary mentioned in issue #278
		1025, // Just over 2048 bytes
		2047, // Just under 4096 bytes
		2048, // Exactly 4096 bytes
		2049, // Just over 4096 bytes
	}

	tableName := "zos_boundary_test"
	db.Exec("DROP TABLE " + tableName)
	_, err := db.Exec("CREATE TABLE " + tableName + " (id INT, data VARCHAR(100))")
	if err != nil {
		fmt.Println("Failed to create test table:", err)
		return err
	}
	defer db.Exec("DROP TABLE " + tableName)

	_, err = db.Exec("INSERT INTO " + tableName + " VALUES (1, 'test')")
	if err != nil {
		fmt.Println("Failed to insert test data:", err)
		return err
	}

	for _, targetLen := range testLengths {
		// Create a query with approximately the target UTF-16 length
		// Base query: "SELECT * FROM zos_boundary_test WHERE id = 1"
		baseQuery := "SELECT * FROM " + tableName + " WHERE id = 1"
		baseLen := len(baseQuery)

		// Add padding with SQL comment to reach target length
		if targetLen > baseLen {
			padding := strings.Repeat(" ", targetLen-baseLen-4) // -4 for "/*" and "*/"
			if len(padding) > 0 {
				baseQuery = baseQuery + " /*" + padding + "*/"
			}
		}

		// Run the query multiple times to catch intermittent issues
		// (the original issue was intermittent due to memory allocation patterns)
		for i := 0; i < 10; i++ {
			rows, err := db.Query(baseQuery)
			if err != nil {
				fmt.Printf("Query failed at length %d, iteration %d: %v\n", targetLen, i, err)
				return err
			}

			hasRows := false
			for rows.Next() {
				hasRows = true
				var id int
				var data string
				if err := rows.Scan(&id, &data); err != nil {
					rows.Close()
					fmt.Printf("Scan failed at length %d, iteration %d: %v\n", targetLen, i, err)
					return err
				}
			}
			rows.Close()

			if err := rows.Err(); err != nil {
				fmt.Printf("Rows error at length %d, iteration %d: %v\n", targetLen, i, err)
				return err
			}

			if !hasRows {
				fmt.Printf("No rows returned at length %d, iteration %d\n", targetLen, i)
				return fmt.Errorf("expected rows but got none")
			}
		}
		fmt.Printf("Query length %d: OK (10 iterations)\n", targetLen)
	}

	fmt.Println("ZosUTF16Boundary test passed")
	return nil
}

// ZosUTF16BoundaryPrepare tests Prepare (SQLPrepare) with various query lengths
func ZosUTF16BoundaryPrepare() error {
	db := Createconnection()
	defer db.Close()

	testLengths := []int{
		255, 256, 257,
		511, 512, 513,
		1023, 1024, 1025,
		2047, 2048, 2049,
	}

	tableName := "zos_boundary_prep_test"
	db.Exec("DROP TABLE " + tableName)
	_, err := db.Exec("CREATE TABLE " + tableName + " (id INT, data VARCHAR(100))")
	if err != nil {
		fmt.Println("Failed to create test table:", err)
		return err
	}
	defer db.Exec("DROP TABLE " + tableName)

	_, err = db.Exec("INSERT INTO " + tableName + " VALUES (1, 'test')")
	if err != nil {
		fmt.Println("Failed to insert test data:", err)
		return err
	}

	for _, targetLen := range testLengths {
		baseQuery := "SELECT * FROM " + tableName + " WHERE id = ?"
		baseLen := len(baseQuery)

		if targetLen > baseLen {
			padding := strings.Repeat(" ", targetLen-baseLen-4)
			if len(padding) > 0 {
				baseQuery = baseQuery + " /*" + padding + "*/"
			}
		}

		// Test prepared statement multiple times
		for i := 0; i < 10; i++ {
			stmt, err := db.Prepare(baseQuery)
			if err != nil {
				fmt.Printf("Prepare failed at length %d, iteration %d: %v\n", targetLen, i, err)
				return err
			}

			rows, err := stmt.Query(1)
			if err != nil {
				stmt.Close()
				fmt.Printf("Prepared query failed at length %d, iteration %d: %v\n", targetLen, i, err)
				return err
			}

			hasRows := false
			for rows.Next() {
				hasRows = true
				var id int
				var data string
				if err := rows.Scan(&id, &data); err != nil {
					rows.Close()
					stmt.Close()
					fmt.Printf("Scan failed at length %d, iteration %d: %v\n", targetLen, i, err)
					return err
				}
			}
			rows.Close()
			stmt.Close()

			if err := rows.Err(); err != nil {
				fmt.Printf("Rows error at length %d, iteration %d: %v\n", targetLen, i, err)
				return err
			}

			if !hasRows {
				fmt.Printf("No rows returned at length %d, iteration %d\n", targetLen, i)
				return fmt.Errorf("expected rows but got none")
			}
		}
		fmt.Printf("Prepare length %d: OK (10 iterations)\n", targetLen)
	}

	fmt.Println("ZosUTF16BoundaryPrepare test passed")
	return nil
}
