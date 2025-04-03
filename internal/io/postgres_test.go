package io

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"etl-tool/internal/config"
	"etl-tool/internal/util" 
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Test PostgresReader ---

// TestNewPostgresReader validates the reader constructor.
func TestNewPostgresReader(t *testing.T) {
	connStr := "postgres://user:pass@host:5432/db"
	query := "SELECT id, name FROM users"
	reader := NewPostgresReader(connStr, query)

	if reader == nil {
		t.Fatal("NewPostgresReader returned nil")
	}
	if reader.connStr != connStr {
		t.Errorf("reader.connStr = %q, want %q", reader.connStr, connStr)
	}
	if reader.query != query {
		t.Errorf("reader.query = %q, want %q", reader.query, query)
	}
}

// NOTE: Unit testing PostgresReader.Read success paths is omitted due to
// the internal direct call to pgx.Connect making mocking difficult without DI.
// Connection errors can still be tested by overriding pgxConnectFunc if needed,
// but query/row processing requires integration tests or refactoring.
// var pgxConnectFunc = pgx.Connect // Keep if testing connection errors needed

// --- Test PostgresWriter ---

// TestNewPostgresWriter validates the writer constructor.
func TestNewPostgresWriter(t *testing.T) {
	connStr := "pg://writer"
	table := "dest_table"
	loader := &config.LoaderConfig{Mode: "sql", Command: "INSERT"}
	writer := NewPostgresWriter(connStr, table, loader)

	if writer == nil {
		t.Fatal("NewPostgresWriter returned nil")
	}
	if writer.connStr != connStr {
		t.Errorf("writer.connStr = %q, want %q", writer.connStr, connStr)
	}
	if writer.targetTable != table {
		t.Errorf("writer.targetTable = %q, want %q", writer.targetTable, table)
	}
	if !reflect.DeepEqual(writer.loaderCfg, loader) {
		t.Errorf("writer.loaderCfg = %v, want %v", writer.loaderCfg, loader)
	}
}

// --- Testing PostgresWriter.Write Edge Cases ---

// pgxPoolNewFunc allows overriding pgxpool.New for specific tests (like pool creation failure).
// This variable is now defined in postgres.go, test just overrides it.

func TestPostgresWriter_Write_EdgeCases(t *testing.T) {
	// Setup necessary variables
	os.Setenv("PGWRITE_EDGE_DB", "edge_db")
	t.Cleanup(func() { os.Unsetenv("PGWRITE_EDGE_DB") })
	connStr := "postgres://test:test@localhost:5432/$PGWRITE_EDGE_DB"
	tableName := "public.edge_table"
	minimalRecords := []map[string]interface{}{{"id": 1}} // Non-empty records for error test

	t.Run("Write Empty Records", func(t *testing.T) {
		writer := NewPostgresWriter(connStr, tableName, nil)
		err := writer.Write([]map[string]interface{}{}, "") // Empty slice
		if err != nil {
			t.Fatalf("Write() with empty records failed unexpectedly: %v", err)
		}
		err = writer.Write(nil, "") // Nil slice
		if err != nil {
			t.Fatalf("Write() with nil records failed unexpectedly: %v", err)
		}
	})

	t.Run("Pool Creation Error", func(t *testing.T) {
		// Override pgxPoolNewFunc (defined in postgres.go) to simulate failure
		poolErr := errors.New("mock pool creation failure")
		originalNewPool := pgxPoolNewFunc
		pgxPoolNewFunc = func(ctx context.Context, connString string) (*pgxpool.Pool, error) {
			expectedExpanded := "postgres://test:test@localhost:5432/edge_db"
			if connString != expectedExpanded {
				return nil, fmt.Errorf("Pool Creation Mock: Unexpected conn string. Got %q, want %q", connString, expectedExpanded)
			}
			return nil, poolErr // Simulate pool creation failure
		}
		t.Cleanup(func() { pgxPoolNewFunc = originalNewPool }) // Restore original

		writer := NewPostgresWriter(connStr, tableName, nil)
		err := writer.Write(minimalRecords, "") // Use non-empty records to trigger pool creation

		if err == nil {
			t.Fatalf("Write() expected an error for pool creation failure, got nil")
		}
		// Check if the error message indicates pool creation failure and wraps the original error
		maskedConnStr := util.MaskCredentials("postgres://test:test@localhost:5432/edge_db")
		// Construct the expected beginning of the error message
		expectedErrMsgPrefix := fmt.Sprintf("PostgresWriter failed to create connection pool (using %s)", maskedConnStr)

		// Check if the error message starts correctly AND wraps the expected underlying error
		if !strings.HasPrefix(err.Error(), expectedErrMsgPrefix) || !errors.Is(err, poolErr) {
			t.Errorf("Write() error message mismatch:\ngot:             %q\nwant prefix:   %q\nand wrap error: '%v'", err, expectedErrMsgPrefix, poolErr)
		}
	})

	// NOTE: Unit tests for success paths (COPY, SQL, helpers) are omitted.
	// RECOMMENDATION: Use integration tests or refactor for dependency injection.
}

// TestPostgresWriter_Close confirms Close is a no-op.
func TestPostgresWriter_Close(t *testing.T) {
	writer := NewPostgresWriter("pg://close", "tbl", nil)
	err := writer.Close() // Should be no-op
	if err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}
}
