// --- START OF CORRECTED FILE internal/io/postgres.go ---
package io

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"etl-tool/internal/config"
	"etl-tool/internal/logging"
	"etl-tool/internal/util"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool" // Keep pgxpool import
)

// pgxPoolNewFunc allows overriding pgxpool.New for testing.
// Defined at package level for both production and test code access.
var pgxPoolNewFunc = pgxpool.New // Store original function

// Default database connection and query timeout
const defaultDbTimeout = 30 * time.Second

// PostgresReader implements the InputReader interface for PostgreSQL sources.
type PostgresReader struct {
	connStr string
	query   string
}

// NewPostgresReader creates a new PostgresReader instance.
func NewPostgresReader(connStr, query string) *PostgresReader {
	return &PostgresReader{
		connStr: connStr,
		query:   query,
	}
}

// pgxConnectFunc allows overriding pgx.Connect for testing.
// Defined at package level for both production and test code access.
// Note: Still difficult to use effectively for unit testing Read without DI.
var pgxConnectFunc = pgx.Connect

// Read executes the configured SQL query against the PostgreSQL database.
func (pr *PostgresReader) Read(_ string) ([]map[string]interface{}, error) {
	logging.Logf(logging.Debug, "PostgresReader reading data using query: %s", pr.query)
	ctx, cancel := context.WithTimeout(context.Background(), defaultDbTimeout*2)
	defer cancel()

	expandedConnStr := util.ExpandEnvUniversal(pr.connStr)
	// Use the overrideable connect function
	conn, err := pgxConnectFunc(ctx, expandedConnStr)
	if err != nil {
		maskedConnStr := util.MaskCredentials(expandedConnStr)
		// Log first, then format error
		logging.Logf(logging.Error, "PostgresReader failed to connect using connection string: %s", maskedConnStr)
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("PostgresReader database connection timed out: %w", ctx.Err())
		}
		// Wrap the underlying error for better context
		return nil, fmt.Errorf("PostgresReader failed to connect to database (using %s): %w", maskedConnStr, err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, pr.query)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("PostgresReader query execution timed out: %w", ctx.Err())
		}
		return nil, fmt.Errorf("PostgresReader failed to execute query '%s': %w", pr.query, err)
	}
	defer rows.Close()

	var records []map[string]interface{}
	fieldDescriptions := rows.FieldDescriptions()
	if len(fieldDescriptions) == 0 {
		logging.Logf(logging.Warning, "PostgresReader query '%s' returned no columns.", pr.query)
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("PostgresReader error after fetching zero field descriptions: %w", err)
		}
		// Initialize records even if no columns
		records = make([]map[string]interface{}, 0)
		return records, nil
	}

	// Initialize slice only if columns exist
	records = make([]map[string]interface{}, 0)

	for rows.Next() {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("PostgresReader database operation timed out or cancelled during row iteration: %w", ctx.Err())
		}

		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("PostgresReader failed to scan row values: %w", err)
		}

		recordMap := make(map[string]interface{}, len(fieldDescriptions))
		for i, fd := range fieldDescriptions {
			colName := string(fd.Name)
			recordMap[colName] = values[i]
		}
		records = append(records, recordMap)
	}

	if err := rows.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("PostgresReader database operation timed out or cancelled after row iteration: %w", ctx.Err())
		}
		return nil, fmt.Errorf("PostgresReader error during row iteration: %w", err)
	}

	if ctx.Err() != nil {
		return nil, fmt.Errorf("PostgresReader database operation timed out or cancelled after reading rows: %w", ctx.Err())
	}

	logging.Logf(logging.Info, "PostgresReader successfully loaded %d records from query", len(records))
	return records, nil
}

// --- PostgreSQL Writer ---

// PostgresWriter implements the OutputWriter interface for PostgreSQL destinations.
type PostgresWriter struct {
	connStr     string
	targetTable string
	loaderCfg   *config.LoaderConfig
}

// NewPostgresWriter creates a new PostgresWriter instance.
func NewPostgresWriter(connStr, targetTable string, loaderCfg *config.LoaderConfig) *PostgresWriter {
	return &PostgresWriter{
		connStr:     connStr,
		targetTable: targetTable,
		loaderCfg:   loaderCfg,
	}
}

// Write directs records to the appropriate PostgreSQL loading function (COPY or custom SQL).
// Database connections are managed within this method.
func (pw *PostgresWriter) Write(records []map[string]interface{}, _ string) error {
	if len(records) == 0 {
		logging.Logf(logging.Info, "PostgresWriter: No records to write to table '%s'. Skipping.", pw.targetTable)
		return nil
	}
	logging.Logf(logging.Debug, "PostgresWriter attempting to write %d records to table '%s'", len(records), pw.targetTable)

	ctx, cancel := context.WithTimeout(context.Background(), defaultDbTimeout*10) // Increased timeout slightly
	defer cancel()

	expandedConnStr := util.ExpandEnvUniversal(pw.connStr)
	// *** USE THE OVERRIDEABLE FUNCTION VARIABLE ***
	pool, err := pgxPoolNewFunc(ctx, expandedConnStr)
	// *** END CHANGE ***
	if err != nil {
		maskedConnStr := util.MaskCredentials(expandedConnStr)
		// Log first, then return wrapped error
		logging.Logf(logging.Error, "PostgresWriter failed to create connection pool: %s", maskedConnStr)
		return fmt.Errorf("PostgresWriter failed to create connection pool (using %s): %w", maskedConnStr, err)
	}
	defer pool.Close()

	useCustomSQL := pw.loaderCfg != nil && strings.ToLower(pw.loaderCfg.Mode) == config.LoaderModeSQL

	// Execute Preload SQL if configured
	if useCustomSQL && len(pw.loaderCfg.Preload) > 0 {
		if err := pw.executeSQLCommands(ctx, pool, pw.loaderCfg.Preload, "preload"); err != nil {
			return err // Return preload error immediately
		}
	}

	// Perform the main data load
	var loadErr error
	if useCustomSQL {
		logging.Logf(logging.Info, "Using custom SQL loader for table '%s'.", pw.targetTable)
		loadErr = pw.loadWithCustomSQL(ctx, pool, records)
	} else {
		logging.Logf(logging.Info, "Using default COPY FROM loader for table '%s'.", pw.targetTable)
		loadErr = pw.loadUsingCopy(ctx, pool, records)
	}

	// Check for load errors before proceeding to Postload
	if loadErr != nil {
		if errors.Is(loadErr, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("PostgresWriter data loading operation timed out: %w", loadErr)
		}
		// Log the error within the helper functions, just return it here
		return loadErr // Return the specific load error
	}

	// Execute Postload SQL if configured (only if load succeeded)
	if useCustomSQL && len(pw.loaderCfg.Postload) > 0 {
		if err := pw.executeSQLCommands(ctx, pool, pw.loaderCfg.Postload, "postload"); err != nil {
			return err // Return postload error immediately
		}
	}

	// Check context after all operations
	if ctx.Err() != nil {
		return fmt.Errorf("PostgresWriter database operation timed out or cancelled after completion: %w", ctx.Err())
	}

	logging.Logf(logging.Info, "PostgresWriter successfully processed writing %d records for table '%s'.", len(records), pw.targetTable)
	return nil
}

// executeSQLCommands executes preload/postload commands within a single transaction.
// Now expects pgxpool.Pool directly.
func (pw *PostgresWriter) executeSQLCommands(ctx context.Context, pool *pgxpool.Pool, commands []string, commandType string) error {
	if len(commands) == 0 {
		return nil // Nothing to execute
	}
	logging.Logf(logging.Debug, "PostgresWriter (%s): Starting transaction for %d commands.", commandType, len(commands))
	tx, err := pool.Begin(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("PostgresWriter (%s): timed out starting transaction: %w", commandType, ctx.Err())
		}
		return fmt.Errorf("PostgresWriter (%s): failed to begin transaction: %w", commandType, err)
	}
	committed := false
	// Use background context for rollback if main context is cancelled
	rollbackCtx := context.Background()
	defer func() {
		if !committed {
			// Add timeout to rollback context
			rbCtx, rbCancel := context.WithTimeout(rollbackCtx, 5*time.Second)
			defer rbCancel()
			if err := tx.Rollback(rbCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
				logging.Logf(logging.Error, "PostgresWriter (%s): Failed to rollback transaction: %v", commandType, err)
			} else if err == nil {
				logging.Logf(logging.Debug, "PostgresWriter (%s): Transaction rolled back successfully.", commandType)
			}
		}
	}()

	for i, cmd := range commands {
		logging.Logf(logging.Debug, "Executing %s command #%d: %s", commandType, i+1, cmd)
		if _, err := tx.Exec(ctx, cmd); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("PostgresWriter (%s): command #%d timed out: %w", commandType, i+1, ctx.Err())
			}
			// Rollback happens in defer, return the specific command error
			return fmt.Errorf("PostgresWriter (%s): command #%d failed ('%s'): %w", commandType, i+1, cmd, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("PostgresWriter (%s): timed out committing transaction: %w", commandType, ctx.Err())
		}
		// Commit failed, rollback already deferred
		return fmt.Errorf("PostgresWriter (%s): failed to commit transaction: %w", commandType, err)
	}
	committed = true // Mark as committed to prevent rollback
	logging.Logf(logging.Info, "PostgresWriter (%s): Successfully executed and committed %d commands.", commandType, len(commands))
	return nil
}

// loadUsingCopy loads records efficiently using the PostgreSQL COPY FROM command.
// Now expects pgxpool.Pool directly.
func (pw *PostgresWriter) loadUsingCopy(ctx context.Context, pool *pgxpool.Pool, records []map[string]interface{}) error {
	if len(records) == 0 { return nil }

	// Determine columns from the first record consistently
	var columns []string
	for k := range records[0] {
		columns = append(columns, k)
	}
	sort.Strings(columns) // Ensure consistent column order
	logging.Logf(logging.Debug, "PostgresWriter (COPY): Determined columns for table '%s': %v", pw.targetTable, columns)

	// Prepare data structure for CopyFromRows
	copyData := make([][]interface{}, len(records))
	for i, rec := range records {
		rowData := make([]interface{}, len(columns))
		for j, colName := range columns {
			rowData[j] = rec[colName] // Map data based on sorted column order
		}
		copyData[i] = rowData
	}

	tableName := pgx.Identifier{pw.targetTable}
	copyCount, err := pool.CopyFrom(ctx, tableName, columns, pgx.CopyFromRows(copyData))

	if err != nil {
		// Check for context error first
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("PostgresWriter (COPY): operation timed out for table '%s': %w", pw.targetTable, ctx.Err())
		}
		// Log detailed PgError if available
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			logging.Logf(logging.Error, "PostgresWriter (COPY) failed for table '%s'. PG Error Code: %s, Message: %s, Detail: %s", pw.targetTable, pgErr.Code, pgErr.Message, pgErr.Detail)
		} else {
			logging.Logf(logging.Error, "PostgresWriter (COPY) failed for table '%s'. Error: %v", pw.targetTable, err)
		}
		// Return wrapped error
		return fmt.Errorf("PostgresWriter (COPY) failed for table '%s': %w", pw.targetTable, err)
	}

	// Check if the number of rows copied matches expectations
	if copyCount != int64(len(records)) {
		logging.Logf(logging.Warning, "PostgresWriter (COPY): Expected to copy %d rows to table '%s', but driver reported %d rows copied.", len(records), pw.targetTable, copyCount)
		// Note: This is generally not treated as a fatal error by the driver itself.
	} else {
		logging.Logf(logging.Info, "PostgresWriter (COPY): Successfully inserted %d rows into table '%s'.", copyCount, pw.targetTable)
	}
	return nil
}

// loadWithCustomSQL loads records using configured SQL commands, supporting batching.
// Now expects pgxpool.Pool directly.
func (pw *PostgresWriter) loadWithCustomSQL(ctx context.Context, pool *pgxpool.Pool, records []map[string]interface{}) error {
	// Basic validation
	if pw.loaderCfg == nil || pw.loaderCfg.Command == "" {
		return fmt.Errorf("PostgresWriter (SQL): loader config or command is missing")
	}
	if len(records) == 0 { return nil }

	// Determine column order for parameters
	var columns []string
	for k := range records[0] {
		columns = append(columns, k)
	}
	sort.Strings(columns) // Ensure consistent parameter order
	logging.Logf(logging.Debug, "PostgresWriter (SQL): Determined parameter order for command: %v", columns)

	batchSize := pw.loaderCfg.BatchSize
	totalRecords := len(records)
	processedCount := 0 // Successfully committed records
	errorCount := 0     // Records in failed batches or individual transactions

	// --- Non-Batched Execution ---
	if batchSize <= 0 {
		// Log warning for inefficiency
		if totalRecords > 1000 {
			logging.Logf(logging.Warning, "PostgresWriter (SQL): Processing a large number of records (%d) in inefficient non-batched mode (BatchSize <= 0).", totalRecords)
		} else {
			logging.Logf(logging.Debug, "PostgresWriter (SQL): Processing %d records individually (non-batched).", totalRecords)
		}

		for i, rec := range records {
			// Check context before starting transaction
			if ctx.Err() != nil {
				return fmt.Errorf("PostgresWriter (SQL): operation timed out or cancelled before processing record %d: %w", i, ctx.Err())
			}

			// Start transaction for single record
			tx, err := pool.Begin(ctx)
			if err != nil {
				errorCount++
				logging.Logf(logging.Error, "PostgresWriter (SQL): Failed to begin transaction for record %d: %v. Skipping record.", i, err)
				if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("PostgresWriter (SQL): timed out starting transaction for record %d: %w", i, ctx.Err())
				}
				continue // Skip to next record
			}

			committed := false
			rollbackCtx := context.Background() // Use background for rollback
			defer func(tx pgx.Tx) {             // Capture the correct transaction
				if !committed {
					rbCtx, rbCancel := context.WithTimeout(rollbackCtx, 5*time.Second)
					defer rbCancel()
					if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
						logging.Logf(logging.Error, "PostgresWriter (SQL): Failed to rollback record %d transaction: %v", i, rbErr)
					}
				}
			}(tx)

			// Prepare parameters based on sorted column order
			params := make([]interface{}, len(columns))
			for j, colName := range columns {
				params[j] = rec[colName]
			}

			// Execute the command
			_, execErr := tx.Exec(ctx, pw.loaderCfg.Command, params...)
			if execErr != nil {
				errorCount++
				logging.Logf(logging.Error, "PostgresWriter (SQL): Failed executing command for record %d: %v. Rolling back. Record data (masked): %v", i, execErr, util.MaskSensitiveData(rec))
				if errors.Is(execErr, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("PostgresWriter (SQL): command execution timed out for record %d: %w", i, ctx.Err()) // Return timeout error
				}
				// Rollback happens in defer, continue to next record
				continue
			}

			// Commit the transaction
			if err := tx.Commit(ctx); err != nil {
				errorCount++
				logging.Logf(logging.Error, "PostgresWriter (SQL): Failed to commit transaction for record %d: %v. Record state uncertain.", i, err)
				if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("PostgresWriter (SQL): timed out committing transaction for record %d: %w", i, ctx.Err()) // Return timeout error
				}
				// Rollback happens in defer, continue to next record
				continue
			}
			committed = true // Mark commit success
			processedCount++
		} // End non-batched loop

	// --- Batched Execution ---
	} else {
		logging.Logf(logging.Debug, "PostgresWriter (SQL): Processing %d records in batches of size %d.", totalRecords, batchSize)
		for i := 0; i < totalRecords; i += batchSize {
			// Check context before starting batch transaction
			if ctx.Err() != nil {
				return fmt.Errorf("PostgresWriter (SQL): operation timed out or cancelled before processing batch starting at %d: %w", i, ctx.Err())
			}

			batchStart := i
			batchEnd := i + batchSize
			if batchEnd > totalRecords {
				batchEnd = totalRecords
			}
			currentBatchRecords := records[batchStart:batchEnd]
			currentBatchSize := len(currentBatchRecords)

			logging.Logf(logging.Debug, "Processing batch %d-%d (size %d)", batchStart, batchEnd-1, currentBatchSize)

			// Start transaction for the batch
			tx, err := pool.Begin(ctx)
			if err != nil {
				// Cannot proceed with this batch or subsequent ones if Begin fails
				if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("PostgresWriter (SQL): timed out starting transaction for batch %d-%d: %w", batchStart, batchEnd-1, ctx.Err())
				}
				return fmt.Errorf("PostgresWriter (SQL): failed to begin transaction for batch %d-%d: %w", batchStart, batchEnd-1, err)
			}

			committed := false
			rollbackCtx := context.Background()
			defer func(tx pgx.Tx, start, end int) { // Capture correct transaction and batch info
				if !committed {
					rbCtx, rbCancel := context.WithTimeout(rollbackCtx, 5*time.Second)
					defer rbCancel()
					if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
						logging.Logf(logging.Error, "PostgresWriter (SQL): Failed to rollback batch %d-%d transaction: %v", start, end-1, rbErr)
					}
				}
			}(tx, batchStart, batchEnd)

			// Queue commands for the batch
			batch := &pgx.Batch{}
			for _, rec := range currentBatchRecords {
				params := make([]interface{}, len(columns))
				for j, colName := range columns {
					params[j] = rec[colName]
				}
				batch.Queue(pw.loaderCfg.Command, params...)
			}

			// Send the batch
			br := tx.SendBatch(ctx, batch)

			// Check results for each command in the batch
			batchErrCount := 0
			var firstBatchErr error
			for k := 0; k < currentBatchSize; k++ {
				// Check context while processing results
				if ctx.Err() != nil && firstBatchErr == nil {
					firstBatchErr = fmt.Errorf("operation timed out or cancelled while processing results for batch %d-%d: %w", batchStart, batchEnd-1, ctx.Err())
					batchErrCount = currentBatchSize // Assume all failed if context cancelled
					break                          // Stop checking results for this batch
				}

				_, execErr := br.Exec() // Get result for the k-th queued command
				if execErr != nil {
					batchErrCount++
					// Record the first error encountered in the batch
					if firstBatchErr == nil {
						recordIndex := k + batchStart
						firstBatchErr = fmt.Errorf("command for record index %d (in batch %d-%d) failed: %w", recordIndex, batchStart, batchEnd-1, execErr)
					}
				}
			}

			// Close the batch results, check for errors during close
			closeErr := br.Close()
			if closeErr != nil && firstBatchErr == nil {
				firstBatchErr = fmt.Errorf("failed closing batch results reader for batch %d-%d: %w", batchStart, batchEnd-1, closeErr)
				// If batchErrCount was 0, increment it as Close error implies something went wrong
				if batchErrCount == 0 {
					batchErrCount = 1
				}
			}

			// If any error occurred during batch execution or closing results reader
			if firstBatchErr != nil {
				errorCount += currentBatchSize // Assume whole batch failed if any part failed
				logging.Logf(logging.Error, "PostgresWriter (SQL): Batch %d-%d failed with %d error(s), rolling back transaction. First error: %v", batchStart, batchEnd-1, batchErrCount, firstBatchErr)
				// Rollback happens in defer. Check if the error was a timeout.
				if errors.Is(firstBatchErr, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("PostgresWriter (SQL): batch %d-%d timed out: %w", batchStart, batchEnd-1, firstBatchErr) // Return timeout error
				}
				// For other batch errors, return the first specific error found
				return fmt.Errorf("PostgresWriter (SQL): batch %d-%d failed: %w", batchStart, batchEnd-1, firstBatchErr)
			}

			// If batch executed without errors, commit the transaction
			if err := tx.Commit(ctx); err != nil {
				errorCount += currentBatchSize // Assume whole batch failed if commit failed
				logging.Logf(logging.Error, "PostgresWriter (SQL): Failed to commit transaction for batch %d-%d: %v", batchStart, batchEnd-1, err)
				if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
					return fmt.Errorf("PostgresWriter (SQL): timed out committing transaction for batch %d-%d: %w", batchStart, batchEnd-1, ctx.Err()) // Return timeout error
				}
				// Rollback happens in defer. Return commit error.
				return fmt.Errorf("PostgresWriter (SQL): failed to commit transaction for batch %d-%d: %w", batchStart, batchEnd-1, err)
			}

			committed = true // Mark commit success
			processedCount += currentBatchSize
			logging.Logf(logging.Debug, "PostgresWriter (SQL): Successfully committed batch %d-%d.", batchStart, batchEnd-1)
		} // End batched loop
	}

	// Final summary logging
	if errorCount > 0 {
		logging.Logf(logging.Warning, "PostgresWriter (SQL): Completed processing for table '%s'. %d records processed successfully, %d records encountered errors (in failed transactions/batches).", pw.targetTable, processedCount, errorCount)
		// Optionally return a generic error indicating some records failed if required by caller
		// return fmt.Errorf("PostgresWriter (SQL): encountered errors processing %d records", errorCount)
	} else {
		logging.Logf(logging.Info, "PostgresWriter (SQL): Successfully executed commands for all %d records for table '%s'.", processedCount, pw.targetTable)
	}
	return nil // Return nil if execution completes, even if some non-batched records failed (logged above)
}

// Close implements the OutputWriter interface. For PostgresWriter, this is a no-op
// as database connections/pools are managed entirely within the Write method scope.
func (pw *PostgresWriter) Close() error {
	logging.Logf(logging.Debug, "PostgresWriter Close called (no-op).")
	return nil
}
