package main

import (
	"errors"
	"fmt"
	"os" // Import 'os' for os.Stderr and os.Exit

	"etl-tool/internal/app"
	"etl-tool/internal/logging"
)

// main is the entry point for the etl-tool application.
// It initializes and runs the AppRunner.
func main() {
	// Create a new application runner instance.
	// Dependencies are managed within the app package.
	runner := app.NewAppRunner()

	// Execute the application logic using command-line arguments.
	// os.Args[1:] excludes the program name itself.
	err := runner.Run(os.Args[1:])
	if err != nil {
		// --- Refined Error Handling ---
		// Determine if usage should be printed to stderr *before* logging.
		printUsage := errors.Is(err, app.ErrUsage) || errors.Is(err, app.ErrConfigNotFound) || errors.Is(err, app.ErrMissingArgs)

		if printUsage {
			fmt.Fprintln(os.Stderr, "") // Add separation before usage.
			runner.Usage(os.Stderr)    // Print usage directly to stderr.
		}

		// Ensure logs capture the error before exiting.
		// If logging hasn't been fully initialized or level is too low for ERROR,
		// ensure this critical error is seen by temporarily setting the level.
		levelAdjusted := false
		if logging.GetLevel() < logging.Error {
			logging.SetLevel(logging.Error)
			levelAdjusted = true // Flag that level was temporarily changed
		}

		// Log the application error using the configured logger.
		// Removed the initial standard log.Printf to avoid duplicate logging.
		logging.Logf(logging.Error, "Application execution failed: %v", err)

		// Optional: Reset level if it was temporarily adjusted, though process is exiting.
		// This might be relevant if error handling becomes more complex later.
		if levelAdjusted {
			// Depending on desired behavior, could reset to a default like Info,
			// but usually exiting after error, so maybe not critical.
			// Example: logging.SetLevel(logging.Info)
		}
		// --- End Refined Error Handling ---

		os.Exit(1) // Exit with a non-zero code indicates failure.
	}

	// Log successful completion if necessary, respecting the log level.
	logging.Logf(logging.Info, "ETL process completed successfully.")
}