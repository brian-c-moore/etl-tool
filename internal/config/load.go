package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadConfig reads, parses, and validates the YAML configuration file.
// It applies defaults before returning the validated configuration.
func LoadConfig(filename string) (*ETLConfig, error) {
	// Read the configuration file content.
	fileBytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %w", filename, err)
	}

	var config ETLConfig
	// Parse the YAML content into the configuration struct.
	// Use yaml.Unmarshal instead of specific decoder for stricter parsing by default
	err = yaml.Unmarshal(fileBytes, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML in '%s': %w", filename, err)
	}

	// Apply defaults before validation.
	applyDefaults(&config) // Ensure applyDefaults exists and is called

	// Perform comprehensive validation of the loaded configuration.
	if err := ValidateConfig(&config); err != nil { // Ensure ValidateConfig exists and is called
		return nil, err // Return validation errors directly.
	}

	return &config, nil
}

// applyDefaults sets default values for various configuration sections.
func applyDefaults(cfg *ETLConfig) {
	// Logging level default
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = DefaultLogLevel
	}
	// Destination loader batch size default
	if cfg.Destination.Loader != nil && cfg.Destination.Loader.BatchSize < 0 {
		// Allow 0 to mean 'no batching', treat negative as unset
		cfg.Destination.Loader.BatchSize = DefaultLoaderBatchSize
	}
	// Error handling defaults
	if cfg.ErrorHandling == nil {
		cfg.ErrorHandling = &ErrorHandlingConfig{Mode: ErrorHandlingModeHalt}
	} else {
		if cfg.ErrorHandling.Mode == "" {
			cfg.ErrorHandling.Mode = ErrorHandlingModeHalt
		}
		if cfg.ErrorHandling.Mode == ErrorHandlingModeSkip && cfg.ErrorHandling.LogErrors == nil {
			trueVal := true
			cfg.ErrorHandling.LogErrors = &trueVal
		}
	}
	// Deduplication defaults
	if cfg.Dedup != nil && cfg.Dedup.Strategy == "" {
		cfg.Dedup.Strategy = DefaultDedupStrategy
	}

	// Flattening Defaults ---
	if cfg.Flattening != nil {
		if cfg.Flattening.IncludeParent == nil {
			trueVal := true
			cfg.Flattening.IncludeParent = &trueVal // Default to true
		}
		if cfg.Flattening.ErrorOnNonList == nil {
			falseVal := false
			cfg.Flattening.ErrorOnNonList = &falseVal // Default to false
		}
	}

	// Apply format-specific defaults
	applyFormatDefaults(&cfg.Source, &cfg.Destination)
}

// applyFormatDefaults sets defaults for format-specific options in source and destination.
func applyFormatDefaults(src *SourceConfig, dest *DestinationConfig) {
	// CSV Source Defaults
	if src.Type == SourceTypeCSV {
		if src.Delimiter == "" {
			src.Delimiter = DefaultCSVDelimiter
		}
	}
	// CSV Destination Defaults
	if dest.Type == DestinationTypeCSV {
		if dest.Delimiter == "" {
			dest.Delimiter = DefaultCSVDelimiter
		}
	}

	// XLSX Source Defaults (Sheet handling defaults in reader)
	// XLSX Destination Defaults
	if dest.Type == DestinationTypeXLSX {
		if dest.SheetName == "" {
			dest.SheetName = DefaultSheetName
		}
	}

	// XML Source Defaults
	if src.Type == SourceTypeXML {
		if src.XMLRecordTag == "" {
			src.XMLRecordTag = DefaultXMLRecordTag
		}
	}
	// XML Destination Defaults
	if dest.Type == DestinationTypeXML {
		if dest.XMLRecordTag == "" {
			dest.XMLRecordTag = DefaultXMLRecordTag
		}
		if dest.XMLRootTag == "" {
			dest.XMLRootTag = DefaultXMLRootTag
		}
	}

	// YAML defaults (currently none specific needed)
}