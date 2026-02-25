# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0-alpha] - 2026-02-25

### Added
- Comprehensive logging system with automatic archiving to `logs/` directory
- Timestamped log files for every run with both console and file output
- Stock price fetching from Stooq API with intelligent filtering
- Orchestrator refactored to execute steps in the order defined in configuration
- Table creation for stock prices database with pandas
- Suppression of verbose third-party library debug logs (chardet)
- Improved error handling with full exception tracebacks in logs

### Fixed
- Stock prices table now properly created before inserting data
- Chardet encoding detection debug logs no longer clutter output
- Step execution order now respects configuration file order
- Stock price API calls now only made for companies with financial data

### Changed
- Replaced print statements with proper logging throughout orchestrator and APIs
- Stock price scraping now filters to only companies in standardized financial data table
- Improved log messages with more context and better formatting

### Known Issues
- None reported

### Architecture
- Application follows MVC pattern with orchestrator managing step execution
- Configuration-driven design allows flexible step ordering and feature toggling
- Comprehensive test suite for core modules
