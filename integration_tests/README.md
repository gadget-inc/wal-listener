# Integration Tests

This directory contains integration tests for wal-listener that run against a real PostgreSQL database with logical replication enabled.

## Overview

The integration tests verify:
- **Message Publishing**: Real wal-listener functionality with database change capture
- **Schema Filtering**: Include/exclude functionality for schemas and tables  
- **Transaction Size Limiting**: Max transaction size with both buffering modes
- **Event Capture**: Verification of published WAL events

## Prerequisites

- Docker and Docker Compose
- Go 1.18 or later

## Running Tests Locally

1. Navigate to the integration_tests directory:
   ```bash
   cd integration_tests
   ```

2. Start the test environment:
   ```bash
   docker compose up -d --wait
   ```

3. Run the integration tests:
   ```bash
   go test -tags=integration -v ./...
   ```

4. Clean up the test environment:
   ```bash
   docker compose down -v
   ```

## CI Integration

The integration tests run automatically in GitHub Actions as part of the CI pipeline.
