# Integration Tests

This directory contains integration tests for wal-listener that run against a real PostgreSQL database with logical replication enabled.

## Overview

The integration tests verify:
- **Database Operations**: Basic INSERT, UPDATE, DELETE operations work correctly
- **Schema Operations**: Operations across different schemas and tables
- **Bulk Operations**: Large transactions with multiple operations
- **Replication Slot Management**: Creating and managing logical replication slots

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
   docker-compose up -d --wait
   ```

3. Run the integration tests:
   ```bash
   go test -tags=integration -v ./...
   ```

4. Clean up the test environment:
   ```bash
   docker-compose down -v
   ```

## Test Environment

The Docker Compose setup includes:
- **PostgreSQL 14**: Configured with logical replication (`wal_level=logical`)
- **Test Database Schema**: Multiple schemas and tables for comprehensive testing

## Test Database Schema

The test database includes:
- `public.users` - Main test table for user data
- `public.orders` - Secondary table for relational testing
- `excluded_schema.internal_logs` - Table in excluded schema
- `public.excluded_table` - Excluded table in public schema

## Test Scenarios

### Basic Database Operations
- Tests INSERT, UPDATE, DELETE operations on the `users` table
- Verifies data integrity and transaction consistency
- Ensures proper database connectivity and operations

### Schema Operations
- Tests operations across multiple schemas (`public`, `excluded_schema`)
- Verifies schema-level filtering capabilities
- Tests table-level exclusions within schemas

### Bulk Operations
- Performs large transactions with multiple INSERT operations
- Tests transaction handling and performance
- Verifies database can handle bulk data operations

### Replication Slot Management
- Tests creation and deletion of logical replication slots
- Verifies replication slot functionality
- Ensures proper cleanup of replication resources

## CI Integration

The integration tests run automatically in GitHub Actions as part of the CI pipeline. They are executed after the unit tests pass to ensure the database infrastructure and logical replication functionality work correctly in a real environment.
