

package integration_tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	dbConnString = "postgres://postgres:postgres@localhost:5432/postgres"
	testTimeout  = 60 * time.Second
)

func TestMain(m *testing.M) {
	if err := setupDockerEnvironment(); err != nil {
		fmt.Printf("Failed to setup Docker environment: %v\n", err)
		os.Exit(1)
	}

	if err := waitForDatabase(dbConnString, testTimeout); err != nil {
		fmt.Printf("Failed to wait for database: %v\n", err)
		teardownDockerEnvironment()
		os.Exit(1)
	}

	code := m.Run()

	if err := teardownDockerEnvironment(); err != nil {
		fmt.Printf("Failed to teardown Docker environment: %v\n", err)
	}

	os.Exit(code)
}

func createReplicationSlot(t *testing.T, slotName string) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slotName))
	if err != nil {
		t.Logf("Replication slot may already exist: %v", err)
	}
}

func dropReplicationSlot(t *testing.T, slotName string) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
	if err != nil {
		t.Logf("Failed to drop replication slot: %v", err)
	}
}

func TestBasicDatabaseOperations(t *testing.T) {
	require.NoError(t, cleanupDatabase(dbConnString))

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "INSERT INTO public.users (name, email) VALUES ($1, $2)", "John Doe", "john@example.com")
	require.NoError(t, err)

	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM public.users WHERE name = $1", "John Doe").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	_, err = conn.Exec(ctx, "UPDATE public.users SET email = $1 WHERE name = $2", "john.doe@example.com", "John Doe")
	require.NoError(t, err)

	var email string
	err = conn.QueryRow(ctx, "SELECT email FROM public.users WHERE name = $1", "John Doe").Scan(&email)
	require.NoError(t, err)
	assert.Equal(t, "john.doe@example.com", email)

	_, err = conn.Exec(ctx, "DELETE FROM public.users WHERE name = $1", "John Doe")
	require.NoError(t, err)

	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM public.users WHERE name = $1", "John Doe").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestSchemaOperations(t *testing.T) {
	require.NoError(t, cleanupDatabase(dbConnString))

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "INSERT INTO public.users (name, email) VALUES ($1, $2)", "Jane Doe", "jane@example.com")
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "INSERT INTO excluded_schema.internal_logs (message) VALUES ($1)", "This is in excluded schema")
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "INSERT INTO public.excluded_table (data) VALUES ($1)", "This is in excluded table")
	require.NoError(t, err)

	var userCount, logCount, excludedCount int

	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM public.users").Scan(&userCount)
	require.NoError(t, err)
	assert.Equal(t, 1, userCount)

	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM excluded_schema.internal_logs").Scan(&logCount)
	require.NoError(t, err)
	assert.Equal(t, 1, logCount)

	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM public.excluded_table").Scan(&excludedCount)
	require.NoError(t, err)
	assert.Equal(t, 1, excludedCount)
}

func TestBulkOperations(t *testing.T) {
	require.NoError(t, cleanupDatabase(dbConnString))

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO public.users (name, email) VALUES ($1, $2)", 
			fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
	}

	err = tx.Commit(ctx)
	require.NoError(t, err)

	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM public.users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 10, count)
}

func TestReplicationSlotCreation(t *testing.T) {
	slotName := "test_slot"
	createReplicationSlot(t, slotName)
	
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	var exists bool
	err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists, "Replication slot should exist")

	dropReplicationSlot(t, slotName)
}
