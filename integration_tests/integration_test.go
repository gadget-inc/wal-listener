
package integration_tests

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sync"
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

type WalEvent struct {
	ID                     string            `json:"id"`
	Schema                 string            `json:"schema"`
	Table                  string            `json:"table"`
	Action                 string            `json:"action"`
	Data                   map[string]any    `json:"data"`
	DataOld                map[string]any    `json:"dataOld"`
	EventTime              time.Time         `json:"commitTime"`
	UnchangedToastedValues []string          `json:"unchangedToastedValues"`
	Tags                   map[string]string `json:"tags"`
}

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

func startWalListener(t *testing.T, configPath string) (chan WalEvent, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", "/tmp/wal-listener", "./cmd/wal-listener")
	buildCmd.Dir = ".."
	err := buildCmd.Run()
	require.NoError(t, err, "Failed to build wal-listener")

	cmd := exec.CommandContext(ctx, "/tmp/wal-listener", "--config", configPath)
	cmd.Dir = "."
	
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)

	err = cmd.Start()
	require.NoError(t, err)

	eventChan := make(chan WalEvent, 100)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			var event WalEvent
			if err := json.Unmarshal([]byte(line), &event); err == nil {
				select {
				case eventChan <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			t.Logf("wal-listener stderr: %s", scanner.Text())
		}
	}()

	cleanup := func() {
		cancel()
		cmd.Process.Kill()
		cmd.Wait()
		wg.Wait()
		close(eventChan)
	}

	return eventChan, cleanup
}

func waitForEvents(t *testing.T, eventChan chan WalEvent, expectedCount int, timeout time.Duration) []WalEvent {
	var events []WalEvent
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for len(events) < expectedCount {
		select {
		case event := <-eventChan:
			events = append(events, event)
		case <-timer.C:
			t.Fatalf("Timeout waiting for events. Expected %d, got %d", expectedCount, len(events))
		}
	}

	return events
}

func createTestConfig(t *testing.T, maxTransactionSize int, skipBuffering bool) string {
	configContent := fmt.Sprintf(`
database:
  host: localhost
  port: 5432
  name: postgres
  user: postgres
  password: postgres

listener:
  slot_name: integration_test_slot_%d
  ack_timeout: 10s
  refresh_connection: 30s
  heartbeat_interval: 10s
  include:
    tables:
      users: ["insert", "update", "delete"]
      orders: ["insert", "update", "delete"]
  exclude:
    schemas: ["excluded_schema"]
    tables: ["excluded_table"]
  skipTransactionBuffering: %t
  maxTransactionSize: %d

publisher:
  type: stdout
  topic: wal_events

logger:
  level: info
  caller: false
  encoding: json
`, time.Now().UnixNano(), skipBuffering, maxTransactionSize)

	configFile := fmt.Sprintf("/tmp/test_config_%d.yml", time.Now().UnixNano())
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)
	
	return configFile
}

func TestWalListenerBasicOperations(t *testing.T) {
	require.NoError(t, cleanupDatabase(dbConnString))

	configPath := createTestConfig(t, 0, false)
	defer os.Remove(configPath)

	eventChan, cleanup := startWalListener(t, configPath)
	defer cleanup()

	time.Sleep(2 * time.Second)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "INSERT INTO public.users (name, email) VALUES ($1, $2)", "John Doe", "john@example.com")
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "UPDATE public.users SET email = $1 WHERE name = $2", "john.doe@example.com", "John Doe")
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "DELETE FROM public.users WHERE name = $1", "John Doe")
	require.NoError(t, err)

	events := waitForEvents(t, eventChan, 3, 10*time.Second)

	assert.Len(t, events, 3)
	
	insertEvent := events[0]
	assert.Equal(t, "public", insertEvent.Schema)
	assert.Equal(t, "users", insertEvent.Table)
	assert.Equal(t, "INSERT", insertEvent.Action)
	assert.Equal(t, "John Doe", insertEvent.Data["name"])
	assert.Equal(t, "john@example.com", insertEvent.Data["email"])

	updateEvent := events[1]
	assert.Equal(t, "UPDATE", updateEvent.Action)
	assert.Equal(t, "john.doe@example.com", updateEvent.Data["email"])
	assert.Equal(t, "john@example.com", updateEvent.DataOld["email"])

	deleteEvent := events[2]
	assert.Equal(t, "DELETE", deleteEvent.Action)
	assert.Equal(t, "John Doe", deleteEvent.DataOld["name"])
}

func TestWalListenerSchemaFiltering(t *testing.T) {
	require.NoError(t, cleanupDatabase(dbConnString))

	configPath := createTestConfig(t, 0, false)
	defer os.Remove(configPath)

	eventChan, cleanup := startWalListener(t, configPath)
	defer cleanup()

	time.Sleep(2 * time.Second)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "INSERT INTO public.users (name, email) VALUES ($1, $2)", "Jane Doe", "jane@example.com")
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "INSERT INTO excluded_schema.internal_logs (message) VALUES ($1)", "This should be excluded")
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "INSERT INTO public.excluded_table (data) VALUES ($1)", "This should also be excluded")
	require.NoError(t, err)

	events := waitForEvents(t, eventChan, 1, 5*time.Second)

	assert.Len(t, events, 1)
	assert.Equal(t, "public", events[0].Schema)
	assert.Equal(t, "users", events[0].Table)
	assert.Equal(t, "INSERT", events[0].Action)
	assert.Equal(t, "Jane Doe", events[0].Data["name"])
}

func TestWalListenerTransactionSizeLimit(t *testing.T) {
	require.NoError(t, cleanupDatabase(dbConnString))

	configPath := createTestConfig(t, 2, false)
	defer os.Remove(configPath)

	eventChan, cleanup := startWalListener(t, configPath)
	defer cleanup()

	time.Sleep(2 * time.Second)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO public.users (name, email) VALUES ($1, $2)", 
			fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
	}

	err = tx.Commit(ctx)
	require.NoError(t, err)

	events := waitForEvents(t, eventChan, 2, 10*time.Second)

	assert.Len(t, events, 2)
	for i, event := range events {
		assert.Equal(t, "public", event.Schema)
		assert.Equal(t, "users", event.Table)
		assert.Equal(t, "INSERT", event.Action)
		assert.Equal(t, fmt.Sprintf("User %d", i), event.Data["name"])
	}
}

func TestWalListenerTransactionSizeLimitWithSkipBuffering(t *testing.T) {
	require.NoError(t, cleanupDatabase(dbConnString))

	configPath := createTestConfig(t, 2, true)
	defer os.Remove(configPath)

	eventChan, cleanup := startWalListener(t, configPath)
	defer cleanup()

	time.Sleep(2 * time.Second)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbConnString)
	require.NoError(t, err)
	defer conn.Close(ctx)

	for i := 0; i < 5; i++ {
		_, err = conn.Exec(ctx, "INSERT INTO public.users (name, email) VALUES ($1, $2)", 
			fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
	}

	events := waitForEvents(t, eventChan, 2, 10*time.Second)

	assert.Len(t, events, 2)
	for i, event := range events {
		assert.Equal(t, "public", event.Schema)
		assert.Equal(t, "users", event.Table)
		assert.Equal(t, "INSERT", event.Action)
		assert.Equal(t, fmt.Sprintf("User %d", i), event.Data["name"])
	}
}
