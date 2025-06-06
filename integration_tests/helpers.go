
package integration_tests

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/jackc/pgx/v5"
)

func setupDockerEnvironment() error {
	cmd := exec.Command("docker", "compose", "up", "-d", "--wait")
	cmd.Dir = "."
	return cmd.Run()
}

func teardownDockerEnvironment() error {
	cmd := exec.Command("docker", "compose", "down", "-v")
	cmd.Dir = "."
	return cmd.Run()
}

func waitForDatabase(connString string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for database")
		default:
			conn, err := pgx.Connect(ctx, connString)
			if err == nil {
				conn.Close(ctx)
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func cleanupDatabase(connString string) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		SELECT pg_drop_replication_slot(slot_name) 
		FROM pg_replication_slots 
		WHERE slot_name LIKE 'integration_test_slot_%'
	`)
	if err != nil {
	}

	_, err = conn.Exec(ctx, "TRUNCATE public.users, public.orders, excluded_schema.internal_logs, public.excluded_table RESTART IDENTITY CASCADE")
	return err
}
