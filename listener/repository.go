package listener

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx"
)

// RepositoryImpl service repository.
type RepositoryImpl struct {
	conn *pgx.Conn
}

// NewRepository returns a new instance of the repository.
func NewRepository(conn *pgx.Conn) *RepositoryImpl {
	return &RepositoryImpl{conn: conn}
}

// GetSlotLSN returns the value of the last offset for a specific slot.
func (r RepositoryImpl) GetSlotLSN(slotName string) (*string, error) {
	var restartLSNStr *string

	err := r.conn.QueryRow("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=$1;", slotName).
		Scan(&restartLSNStr)

	return restartLSNStr, err
}

// CreatePublication create publication fo all.
func (r RepositoryImpl) CreatePublication(name string) error {
	if _, err := r.conn.Exec(`CREATE PUBLICATION "` + name + `" FOR ALL TABLES`); err != nil && !strings.Contains("already exists", err.Error()) {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

// IsAlive check database connection problems.
func (r RepositoryImpl) NewStandbyStatus(walPositions ...uint64) (status *pgx.StandbyStatus, err error) {
	return pgx.NewStandbyStatus(walPositions...)
}

// IsAlive check database connection problems.
func (r RepositoryImpl) IsAlive() bool {
	return r.conn.IsAlive()
}

// Close database connection.
func (r RepositoryImpl) Close() error {
	return r.conn.Close()
}
