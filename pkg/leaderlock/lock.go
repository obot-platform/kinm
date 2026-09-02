// Package leaderlock implements client-go's resourcelock.Interface on a plain SQL
// table, so leader election can use the same database as the store without going
// through the versioned object tables.
//
// The versioned store is the wrong shape for a lock. An election lock is a single
// object whose content changes on every renew, and in an append-only store that
// produces hundreds of row versions of one row between compactions, every one of
// which the list query has to sort through to answer a Get. Here the lock is one
// row, read by primary key and updated in place, so its cost does not grow with the
// renew period, the compaction interval, or the number of replicas polling it.
package leaderlock

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	createTableSQL = `CREATE TABLE IF NOT EXISTS leader_lock (
    name    TEXT    PRIMARY KEY,
    record  TEXT    NOT NULL,
    version INTEGER NOT NULL
)`
	getSQL    = `SELECT record, version FROM leader_lock WHERE name = $1`
	createSQL = `INSERT INTO leader_lock (name, record, version) VALUES ($1, $2, 1) ON CONFLICT (name) DO NOTHING`
	updateSQL = `UPDATE leader_lock SET record = $1, version = version + 1 WHERE name = $2 AND version = $3`
)

// groupResource is what NotFound, AlreadyExists and Conflict errors report. There is
// no API group behind this table; client-go only inspects an error's reason.
var groupResource = schema.GroupResource{Group: "kinm.obot.ai", Resource: "leaderlocks"}

// Lock is a resourcelock.Interface backed by one row in the leader_lock table.
//
// Like client-go's LeaseLock it is stateful: Get and Create remember the row version
// they observed, and Update succeeds only if that version is still current. That
// check is what stops two replicas from both believing they hold the lock. Use one
// Lock per elector, per process.
type Lock struct {
	db       *sql.DB
	name     string
	identity string

	// version is the row version last observed by Get or written by Create or
	// Update. Zero means neither has happened and Update must refuse to run.
	version int64
}

// New returns a Lock for the election called name, held under identity, and makes
// sure the backing table exists.
func New(ctx context.Context, db *sql.DB, name, identity string) (*Lock, error) {
	if name == "" {
		return nil, errors.New("leaderlock: name is required")
	}
	if identity == "" {
		return nil, errors.New("leaderlock: identity is required")
	}
	if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
		return nil, fmt.Errorf("leaderlock: creating leader_lock table: %w", err)
	}
	return &Lock{db: db, name: name, identity: identity}, nil
}

// Get returns the current record. A missing row is reported as a Kubernetes
// NotFound because that is the only shape client-go's elector reads as "no lock
// yet, try Create".
func (l *Lock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	var (
		raw     []byte
		version int64
	)
	err := l.db.QueryRowContext(ctx, getSQL, l.name).Scan(&raw, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, apierrors.NewNotFound(groupResource, l.name)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("leaderlock: reading %s: %w", l.name, err)
	}

	var record resourcelock.LeaderElectionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, nil, fmt.Errorf("leaderlock: decoding %s: %w", l.name, err)
	}

	l.version = version
	return &record, raw, nil
}

// Create inserts the record. It fails with AlreadyExists if the row is present;
// client-go treats that as a lost race and tries again on its next period.
func (l *Lock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	raw, err := json.Marshal(ler)
	if err != nil {
		return fmt.Errorf("leaderlock: encoding %s: %w", l.name, err)
	}

	result, err := l.db.ExecContext(ctx, createSQL, l.name, string(raw))
	if err != nil {
		return fmt.Errorf("leaderlock: creating %s: %w", l.name, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("leaderlock: creating %s: %w", l.name, err)
	}
	if n == 0 {
		return apierrors.NewAlreadyExists(groupResource, l.name)
	}

	l.version = 1
	return nil
}

// Update replaces the record, but only while the row is still at the version this
// Lock last observed. If another replica has written since, no row matches and this
// returns Conflict, which client-go answers by re-reading on its slow path.
func (l *Lock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	if l.version == 0 {
		return errors.New("leaderlock: lock not initialized, call Get or Create first")
	}

	raw, err := json.Marshal(ler)
	if err != nil {
		return fmt.Errorf("leaderlock: encoding %s: %w", l.name, err)
	}

	result, err := l.db.ExecContext(ctx, updateSQL, string(raw), l.name, l.version)
	if err != nil {
		return fmt.Errorf("leaderlock: updating %s: %w", l.name, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("leaderlock: updating %s: %w", l.name, err)
	}
	if n == 0 {
		return apierrors.NewConflict(groupResource, l.name,
			fmt.Errorf("version %d is no longer current", l.version))
	}

	l.version++
	return nil
}

// RecordEvent is a no-op; there is no event stream behind a SQL table.
func (*Lock) RecordEvent(string) {}

// Identity returns the identity this Lock acquires and renews under.
func (l *Lock) Identity() string { return l.identity }

// Describe names the lock for log lines.
func (l *Lock) Describe() string { return "leader_lock/" + l.name }
