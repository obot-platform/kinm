package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/obot-platform/kinm/pkg/db/glogrus"
	"github.com/obot-platform/kinm/pkg/strategy"
	"github.com/obot-platform/kinm/pkg/types"
	"github.com/sirupsen/logrus"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

var (
	maxConnections     = 5
	maxIdleConnections = 2
	maxConnLifetime    = 3 * time.Minute

	// watchPollInterval is how often a watch lists again when nothing has woken it
	// and the change listener is connected. Every process announces its own writes
	// then, so the poll only has to cover a missed notification.
	watchPollInterval = time.Minute

	// fallbackWatchPollInterval is the same wait with no connected listener, when
	// polling is the only way to see another process's writes.
	fallbackWatchPollInterval = 2 * time.Second

	// notifyDebounce bounds how often notifications from other processes wake the
	// watches on one table. The first notification after a quiet moment is never
	// held back, so it does not affect how quickly an isolated change is seen. It
	// caps what a continuously written table costs every other process: at one
	// second that is one list a second, against one every two seconds for the poll
	// it replaces.
	notifyDebounce = time.Second

	// notifyDisabled turns the whole LISTEN/NOTIFY path off, leaving watches on the
	// short poll, so that an operator can get the old behavior without a rollback.
	notifyDisabled bool
)

func init() {
	if x, err := strconv.Atoi(os.Getenv("KINM_DB_CONNECTIONS")); err == nil && x > 0 {
		maxConnections = x
		maxIdleConnections = x
	}
	if x, err := strconv.Atoi(os.Getenv("KINM_DB_MAX_IDLE_CONNECTIONS")); err == nil && x > 0 {
		maxIdleConnections = x
	}
	if x, err := strconv.Atoi(os.Getenv("KINM_DB_MAX_CONNECTIONS")); err == nil && x > 0 {
		maxConnections = x
	}
	if x, err := strconv.Atoi(os.Getenv("KINM_DB_MAX_CONNECTION_LIFETIME_SECONDS")); err == nil && x > 0 {
		maxConnLifetime = time.Duration(x) * time.Second
	}
	if x, err := strconv.Atoi(os.Getenv("KINM_DB_WATCH_POLL_SECONDS")); err == nil && x > 0 {
		watchPollInterval = time.Duration(x) * time.Second
	}
	if x, err := strconv.Atoi(os.Getenv("KINM_DB_NOTIFY_DEBOUNCE_MILLISECONDS")); err == nil && x > 0 {
		notifyDebounce = time.Duration(x) * time.Millisecond
	}
	if x, err := strconv.ParseBool(os.Getenv("KINM_DB_DISABLE_NOTIFY")); err == nil {
		notifyDisabled = x
	}
}

type Factory struct {
	DB               *gorm.DB
	SQLDB            *sql.DB
	schema           *runtime.Scheme
	migrationTimeout time.Duration
	postgres         bool
	listener         *Listener
}

func NewFactory(schema *runtime.Scheme, dsn string) (*Factory, error) {
	f := &Factory{
		schema: schema,
	}

	var (
		gdb                    gorm.Dialector
		postgres               bool
		skipDefaultTransaction bool
	)
	switch {
	case strings.HasPrefix(dsn, "sqlite://"):
		// One process, so the in process broadcast already covers every write.
		skipDefaultTransaction = true
		gdb = sqlite.Open(strings.TrimPrefix(dsn, "sqlite://"))
	case strings.HasPrefix(dsn, "postgresql://"):
		dsn = strings.Replace(dsn, "postgresql://", "postgres://", 1)
		fallthrough
	case strings.HasPrefix(dsn, "postgres://"):
		gdb = pgdriver.Open(dsn)
		postgres = true
	default:
		return nil, fmt.Errorf("unsupported database: %s", dsn)
	}
	db, err := gorm.Open(gdb, &gorm.Config{
		SkipDefaultTransaction: skipDefaultTransaction,
		Logger: glogrus.New(glogrus.Config{
			SlowThreshold:             200 * time.Millisecond,
			IgnoreRecordNotFoundError: true,
			LogSQL:                    true,
		}),
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetConnMaxLifetime(maxConnLifetime)
	if postgres {
		sqlDB.SetMaxIdleConns(maxIdleConnections)
		sqlDB.SetMaxOpenConns(maxConnections)
	} else {
		sqlDB.SetMaxIdleConns(1)
		sqlDB.SetMaxOpenConns(1)
	}
	f.DB = db
	f.SQLDB = sqlDB
	f.postgres = postgres

	if postgres && !notifyDisabled {
		// One connection of its own, outside the pool, reconnecting by itself. A
		// database that is not reachable yet is not an error, because watches poll
		// until it answers.
		f.listener = NewListener(dsn)
		f.listener.Start(context.Background())
	}

	return f, nil
}

// Close releases the change listener's connection. The pool is shared with every
// strategy this factory built and is closed by Strategy.Destroy.
func (f *Factory) Close() {
	if f.listener != nil {
		f.listener.Close()
	}
}

func (f *Factory) Scheme() *runtime.Scheme {
	return f.schema
}

func (f *Factory) Name() string {
	return "Kinm DB"
}

func (f *Factory) Check(req *http.Request) error {
	err := f.SQLDB.PingContext(req.Context())
	if err != nil {
		logrus.Warnf("Failed to ping database: %v", err)
	}

	return err
}

type TableNamer interface {
	TableName() string
}

func (f *Factory) NewDBStrategy(obj types.Object) (strategy.CompleteStrategy, error) {
	gvk, err := apiutil.GVKForObject(obj, f.schema)
	if err != nil {
		return nil, err
	}

	tableName := strings.ToLower(gvk.Kind)
	if tn, ok := obj.(TableNamer); ok {
		tableName = tn.TableName()
	}

	ctx := context.Background()
	if f.migrationTimeout != 0 {
		// If configured, set a timeout for the migration
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.migrationTimeout)
		defer cancel()
	}
	return New(ctx, f.SQLDB, gvk, f.schema, tableName, f.postgres, f.listener)
}
