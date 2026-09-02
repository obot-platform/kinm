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
	"gorm.io/driver/postgres"
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
	// at that point, so the poll only has to cover a missed notification and it can
	// be long.
	watchPollInterval = time.Minute

	// fallbackWatchPollInterval is the same wait when there is no connected
	// listener and polling is the only way to see another process's writes.
	fallbackWatchPollInterval = 2 * time.Second

	// notifyDebounce bounds how often notifications from other processes wake the
	// watches on one table. The first notification after a quiet moment is never
	// held back, so a change to a table nothing else is writing still arrives in
	// about a millisecond whatever this is set to. It only decides how quickly a
	// run of changes to one table is passed on, and it is the ceiling on how much
	// extra listing a busy table can cause in every other process.
	//
	// At one second, a table written continuously costs another process one list a
	// second, where polling cost it one every two seconds, so no table gets more
	// expensive than it was before this change. A shorter value buys finer updates
	// for a client watching a busy object through a process that is not the writer,
	// and pays for it in lists: at 100ms that same table costs ten a second.
	notifyDebounce = time.Second

	// notifyDisabled turns the whole LISTEN/NOTIFY path off, which means no
	// listening connection, no announcement on write, and watches back on the short
	// poll. It is here so that an operator can get the old behavior without a
	// rollback.
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
	listener         *Listener
}

func NewFactory(schema *runtime.Scheme, dsn string) (*Factory, error) {
	f := &Factory{
		schema: schema,
	}

	var (
		gdb                    gorm.Dialector
		pool                   bool
		skipDefaultTransaction bool
		listen                 bool
	)
	switch {
	case strings.HasPrefix(dsn, "sqlite://"):
		// sqlite is always one process, so the in process broadcast already covers
		// every write and there is no other process to listen to.
		skipDefaultTransaction = true
		gdb = sqlite.Open(strings.TrimPrefix(dsn, "sqlite://"))
	case strings.HasPrefix(dsn, "postgresql://"):
		dsn = strings.Replace(dsn, "postgresql://", "postgres://", 1)
		fallthrough
	case strings.HasPrefix(dsn, "postgres://"):
		gdb = postgres.Open(dsn)
		pool = true
		listen = !notifyDisabled
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
	if pool {
		sqlDB.SetMaxIdleConns(maxIdleConnections)
		sqlDB.SetMaxOpenConns(maxConnections)
	} else {
		sqlDB.SetMaxIdleConns(1)
		sqlDB.SetMaxOpenConns(1)
	}
	f.DB = db
	f.SQLDB = sqlDB

	if listen {
		// The listener holds one connection of its own, outside the pool, and
		// reconnects by itself if it loses that connection. A database that is not
		// reachable yet is not an error here, because watches poll until it
		// answers.
		f.listener = NewListener(dsn)
		f.listener.Start(context.Background())
	}

	return f, nil
}

// Close releases the change listener's connection. Every strategy this factory
// built shares the connection pool, and Strategy.Destroy closes that.
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
	return New(ctx, f.SQLDB, gvk, f.schema, tableName, f.listener)
}
