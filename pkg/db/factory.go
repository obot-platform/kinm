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

// defaultCompactionInterval is how often each table drops superseded row versions.
//
// The store is append-only, so a table's steady-state size is its write rate times
// this interval, and list.sql pays for every row version it holds. An object that
// is rewritten every 2s accumulates ~450 versions over 15 minutes and ~90 over 3.
// Watchers that fall more than one interval behind must re-list, so this must stay
// comfortably above the 2s watch poll; 3 minutes is 90 polls of headroom.
const defaultCompactionInterval = 3 * time.Minute

var (
	maxConnections     = 5
	maxIdleConnections = 2
	maxConnLifetime    = 3 * time.Minute
	compactionInterval = defaultCompactionInterval
)

// compactionIntervalFromEnv parses KINM_DB_COMPACTION_INTERVAL_SECONDS, falling back
// to defaultCompactionInterval when the value is unset, unparseable, or not positive.
func compactionIntervalFromEnv(value string) time.Duration {
	if x, err := strconv.Atoi(value); err == nil && x > 0 {
		return time.Duration(x) * time.Second
	}
	return defaultCompactionInterval
}

func init() {
	compactionInterval = compactionIntervalFromEnv(os.Getenv("KINM_DB_COMPACTION_INTERVAL_SECONDS"))
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
}

type Factory struct {
	DB               *gorm.DB
	SQLDB            *sql.DB
	schema           *runtime.Scheme
	migrationTimeout time.Duration
}

func NewFactory(schema *runtime.Scheme, dsn string) (*Factory, error) {
	f := &Factory{
		schema: schema,
	}

	var (
		gdb                    gorm.Dialector
		pool                   bool
		skipDefaultTransaction bool
	)
	switch {
	case strings.HasPrefix(dsn, "sqlite://"):
		skipDefaultTransaction = true
		gdb = sqlite.Open(strings.TrimPrefix(dsn, "sqlite://"))
	case strings.HasPrefix(dsn, "postgresql://"):
		dsn = strings.Replace(dsn, "postgresql://", "postgres://", 1)
		fallthrough
	case strings.HasPrefix(dsn, "postgres://"):
		gdb = postgres.Open(dsn)
		pool = true
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
	return f, nil
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
	return New(ctx, f.SQLDB, gvk, f.schema, tableName)
}
