package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/storage/value"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

type Factory struct {
	DB                  *gorm.DB
	SQLDB               *sql.DB
	schema              *runtime.Scheme
	migrationTimeout    time.Duration
	transformers        map[schema.GroupKind]value.Transformer
	partitionIDRequired bool
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
	sqlDB.SetConnMaxLifetime(time.Minute * 3)
	if pool {
		sqlDB.SetMaxIdleConns(5)
		sqlDB.SetMaxOpenConns(5)
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
