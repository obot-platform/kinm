package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	_ "github.com/lib/pq"
	"github.com/obot-platform/kinm/pkg/db/statements"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apiserver/pkg/storage"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "knowledge"
	password = "knowledge"
	dbname   = "knowledge"
)

func newDatabase(t *testing.T) *db {
	t.Helper()

	extraFields := []string{"field.selector"}

	sqldb, lock := newSQLDB(t)
	_, err := sqldb.ExecContext(context.Background(), "DROP TABLE IF EXISTS recordstest")
	require.NoError(t, err)
	s := &db{
		sqlDB: sqldb,
		stmt:  statements.New("recordstest", extraFields, lock),
		gvk:   testGVK,
	}
	require.NoError(t, s.migrate(context.Background(), extraFields, extraFields))
	insertRows(t, s)
	_, err = sqldb.Exec("INSERT INTO compaction(name, id) values('recordstest', 1) ON CONFLICT(name) DO UPDATE SET id = 1")
	require.NoError(t, err)

	// Migrating a second time should succeed, drop the indexes because we don't need them.
	require.NoError(t, s.migrate(context.Background(), extraFields, nil))
	return s
}

func newSQLDB(t *testing.T) (*sql.DB, bool) {
	t.Helper()

	var (
		err  error
		lock bool
		db   *sql.DB
	)
	if os.Getenv("KINM_TEST_DB") == "postgres" {
		lock = true
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			host, port, user, password, dbname)
		db, err = sql.Open("postgres", psqlInfo)
	} else {
		db, err = sql.Open("sqlite", "otto.db")
		db.SetMaxOpenConns(1)
	}
	if err != nil {
		log.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	return db, lock
}

func TestMigrate(t *testing.T) {
	_ = newDatabase(t)
}

func insertRows(t *testing.T, s *db) {
	t.Helper()

	id, err := s.insert(context.Background(), record{
		name:      "test",
		namespace: "default",
		created:   1,
		vals:      []any{"selector1"},
		value:     "value1",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), id)

	id, err = s.insert(context.Background(), record{
		name:       "test",
		namespace:  "default",
		previousID: &id,
		vals:       []any{"selector2"},
		value:      "value2",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(2), id)

	id, err = s.insert(context.Background(), record{
		name:       "test",
		namespace:  "default",
		previousID: &id,
		vals:       []any{"selector3"},
		value:      "value3",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(3), id)
}

func TestInsert(t *testing.T) {
	s := newDatabase(t)

	_, records, err := s.list(context.Background(), nil, nil, 1, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int16(1), records[0].created)

	_, records, err = s.list(context.Background(), nil, nil, 1, true, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 2)

	assert.Equal(t, int16(0), records[0].created)
	assert.Equal(t, int16(0), records[1].created)
}

func TestAlreadyExists(t *testing.T) {
	s := newDatabase(t)

	_, err := s.insert(context.Background(), record{
		name:      "test",
		namespace: "default",
		created:   1,
		value:     "value1",
	})
	require.Error(t, err)
	assert.True(t, apierrors.IsAlreadyExists(err))
}

func TestGet(t *testing.T) {
	s := newDatabase(t)

	rec, err := s.get(context.Background(), "default", "test")
	require.NoError(t, err)
	assert.Equal(t, "test", rec.name)
	assert.Equal(t, "default", rec.namespace)
	assert.Equal(t, int64(3), rec.id)
	assert.Equal(t, int64(2), *rec.previousID)
}

func TestGetNotFound(t *testing.T) {
	s := newDatabase(t)

	_, err := s.get(context.Background(), "not_found", "default")
	assert.True(t, apierrors.IsNotFound(err))

	_, err = s.get(context.Background(), "deleted", "default")
	assert.True(t, apierrors.IsNotFound(err))
}

func ptr[T any](v T) *T {
	return &v
}

func TestCompactionError(t *testing.T) {
	s := newDatabase(t)

	meta, records, err := s.list(context.Background(), ptr("default"), nil, 0, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	_, err = s.sqlDB.Exec("UPDATE compaction SET id = 3 WHERE name = 'recordstest'")
	require.NoError(t, err)

	meta, records, err = s.list(context.Background(), ptr("default"), nil, 0, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(3), meta.CompactionID)

	_, _, err = s.list(context.Background(), ptr("default"), nil, 2, false, 0, 0, nil)
	assert.True(t, apierrors.IsResourceExpired(err))
}

func TestList(t *testing.T) {
	s := newDatabase(t)

	meta, records, err := s.list(context.Background(), ptr("default"), nil, 0, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	meta, records, err = s.list(context.Background(), ptr("not_default"), nil, 0, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 0)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	meta, records, err = s.list(context.Background(), nil, nil, 0, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(3), records[0].id)
	assert.Equal(t, "value3", records[0].value)
	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	meta, records, err = s.list(context.Background(), nil, nil, 2, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(2), records[0].id)
	assert.Equal(t, "value2", records[0].value)
	assert.Equal(t, int64(2), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	meta, records, err = s.list(context.Background(), nil, nil, 0, false, 0, 0, fields.SelectorFromSet(map[string]string{"field.selector": "selector3"}))
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, int64(3), records[0].id)
	assert.Equal(t, "value3", records[0].value)
	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	meta, records, err = s.list(context.Background(), nil, nil, 0, false, 0, 0, fields.SelectorFromSet(map[string]string{"field.selector": "selector2"}))
	require.NoError(t, err)
	assert.Len(t, records, 0)
}

func TestListAfter(t *testing.T) {
	s := newDatabase(t)

	meta, records, err := s.list(context.Background(), ptr("default"), nil, 1, true, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 2)

	assert.Equal(t, "value2", records[0].value)
	assert.Equal(t, "value3", records[1].value)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)

	meta, records, err = s.list(context.Background(), ptr("default"), nil, 1, true, 0, 0, fields.SelectorFromSet(map[string]string{"field.selector": "selector2"}))
	require.NoError(t, err)
	assert.Len(t, records, 1)

	assert.Equal(t, "value2", records[0].value)

	assert.Equal(t, int64(3), meta.ListID)
	assert.Equal(t, int64(1), meta.CompactionID)
}

func TestDelete(t *testing.T) {
	s := newDatabase(t)

	r, err := s.get(context.Background(), "default", "test")
	require.NoError(t, err)

	id := r.id
	r.previousID = &id
	r.id = 0

	id, err = s.delete(context.Background(), *r)
	require.NoError(t, err)

	assert.Equal(t, int64(4), id)

	_, records, err := s.list(context.Background(), ptr("default"), ptr("test"), 0, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 0)

	_, err = s.get(context.Background(), "default", "test")
	assert.True(t, apierrors.IsNotFound(err))

	_, err = s.sqlDB.ExecContext(context.Background(), "DELETE FROM compaction WHERE name = 'recordstest'")
	require.NoError(t, err)

	_, records, err = s.list(context.Background(), &r.namespace, &r.name, id-1, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)
	assert.True(t, records[0].created == 0)

	_, records, err = s.list(context.Background(), &r.namespace, &r.name, 1, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 1)
	assert.True(t, records[0].created == 1)

	_, records, err = s.list(context.Background(), &r.namespace, &r.name, 1, true, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 3)

	assert.Equal(t, "value2", records[0].value)
	assert.False(t, records[0].deleted == 1)

	assert.Equal(t, "value3", records[1].value)
	assert.False(t, records[1].deleted == 1)

	assert.Equal(t, "value3", records[2].value)
	assert.True(t, records[2].deleted == 1)

	_, err = s.insert(context.Background(), record{
		name:      "test",
		namespace: "default",
		created:   1,
	})
	require.NoError(t, err)
}

func TestCompaction(t *testing.T) {
	s := newDatabase(t)

	test2ID, err := s.insert(context.Background(), record{
		name:    "test2",
		value:   "value1",
		created: 1,
	})
	require.NoError(t, err)

	test3ID, err := s.insert(context.Background(), record{
		name:    "test3",
		value:   "value1",
		created: 1,
	})
	require.NoError(t, err)

	_, err = s.insert(context.Background(), record{
		name:       "test2",
		value:      "value2",
		previousID: &test2ID,
	})
	require.NoError(t, err)

	test3ID, err = s.insert(context.Background(), record{
		name:       "test3",
		value:      "value2",
		previousID: &test3ID,
	})
	require.NoError(t, err)

	_, err = s.delete(context.Background(), record{
		name:       "test3",
		value:      "value3",
		previousID: &test3ID,
	})
	require.NoError(t, err)

	_, records, err := s.list(context.Background(), nil, nil, 1, true, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 7)

	deleted, err := s.compact(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	deleted, err = s.compact(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(4), deleted)

	var count int64
	err = s.sqlDB.QueryRow("SELECT count(*) FROM recordstest").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(4), count)

	_, records, err = s.list(context.Background(), nil, nil, 8, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 2)

	assert.Equal(t, int64(3), records[0].id)
	assert.Equal(t, "test", records[0].name)
	assert.Equal(t, "value3", records[0].value)

	assert.Equal(t, int64(6), records[1].id)
	assert.Equal(t, "test2", records[1].name)
	assert.Equal(t, "value2", records[1].value)
}

func TestCompactionGreaterThan500Records(t *testing.T) {
	s := newDatabase(t)

	// Create 550 other records with created = 1 for all of them.
	for i := range 550 {
		_, err := s.insert(context.Background(), record{
			// Use a hyphen to not conflict with other names.
			name:    fmt.Sprintf("test-%d", i),
			value:   "value",
			created: 1,
		})
		require.NoError(t, err)
	}

	test2ID, err := s.insert(context.Background(), record{
		name:    "test2",
		value:   "value1",
		created: 1,
	})
	require.NoError(t, err)

	test3ID, err := s.insert(context.Background(), record{
		name:    "test3",
		value:   "value1",
		created: 1,
	})
	require.NoError(t, err)

	_, err = s.insert(context.Background(), record{
		name:       "test2",
		value:      "value2",
		previousID: &test2ID,
	})
	require.NoError(t, err)

	test3ID, err = s.insert(context.Background(), record{
		name:       "test3",
		value:      "value2",
		previousID: &test3ID,
	})
	require.NoError(t, err)

	_, err = s.delete(context.Background(), record{
		name:       "test3",
		value:      "value3",
		previousID: &test3ID,
	})
	require.NoError(t, err)

	_, records, err := s.list(context.Background(), nil, nil, 1, true, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 557)

	deleted, err := s.compact(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	deleted, err = s.compact(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(4), deleted)

	var count int64
	err = s.sqlDB.QueryRow("SELECT count(*) FROM recordstest").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(554), count)

	_, records, err = s.list(context.Background(), nil, nil, 558, false, 0, 0, nil)
	require.NoError(t, err)
	assert.Len(t, records, 552)

	assert.Equal(t, int64(3), records[0].id)
	assert.Equal(t, "test", records[0].name)
	assert.Equal(t, "value3", records[0].value)

	assert.Equal(t, int64(556), records[551].id)
	assert.Equal(t, "test2", records[551].name)
	assert.Equal(t, "value2", records[551].value)
}

func TestConflict(t *testing.T) {
	s := newDatabase(t)
	_, err := s.insert(context.Background(), record{
		name:       "test",
		namespace:  "default",
		previousID: ptr(int64(1)),
		value:      "value",
	})
	assert.True(t, apierrors.IsConflict(err))
}

func TestUIDMatch(t *testing.T) {
	s := newDatabase(t)
	_, err := s.insert(context.Background(), record{
		name:       "test",
		namespace:  "default",
		previousID: ptr(int64(3)),
		uid:        "uid",
		value:      "value",
	})
	require.NotNil(t, err)
	assert.Equal(t, `StorageError: invalid object, Code: 4, Key: test, ResourceVersion: 0, AdditionalErrorMsg: Precondition failed: UID in precondition: , UID in object meta: uid`, err.Error())
	_, ok := err.(*storage.StorageError)
	assert.True(t, ok)
}
