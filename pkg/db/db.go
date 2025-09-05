package db

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"github.com/obot-platform/kinm/pkg/db/errors"
	"github.com/obot-platform/kinm/pkg/db/statements"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type db struct {
	sqlDB           *sql.DB
	stmt            *statements.Statements
	gvk             schema.GroupVersionKind
	extraFieldNames map[string]int
}

func (d *db) Close() {
	_ = d.sqlDB.Close()
}

func (d *db) migrate(ctx context.Context, extraColumnNames, indexFields []string) error {
	d.extraFieldNames = make(map[string]int, len(extraColumnNames))
	for i, name := range extraColumnNames {
		d.extraFieldNames[name] = i
	}

	_, err := d.execContext(ctx, d.stmt.CreateSQL())
	if err != nil {
		return err
	}

	var count int
	for _, name := range extraColumnNames {
		// Check if column already exists
		if err = d.queryRowContext(ctx, d.stmt.CheckColumnSQL(name)).Scan(&count); err == nil && count > 0 {
			// Ignore errors because we will just try to add the column.
			// Skip adding column if it already exists
			continue
		}

		if _, err = d.execContext(ctx, d.stmt.AddColumnSQL(name)); err != nil {
			switch e := err.(type) {
			case *pq.Error:
				if e.Code == "42701" {
					continue
				}
			case *pgconn.PgError:
				if e.Code == "42701" {
					continue
				}
			case sqlCode:
				if e.Code() == 1 && strings.Contains(err.Error(), "duplicate column name") {
					continue
				}
			}
			return err
		}
	}

	_, err = d.execContext(ctx, d.stmt.DropFieldsIndexSQL())
	if err != nil {
		return err
	}

	if len(indexFields) > 0 {
		_, err = d.execContext(ctx, d.stmt.AddFieldsIndexSQL(indexFields))
	}

	return err
}

type txKey struct{}

func (d *db) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if query == "" {
		return nil, nil
	}
	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	if ok {
		return tx.ExecContext(ctx, query, args...)
	}
	return d.sqlDB.ExecContext(ctx, query, args...)
}

func (d *db) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	ctx, span := tracer.Start(ctx, "dbQueryContext", trace.WithAttributes(attribute.String("query", query), attribute.String("args", fmt.Sprint(args...))))
	defer span.End()

	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	if ok {
		return tx.QueryContext(ctx, query, args...)
	}
	return d.sqlDB.QueryContext(ctx, query, args...)
}

func (d *db) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	ctx, span := tracer.Start(ctx, "dbQueryRowContext", trace.WithAttributes(attribute.String("query", query), attribute.String("args", fmt.Sprint(args...))))
	defer span.End()

	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	if ok {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return d.sqlDB.QueryRowContext(ctx, query, args...)
}

type tx interface {
	Rollback() error
	Commit() error
}

type noopTx struct{}

func (n noopTx) Rollback() error {
	return nil
}

func (n noopTx) Commit() error {
	return nil
}

func (d *db) beginTx(ctx context.Context, options *sql.TxOptions) (context.Context, tx, error) {
	// Don't use the context here because it will look like a nested span for everything the callers does after calling this.
	_, span := tracer.Start(ctx, "dbBeginTx")
	defer span.End()

	_, ok := ctx.Value(txKey{}).(*sql.Tx)
	if ok {
		// don't actually nest transactions
		return ctx, noopTx{}, nil
	}
	tx, err := d.sqlDB.BeginTx(ctx, options)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, txKey{}, tx), tx, nil
}

func (d *db) get(ctx context.Context, namespace, name string) (*record, error) {
	ctx, span := tracer.Start(ctx, "dbGet", trace.WithAttributes(attribute.String("namespace", namespace), attribute.String("name", name)))
	defer span.End()

	_, records, err := d.list(ctx, getNamespace(namespace), &name, 0, false, 0, 1, nil)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, errors.NewNotFound(d.gvk, name)
	}
	return &records[0], nil
}

type tableMeta struct {
	ListID       int64
	CompactionID int64
}

// list after=true will return all records after rev, whereas after=false it will return just the latest resourceVersion
// for each name,namespace pair for all records <= rev
func (d *db) list(ctx context.Context, namespace, name *string, rev int64, after bool, cont, limit int64, fieldSelector fields.Selector) (tableMeta, []record, error) {
	ctx, span := tracer.Start(ctx, "dbList")
	defer span.End()

	if cont > 0 && rev <= 0 {
		panic("rev must be set when cont is set")
	}
	if after && cont != 0 {
		panic("cont must be zero when after is true")
	}

	vals := make([]any, len(d.extraFieldNames))
	if fieldSelector != nil {
		for _, r := range fieldSelector.Requirements() {
			if idx, ok := d.extraFieldNames[r.Field]; ok {
				vals[idx] = r.Value
			}
		}
	}

	ctx, tx, err := d.beginTx(ctx, &sql.TxOptions{
		// Repeatable read is needed to ensure that the ListID is consistent across multiple queries
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return tableMeta{}, nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	meta, records, err := d.doList(ctx, namespace, name, rev, after, cont, limit, vals)
	if err != nil {
		return tableMeta{}, nil, err
	}

	if rev > 0 && !after {
		// Set the ListID to the requested revision
		meta.ListID = rev
	}

	// this can possibly be zero if when no results were found. Also notice the isolation is repeatable read
	// so that we will get the same ID that was used in the first query
	if meta.ListID == 0 {
		meta, err = d.getTableMeta(ctx)
		if err != nil {
			return tableMeta{}, nil, err
		}
	}

	// ListID can be zero if no records exist in the table. Also don't check if rev is zero that means
	// a specific revision was not requested and there we don't need to consider compaction. This condition
	// is important for when the compaction ID is greater than any existing ID in the table. That can happen
	// after a compaction where the last row was a delete=true row.
	if rev != 0 && meta.ListID != 0 && meta.ListID < meta.CompactionID {
		return meta, nil, errors.NewCompactionError(uint(meta.ListID), uint(meta.CompactionID))
	}

	return meta, records, tx.Commit()
}

func (d *db) getTableMeta(ctx context.Context) (meta tableMeta, _ error) {
	ctx, span := tracer.Start(ctx, "dbGetTableMeta")
	defer span.End()

	err := d.queryRowContext(ctx, d.stmt.TableMetaSQL()).Scan(&meta.ListID, &meta.CompactionID)
	return meta, err
}

func (d *db) doList(ctx context.Context, namespace, name *string, rev int64, after bool, cont, limit int64, vals []any) (meta tableMeta, _ []record, _ error) {
	ctx, span := tracer.Start(ctx, "dbDoList")
	defer span.End()

	var (
		rows *sql.Rows
		err  error
	)
	if vals == nil {
		vals = make([]any, len(d.extraFieldNames))
	} else if len(vals) != len(d.extraFieldNames) {
		panic("vals must have the same length as extraFieldNames")
	}

	if after {
		vals = append([]any{namespace, name, rev}, vals...)
		span.AddEvent("listAfterSQL")
		rows, err = d.queryContext(ctx, d.stmt.ListAfterSQL(limit), vals...)
	} else {
		vals = append([]any{namespace, name, rev, cont}, vals...)
		span.AddEvent("listSQL")
		rows, err = d.queryContext(ctx, d.stmt.ListSQL(limit), vals...)
	}
	if err != nil {
		return meta, nil, err
	}
	defer rows.Close()

	var records []record
	for rows.Next() {
		var (
			r       record
			created sql.NullInt16
		)
		if err := rows.Scan(
			&meta.ListID,
			&meta.CompactionID,
			&r.id, &r.name, &r.namespace, &r.previousID, &r.uid, &created, &r.deleted, &r.value); err != nil {
			return meta, nil, err
		}
		if created.Valid {
			r.created = created.Int16
		}
		records = append(records, r)
	}
	return meta, records, nil
}

func (d *db) insert(ctx context.Context, rec record) (id int64, _ error) {
	ctx, span := tracer.Start(ctx, "dbInsert")
	defer span.End()

	span.AddEvent("beginTx")
	ctx, tx, err := d.beginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	span.AddEvent("doInsert")
	id, err = d.doInsert(ctx, rec)
	if err != nil {
		return 0, err
	}

	return id, tx.Commit()
}

type sqlError interface {
	SQLState() string
}

type sqlCode interface {
	Code() int
}

func (d *db) doInsert(ctx context.Context, rec record) (id int64, err error) {
	ctx, span := tracer.Start(ctx, "dbDoInsert")
	defer span.End()

	if rec.vals == nil {
		rec.vals = make([]any, len(d.extraFieldNames))
	} else if len(rec.vals) != len(d.extraFieldNames) {
		panic("vals must have the same length as extraFieldNames")
	}

	span.AddEvent("lockTable")
	_, err = d.execContext(ctx, d.stmt.TableLockSQL())
	if err != nil {
		return 0, err
	}

	if rec.id != 0 {
		panic("id must be zero")
	}
	if rec.created == 1 && rec.previousID != nil {
		panic("previousID must be nil when created is true")
	}
	if rec.created == 0 && rec.previousID == nil {
		panic("previousID must be set when created is false")
	}

	// only check on update, on create DB constraints errors
	if rec.created == 0 {
		existing, err := d.get(ctx, rec.namespace, rec.name)
		if apierrors.IsNotFound(err) {
			return 0, errors.NewResourceVersionMismatch(d.gvk, rec.name)
		} else if err != nil {
			return 0, err
		} else if existing.id != *rec.previousID {
			return 0, errors.NewResourceVersionMismatch(d.gvk, rec.name)
		} else if existing.uid != rec.uid {
			return 0, errors.NewUIDMismatch(rec.name, existing.uid, rec.uid)
		} else if rec.deleted == 0 && existing.value == rec.value {
			return existing.id, nil
		}
	}

	var createdAny any
	if rec.created == 1 {
		createdAny = 1
	}

	args := append([]any{rec.name, rec.namespace, rec.previousID, rec.uid, createdAny, rec.deleted, rec.value}, rec.vals...)
	span.AddEvent("insertSQL")
	err = d.queryRowContext(ctx, d.stmt.InsertSQL(), args...).Scan(&id)
	if pgErr, ok := err.(sqlError); ok && pgErr.SQLState() == "23505" {
		return 0, errors.NewAlreadyExists(d.gvk, rec.name)
	} else if sqliteErr, ok := err.(sqlCode); ok && sqliteErr.Code() == 2067 {
		return 0, errors.NewAlreadyExists(d.gvk, rec.name)
	} else if err != nil {
		return 0, err
	}
	return
}

func (d *db) delete(ctx context.Context, r record) (int64, error) {
	ctx, span := tracer.Start(ctx, "dbDelete")
	defer span.End()

	ctx, tx, err := d.beginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if r.previousID == nil {
		panic("previousID must be set")
	}

	r.created = 0
	r.deleted = 1

	id, err := d.doInsert(ctx, r)
	if err != nil {
		return 0, err
	}

	span.AddEvent("clearCreatedSQL")
	if _, err := d.execContext(ctx, d.stmt.ClearCreatedSQL(), r.namespace, r.name, id); err != nil {
		return 0, err
	}

	return id, tx.Commit()
}

func (d *db) compact(ctx context.Context) (resultCount int64, _ error) {
	ctx, span := tracer.Start(ctx, "dbCompact", trace.WithAttributes(attribute.String("gvk", d.gvk.String())))
	defer span.End()
	for {
		span.AddEvent("compactSQL")
		result, err := d.execContext(ctx, d.stmt.CompactSQL())
		if err != nil {
			return resultCount, err
		}
		count, err := result.RowsAffected()
		resultCount += count
		if err != nil {
			return resultCount, err
		} else if count == 0 {
			break
		}
	}

	span.AddEvent("updateCompactionSQL")
	_, err := d.execContext(ctx, d.stmt.UpdateCompactionSQL())
	return resultCount, err
}
