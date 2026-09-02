package statements

import (
	"embed"
	_ "embed"
	"fmt"
	"strings"
)

//go:embed *.sql
var fs embed.FS

// NotifyChannel is the Postgres LISTEN/NOTIFY channel every table announces its
// writes on, and the payload is the table name. It is substituted into notify.sql
// so that the constant and the statement cannot disagree.
const NotifyChannel = "kinm"

func (s *Statements) CreateSQL() string { return s.statements["migrate.sql"] }

func (s *Statements) CheckColumnSQL(name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	return strings.Replace(
		strings.Replace(s.statements["checkcolumn.sql"], "new_column", name, 1),
		// Some databases transform the column name to lowercase. Check that too.
		"new_column_lower", strings.ToLower(name),
		1,
	)
}

func (s *Statements) AddColumnSQL(name string) string {
	return strings.Replace(s.statements["addcolumn.sql"], "new_column", strings.ReplaceAll(name, ".", "_"), 1)
}

func (s *Statements) AddFieldsIndexSQL(fields []string) string {
	var fieldsToIndex string
	for _, f := range fields {
		if f != "" {
			fieldsToIndex += fmt.Sprintf(", %s", strings.ReplaceAll(f, ".", "_"))
		}
	}

	fieldsToIndex = strings.TrimPrefix(fieldsToIndex, ", ")

	if fieldsToIndex == "" {
		return ""
	}

	return strings.Replace(s.statements["addfieldsindex.sql"], "extra_fields", fieldsToIndex, 1)
}

func (s *Statements) DropFieldsIndexSQL() string { return s.statements["dropfieldsindex.sql"] }

func (s *Statements) InsertSQL() string { return s.statements["insert.sql"] }

// NotifySQL announces a write to this table on NotifyChannel, so that listeners in
// other processes can wake their watches instead of waiting for the next poll. Run
// it inside the writing transaction. Postgres holds notifications until the
// transaction commits, so a listener is never told about a row it cannot read yet.
//
// It is empty on sqlite, which is always a single process and has no other process
// to notify.
func (s *Statements) NotifySQL() string {
	if !s.postgres {
		return ""
	}
	return s.statements["notify.sql"]
}

func (s *Statements) TableMetaSQL() string { return s.statements["tablemeta.sql"] }

func (s *Statements) ClearCreatedSQL() string { return s.statements["clearcreated.sql"] }

func (s *Statements) UpdateCompactionSQL() string { return s.statements["updatecompaction.sql"] }

func (s *Statements) CompactSQL() string { return s.statements["compact.sql"] }

func (s *Statements) listSQL() string { return s.statements["list.sql"] }

func (s *Statements) listAfterSQL() string { return s.statements["listafter.sql"] }

func (s *Statements) TableLockSQL() string {
	if s.postgres {
		return s.statements["tablelock.sql"]
	}
	return ""
}
