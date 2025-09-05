package statements

import (
	"embed"
	_ "embed"
	"fmt"
	"strings"
)

//go:embed *.sql
var fs embed.FS

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

func (s *Statements) TableMetaSQL() string { return s.statements["tablemeta.sql"] }

func (s *Statements) ClearCreatedSQL() string { return s.statements["clearcreated.sql"] }

func (s *Statements) UpdateCompactionSQL() string { return s.statements["updatecompaction.sql"] }

func (s *Statements) CompactSQL() string { return s.statements["compact.sql"] }

func (s *Statements) listSQL() string { return s.statements["list.sql"] }

func (s *Statements) listAfterSQL() string { return s.statements["listafter.sql"] }

func (s *Statements) TableLockSQL() string {
	if s.lock {
		return s.statements["tablelock.sql"]
	}
	return ""
}
