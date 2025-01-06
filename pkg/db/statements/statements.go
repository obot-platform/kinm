package statements

import (
	_ "embed"
	"fmt"
	"strconv"
	"strings"
)

type Statements struct {
	tableName  string
	statements map[string]string
	lock       bool
}

func New(tableName string, extraFieldNames []string, lock bool) *Statements {
	s := &Statements{
		tableName:  tableName,
		statements: map[string]string{},
		lock:       lock,
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		panic("failed to read sql files: " + err.Error())
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		sql, err := fs.ReadFile(entry.Name())
		if err != nil {
			panic("failed to read sql file: " + err.Error())
		}
		s.initSQL(entry.Name(), sql, extraFieldNames)
	}
	return s
}

func (s *Statements) initSQL(name string, sqlData []byte, extraFieldNames []string) {
	// This is hacky, sue me
	sql := strings.ReplaceAll(string(sqlData), "'placeholder'", fmt.Sprintf(`'%s'`, s.tableName))
	sql = strings.ReplaceAll(sql, "placeholder", fmt.Sprintf(`"%s"`, s.tableName))
	sql = strings.ReplaceAll(sql, fmt.Sprintf(`"%s"_`, s.tableName), fmt.Sprintf(`%s_`, s.tableName))

	// Add operation-specific transformations
	switch name {
	case "list.sql":
		sql = strings.Replace(sql, "extra_fields", extraFieldsWithIndexOffset(extraFieldNames, 5), 1)
	case "listafter.sql":
		sql = strings.Replace(sql, "extra_fields", extraFieldsWithIndexOffset(extraFieldNames, 4), 1)
	case "insert.sql":
		var extraFields, extraVals string
		for i, f := range extraFieldNames {
			extraFields += fmt.Sprintf(", %s", strings.ReplaceAll(f, ".", "_"))
			extraVals += fmt.Sprintf(",$%d", i+8)
		}
		sql = strings.Replace(strings.Replace(sql, "extra_fields", extraFields, 1), "extra_vals", extraVals, 1)
	}

	s.statements[name] = strings.TrimSpace(sql)
}

func (s *Statements) ListSQL(limit int64) string {
	if limit > 0 {
		return s.listSQL() + " LIMIT " + strconv.FormatInt(limit+1, 10)
	}
	return s.listSQL()
}

func (s *Statements) ListAfterSQL(limit int64) string {
	if limit > 0 {
		return s.listAfterSQL() + " LIMIT " + strconv.FormatInt(limit+1, 10)
	}
	return s.listAfterSQL()
}

func extraFieldsWithIndexOffset(extraFields []string, offset int) string {
	var extraFieldsStr string
	for i, f := range extraFields {
		extraFieldsStr += fmt.Sprintf(`
        AND (%s IS NULL OR %[1]s = $%d OR $%[2]d IS NULL)`, strings.ReplaceAll(f, ".", "_"), i+offset)
	}

	return extraFieldsStr
}
