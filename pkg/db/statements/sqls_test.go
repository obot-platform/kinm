package statements

import (
	"strings"
	"testing"
)

// The index created here is what keeps list.sql (and every Get built on it) off a
// full scan-and-sort of the table. Its shape has to stay in step with the window
// clause in list.sql: PARTITION BY name, namespace ORDER BY id DESC. If either the
// column order or the DESC drifts, Postgres silently stops using the index and
// falls back to sorting every row version, so assert the shape rather than trusting
// that a query still returns the right answer.
func TestAddLatestIndexSQL(t *testing.T) {
	for _, tableName := range []string{"lease", "mcpservercatalogentry"} {
		t.Run(tableName, func(t *testing.T) {
			got := New(tableName, nil, true).AddLatestIndexSQL()

			for _, want := range []string{
				// Unquoted, valid identifier: the table name is substituted quoted and
				// then unquoted again when followed by "_", so a regression in that
				// rewriting shows up here as idx_"lease"_latest, which will not parse.
				"CREATE INDEX IF NOT EXISTS idx_" + tableName + "_latest",
				`ON "` + tableName + `"`,
				"(name, namespace, id DESC)",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("AddLatestIndexSQL() = %q\nmissing %q", got, want)
				}
			}

			// Existing deployments already have the table, so migrate() runs this on
			// every start and it has to be idempotent.
			if !strings.Contains(got, "IF NOT EXISTS") {
				t.Errorf("AddLatestIndexSQL() must be idempotent, got %q", got)
			}

			// An all-ascending index cannot serve "ORDER BY name, namespace, id DESC";
			// the planner ignores it and the sort comes back.
			if strings.Contains(got, "id)") {
				t.Errorf("AddLatestIndexSQL() must order id DESC, got %q", got)
			}
		})
	}
}

// list.sql is what the index exists to serve. If this window clause changes, the
// index in addlatestindex.sql has to change with it.
func TestListSQLWindowClauseMatchesIndex(t *testing.T) {
	got := New("lease", nil, true).ListSQL(0)

	for _, want := range []string{"PARTITION BY name, namespace", "ORDER BY ID DESC"} {
		if !strings.Contains(got, want) {
			t.Errorf("list.sql no longer contains %q; addlatestindex.sql must be updated to match\ngot: %s", want, got)
		}
	}
}
