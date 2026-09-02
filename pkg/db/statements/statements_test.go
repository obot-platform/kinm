package statements

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNotifySQL(t *testing.T) {
	// The channel is fixed and shared by every table; the payload says which table
	// was written. A listener has nothing else to go on.
	assert.Equal(t, "SELECT pg_notify('kinm', 'things')", New("things", nil, true).NotifySQL())
	assert.Equal(t, "kinm", NotifyChannel)

	// sqlite is one process and has no one to tell, and an empty statement is
	// skipped rather than run.
	assert.Empty(t, New("things", nil, false).NotifySQL())
}
