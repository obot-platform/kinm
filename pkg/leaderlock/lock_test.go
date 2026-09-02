package leaderlock

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "lock.db"))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// record builds a record the way client-go does on acquire. Times are truncated to
// seconds because metav1.Time serializes at that precision.
func record(holder string) resourcelock.LeaderElectionRecord {
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	return resourcelock.LeaderElectionRecord{
		HolderIdentity:       holder,
		LeaseDurationSeconds: 60,
		AcquireTime:          now,
		RenewTime:            now,
	}
}

func TestNewRequiresNameAndIdentity(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)

	_, err := New(ctx, db, "", "a")
	require.Error(t, err)

	_, err = New(ctx, db, "election", "")
	require.Error(t, err)

	// Creating the table twice must be fine: every replica calls New on startup.
	_, err = New(ctx, db, "election", "a")
	require.NoError(t, err)
	_, err = New(ctx, db, "election", "b")
	require.NoError(t, err)
}

func TestGetBeforeCreateIsNotFound(t *testing.T) {
	ctx := context.Background()
	l, err := New(ctx, newDB(t), "election", "a")
	require.NoError(t, err)

	_, _, err = l.Get(ctx)
	// client-go only calls Create after a NotFound, so the error type matters.
	assert.True(t, apierrors.IsNotFound(err), "expected NotFound, got %v", err)
}

func TestCreateThenGetRoundTrips(t *testing.T) {
	ctx := context.Background()
	l, err := New(ctx, newDB(t), "election", "a")
	require.NoError(t, err)

	want := record("a")
	require.NoError(t, l.Create(ctx, want))

	got, raw, err := l.Get(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, raw)
	assert.Equal(t, want.HolderIdentity, got.HolderIdentity)
	assert.Equal(t, want.LeaseDurationSeconds, got.LeaseDurationSeconds)
	assert.True(t, want.RenewTime.Equal(&got.RenewTime))
	assert.True(t, want.AcquireTime.Equal(&got.AcquireTime))
}

func TestCreateTwiceIsAlreadyExists(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	a, err := New(ctx, db, "election", "a")
	require.NoError(t, err)
	b, err := New(ctx, db, "election", "b")
	require.NoError(t, err)

	require.NoError(t, a.Create(ctx, record("a")))

	err = b.Create(ctx, record("b"))
	assert.True(t, apierrors.IsAlreadyExists(err), "expected AlreadyExists, got %v", err)

	// The loser must not have clobbered the winner.
	got, _, err := b.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, "a", got.HolderIdentity)
}

func TestUpdateBeforeGetOrCreateRefuses(t *testing.T) {
	ctx := context.Background()
	l, err := New(ctx, newDB(t), "election", "a")
	require.NoError(t, err)

	// Without an observed version there is nothing to check against, so an
	// Update here could silently overwrite another holder. Refuse instead.
	require.Error(t, l.Update(ctx, record("a")))
}

func TestUpdateAdvancesRecord(t *testing.T) {
	ctx := context.Background()
	l, err := New(ctx, newDB(t), "election", "a")
	require.NoError(t, err)
	require.NoError(t, l.Create(ctx, record("a")))

	renewed := record("a")
	renewed.RenewTime = metav1.NewTime(renewed.RenewTime.Add(2 * time.Second))
	require.NoError(t, l.Update(ctx, renewed))

	// A holder renewing repeatedly, as the leader does every RetryPeriod, must keep
	// succeeding against its own writes.
	renewed.RenewTime = metav1.NewTime(renewed.RenewTime.Add(2 * time.Second))
	require.NoError(t, l.Update(ctx, renewed))

	got, _, err := l.Get(ctx)
	require.NoError(t, err)
	assert.True(t, renewed.RenewTime.Equal(&got.RenewTime))
}

// This is the property the lock exists to provide: two replicas cannot both win.
func TestStaleUpdateConflicts(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	a, err := New(ctx, db, "election", "a")
	require.NoError(t, err)
	b, err := New(ctx, db, "election", "b")
	require.NoError(t, err)

	// a becomes leader; b observes a's record.
	require.NoError(t, a.Create(ctx, record("a")))
	got, _, err := b.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, "a", got.HolderIdentity)

	// a renews, moving the row past what b observed.
	renewed := record("a")
	renewed.RenewTime = metav1.NewTime(renewed.RenewTime.Add(2 * time.Second))
	require.NoError(t, a.Update(ctx, renewed))

	// b, acting on its stale view, tries to take over. It must not succeed.
	err = b.Update(ctx, record("b"))
	assert.True(t, apierrors.IsConflict(err), "expected Conflict, got %v", err)

	// After re-reading, as client-go's slow path does, b sees that a still holds
	// the lock, and only then can it write. This is the path a follower takes once
	// the leader's lease has actually expired. All verification below reads through
	// b on purpose: a Get refreshes the caller's observed version, and the point of
	// the final assertion is that a has NOT re-read since its own renew.
	got, _, err = b.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, "a", got.HolderIdentity, "a must still hold the lock")
	require.NoError(t, b.Update(ctx, record("b")))

	got, _, err = b.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, "b", got.HolderIdentity)

	// a last wrote at version 2 and has not looked since; the row is now at 3.
	err = a.Update(ctx, renewed)
	assert.True(t, apierrors.IsConflict(err), "expected Conflict, got %v", err)
}

func TestElectionsAreIndependent(t *testing.T) {
	ctx := context.Background()
	db := newDB(t)
	x, err := New(ctx, db, "election-x", "a")
	require.NoError(t, err)
	y, err := New(ctx, db, "election-y", "a")
	require.NoError(t, err)

	require.NoError(t, x.Create(ctx, record("a")))

	_, _, err = y.Get(ctx)
	assert.True(t, apierrors.IsNotFound(err), "a different election must not see this lock")
	require.NoError(t, y.Create(ctx, record("a")))
}

func TestIdentityAndDescribe(t *testing.T) {
	l, err := New(context.Background(), newDB(t), "obot-controller", "pod-1")
	require.NoError(t, err)
	assert.Equal(t, "pod-1", l.Identity())
	assert.Equal(t, "leader_lock/obot-controller", l.Describe())
	// resourcelock.Interface compliance is checked at compile time here.
	var _ resourcelock.Interface = l
}
