package db

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// counter registers on a listener and counts the wake ups it is sent.
type counter struct {
	n          atomic.Int64
	unregister func()
}

func newCounter(t *testing.T, l *Listener, table string) *counter {
	t.Helper()

	c := &counter{}
	c.unregister = l.Register(table, func() { c.n.Add(1) })
	t.Cleanup(c.unregister)
	return c
}

func (c *counter) count() int64 { return c.n.Load() }

func TestListenerBroadcastsToRegisteredTables(t *testing.T) {
	l := NewListener("")
	things := newCounter(t, l, "things")
	others := newCounter(t, l, "others")

	l.notify("things")
	assert.Equal(t, int64(1), things.count())
	assert.Equal(t, int64(0), others.count())

	// A table nobody in this process serves is not an error. Another process may
	// hold types this one does not.
	l.notify("neverheardofit")

	l.broadcastAll()
	assert.Equal(t, int64(2), things.count())
	assert.Equal(t, int64(1), others.count())
}

func TestListenerBroadcastsToEveryRegistrationForATable(t *testing.T) {
	l := NewListener("")
	first := newCounter(t, l, "things")
	second := newCounter(t, l, "things")

	l.notify("things")
	assert.Equal(t, int64(1), first.count())
	assert.Equal(t, int64(1), second.count())
}

func TestListenerUnregister(t *testing.T) {
	l := NewListener("")
	kept := newCounter(t, l, "things")
	dropped := newCounter(t, l, "things")

	dropped.unregister()
	l.notify("things")
	assert.Equal(t, int64(1), kept.count())
	assert.Equal(t, int64(0), dropped.count())

	kept.unregister()
	l.notify("things")
	l.broadcastAll()
	assert.Equal(t, int64(1), kept.count())

	// The last registration going away takes the table with it.
	assert.Empty(t, l.tables)
	assert.Empty(t, l.timers)
}

func TestListenerCoalescesNotifications(t *testing.T) {
	// Two values, so the setting is known to be what decides the behavior.
	for _, debounce := range []time.Duration{time.Second, 100 * time.Millisecond} {
		t.Run(debounce.String(), func(t *testing.T) {
			original := notifyDebounce
			notifyDebounce = debounce
			t.Cleanup(func() { notifyDebounce = original })

			synctest.Test(t, func(t *testing.T) {
				l := NewListener("")
				things := newCounter(t, l, "things")

				for range 100 {
					l.notify("things")
				}

				// The first goes straight through.
				require.Equal(t, int64(1), things.count())

				// The other 99 collapse into one wake up, a debounce later.
				time.Sleep(debounce - time.Millisecond)
				synctest.Wait()
				require.Equal(t, int64(1), things.count())

				time.Sleep(time.Millisecond)
				synctest.Wait()
				require.Equal(t, int64(2), things.count())

				// And that is all of them.
				time.Sleep(10 * debounce)
				synctest.Wait()
				require.Equal(t, int64(2), things.count())
			})
		})
	}
}

func TestListenerNotifiesAgainAfterTheDebouncePasses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewListener("")
		things := newCounter(t, l, "things")

		for range 5 {
			l.notify("things")
			time.Sleep(2 * notifyDebounce)
			synctest.Wait()
		}

		// Spaced out writes are not held back at all.
		require.Equal(t, int64(5), things.count())
	})
}

func TestWatchPollDelay(t *testing.T) {
	s := &Strategy{}

	// No listener at all, as on sqlite: the poll is the only way to see another
	// process, so it stays where it has always been.
	assert.Equal(t, fallbackWatchPollInterval, s.watchPollDelay())

	l := NewListener("")
	s.listener = l
	assert.Equal(t, fallbackWatchPollInterval, s.watchPollDelay())

	l.connected.Store(true)
	for range 100 {
		delay := s.watchPollDelay()
		assert.GreaterOrEqual(t, delay, watchPollInterval)
		assert.Less(t, delay, watchPollInterval+watchPollInterval/4)
	}
}

// --- everything below needs a real Postgres ---

// testIsPostgres reports which backend the suite is running against.
func testIsPostgres() bool { return os.Getenv("KINM_TEST_DB") == "postgres" }

func postgresDSN(t *testing.T) string {
	t.Helper()

	if !testIsPostgres() {
		t.Skip("cross process notification is Postgres only; set KINM_TEST_DB=postgres to run this")
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
}

// startListener brings up a listener and waits for it to be connected, standing
// in for one process.
func startListener(t *testing.T, dsn string) *Listener {
	t.Helper()

	l := NewListener(dsn)
	l.Start()
	t.Cleanup(l.Close)

	require.Eventually(t, l.Connected, 10*time.Second, 10*time.Millisecond,
		"listener never connected")
	return l
}

// newTableStrategy builds a strategy on its own table and connection, standing in
// for one process's view of that table.
func newTableStrategy(t *testing.T, tableName string, listener *Listener) *Strategy {
	t.Helper()

	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(testGVK.GroupVersion(), &TestKind{}, &TestKindList{})

	sqlDB, postgres := newSQLDB(t)
	s, err := New(ctx, sqlDB, testGVK, scheme, tableName, postgres, listener)
	require.NoError(t, err)
	t.Cleanup(s.Destroy)
	return s
}

func dropTable(t *testing.T, tableName string) {
	t.Helper()

	sqlDB, _ := newSQLDB(t)
	_, err := sqlDB.Exec("DROP TABLE IF EXISTS " + tableName)
	require.NoError(t, err)
}

func newTestKind(name string) *TestKind {
	return &TestKind{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "testnamespace",
			UID:       ktypes.UID("uid-" + name),
		},
		Value: "value-" + name,
	}
}

// TestNotifyWakesAWatchInAnotherProcess covers what the listener is for: a write
// through one process's strategy reaching another process's watch without a poll.
func TestNotifyWakesAWatchInAnotherProcess(t *testing.T) {
	dsn := postgresDSN(t)
	dropTable(t, "notifytest")

	writer := newTableStrategy(t, "notifytest", startListener(t, dsn))
	reader := newTableStrategy(t, "notifytest", startListener(t, dsn))

	events, err := reader.Watch(t.Context(), "testnamespace", storage.ListOptions{})
	require.NoError(t, err)

	// Let the watch settle. Its listener is connected, so it will not list again
	// for a minute unless woken, and the write goes through a different strategy so
	// no in process broadcast can reach it.
	time.Sleep(time.Second)

	start := time.Now()
	_, err = writer.Create(t.Context(), newTestKind("one"))
	require.NoError(t, err)

	select {
	case event := <-events:
		assert.Equal(t, watch.Added, event.Type)
		assert.Equal(t, "one", event.Object.(kclient.Object).GetName())
		assert.Less(t, time.Since(start), watchPollInterval,
			"the watch could only have been woken by the poll, not the notification")
		t.Logf("watch in the other process woke in %v", time.Since(start))
	case <-time.After(10 * time.Second):
		t.Fatal("the write never reached the watch in the other process")
	}
}

// TestNotifyCarriesTheTableName checks a write announces its own table and not
// another, since the payload is all a listener has to go on.
func TestNotifyCarriesTheTableName(t *testing.T) {
	dsn := postgresDSN(t)
	dropTable(t, "payloadtest")
	dropTable(t, "payloadtestother")

	l := startListener(t, dsn)
	written := newCounter(t, l, "payloadtest")
	untouched := newCounter(t, l, "payloadtestother")

	s := newTableStrategy(t, "payloadtest", l)
	newTableStrategy(t, "payloadtestother", l)

	before := written.count()
	obj, err := s.Create(t.Context(), newTestKind("one"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return written.count() > before }, 10*time.Second, 10*time.Millisecond,
		"the create was never announced")

	// Update and delete announce too, being the same insert underneath.
	before = written.count()
	obj.(*TestKind).Value = "changed"
	obj, err = s.Update(t.Context(), obj)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return written.count() > before }, 10*time.Second, 10*time.Millisecond,
		"the update was never announced")

	before = written.count()
	_, err = s.Delete(t.Context(), obj)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return written.count() > before }, 10*time.Second, 10*time.Millisecond,
		"the delete was never announced")

	assert.Equal(t, int64(0), untouched.count(), "a table nobody wrote to was woken")
}

// TestListenerReconnects covers losing the connection, and with it every
// notification made while it was gone. The listener has to come back and tell
// every table to list again, since it cannot know what it missed.
func TestListenerReconnects(t *testing.T) {
	dsn := postgresDSN(t)
	dropTable(t, "reconnecttest")

	l := startListener(t, dsn)
	things := newCounter(t, l, "reconnecttest")
	s := newTableStrategy(t, "reconnecttest", l)

	events, err := s.Watch(t.Context(), "testnamespace", storage.ListOptions{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	before := things.count()
	killListenerConnections(t)

	// Two wake ups: one when the loss is noticed, putting watches back on the short
	// poll, and one on reconnect to cover the gap.
	require.Eventually(t, func() bool { return things.count() >= before+2 }, 30*time.Second, 50*time.Millisecond,
		"the listener did not wake its tables across the reconnect")
	require.Eventually(t, l.Connected, 30*time.Second, 50*time.Millisecond,
		"the listener never reconnected")

	// A write made around the outage still reaches the watch.
	_, err = s.Create(t.Context(), newTestKind("survivor"))
	require.NoError(t, err)

	select {
	case event := <-events:
		assert.Equal(t, watch.Added, event.Type)
		assert.Equal(t, "survivor", event.Object.(kclient.Object).GetName())
	case <-time.After(30 * time.Second):
		t.Fatal("a write around the outage never reached the watch")
	}
}

// killListenerConnections terminates the listening backends, which is what a
// database restart or a dropped connection looks like from here.
func killListenerConnections(t *testing.T) {
	t.Helper()

	sqlDB, _ := newSQLDB(t)
	var killed int
	err := sqlDB.QueryRow(`
		SELECT count(pg_terminate_backend(pid))
		FROM pg_stat_activity
		WHERE application_name = $1 AND pid <> pg_backend_pid()`,
		listenerAppName).Scan(&killed)
	require.NoError(t, err)
	require.NotZero(t, killed, "no listening connection to terminate")
	t.Logf("terminated %d listening connection(s)", killed)
}

// TestNotifyUnderSustainedWrites is the case the debounce exists for: what a
// continuously written table costs every other process.
//
// Two rates, because a watch behaves differently either side of its own drain
// speed. Written faster than it can keep up, it never parks and lists
// continuously, so the debounce has nothing to bound. The rate that exercises the
// debounce is the one a watch can keep up with.
func TestNotifyUnderSustainedWrites(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause time.Duration
	}{
		{"as fast as it will go", 0},
		{"20 writes per second", 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := postgresDSN(t)
			table := "stormtest"
			dropTable(t, table)

			writer := newTableStrategy(t, table, startListener(t, dsn))

			observerListener := startListener(t, dsn)
			observer := newTableStrategy(t, table, observerListener)
			wakes := newCounter(t, observerListener, table)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			events, err := observer.Watch(ctx, "testnamespace", storage.ListOptions{})
			require.NoError(t, err)

			// Drain the way a real consumer does, so the watch parks between changes
			// rather than blocking on an unread channel.
			var (
				seenLock sync.Mutex
				seen     = map[string]bool{}
			)
			go func() {
				for event := range events {
					if event.Type == watch.Added {
						seenLock.Lock()
						seen[event.Object.(kclient.Object).GetName()] = true
						seenLock.Unlock()
					}
				}
			}()

			// Settle onto the long poll, so anything counted came from a
			// notification.
			time.Sleep(time.Second)

			before := wakes.count()
			start := time.Now()

			var written int
			for time.Since(start) < 6*time.Second {
				_, err = writer.Create(ctx, newTestKind(fmt.Sprintf("storm-%d", written)))
				require.NoError(t, err)
				written++
				if tc.pause > 0 {
					time.Sleep(tc.pause)
				}
			}
			elapsed := time.Since(start)

			// Give the last debounce window time to close.
			time.Sleep(2 * notifyDebounce)
			woke := wakes.count() - before

			// One per debounce window, plus the leading edge and the closing one.
			ceiling := int64(elapsed/notifyDebounce) + 3

			writeRate := float64(written) / elapsed.Seconds()
			t.Logf("%d writes in %v is %.0f/s, and woke the other process %d times, which is %.2f/s against a ceiling of %d",
				written, elapsed.Round(time.Millisecond), writeRate, woke, float64(woke)/elapsed.Seconds(), ceiling)
			t.Logf("polling every 2s would have woken it 0.50/s, and one wake up per write would have been %.0f/s", writeRate)

			require.Greater(t, written, 100, "the storm was too slow to prove anything")
			require.LessOrEqual(t, woke, ceiling, "the debounce did not bound the wake ups")
			require.Less(t, woke, int64(written)/10, "wake ups tracked the write rate instead of the debounce")

			// Combining wake ups must not combine away changes. A watch lists
			// everything after its resource version, so one wake up carries them all.
			require.Eventually(t, func() bool {
				seenLock.Lock()
				defer seenLock.Unlock()
				return len(seen) == written
			}, 60*time.Second, 100*time.Millisecond, "not every write reached the other process")
			t.Logf("all %d writes arrived at the other process", written)
		})
	}
}

// TestPostgresStatementsDoNotDependOnPoolSize guards the dialect choice.
// KINM_DB_MAX_CONNECTIONS=1 is a legitimate setting, and a Postgres pool holding
// one connection is indistinguishable from sqlite by pool size alone.
func TestPostgresStatementsDoNotDependOnPoolSize(t *testing.T) {
	postgresDSN(t)

	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(testGVK.GroupVersion(), &TestKind{}, &TestKindList{})

	sqlDB, postgres := newSQLDB(t)
	require.True(t, postgres)
	sqlDB.SetMaxOpenConns(1)

	_, err := sqlDB.Exec("DROP TABLE IF EXISTS poolsizetest")
	require.NoError(t, err)

	s, err := New(ctx, sqlDB, testGVK, scheme, "poolsizetest", postgres, nil)
	require.NoError(t, err)
	t.Cleanup(s.Destroy)

	assert.NotEmpty(t, s.db.stmt.NotifySQL(), "a write would announce nothing")
	assert.NotEmpty(t, s.db.stmt.TableLockSQL(), "a write would not take the table lock")
}
