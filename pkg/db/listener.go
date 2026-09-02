package db

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/obot-platform/kinm/pkg/db/statements"
	"k8s.io/klog/v2"
)

var (
	// listenerKeepalive is how long the listener waits for a notification before
	// checking that its connection is still alive. A dead connection and a quiet
	// one look the same from here, so the listener has to ask.
	listenerKeepalive = time.Minute

	// listenerProbeTimeout is how long a new connection has to prove that a
	// notification sent on it comes back on it.
	listenerProbeTimeout = 10 * time.Second

	// listenerPingTimeout bounds the keepalive check.
	listenerPingTimeout = 10 * time.Second

	listenerMinBackoff = time.Second
	listenerMaxBackoff = 30 * time.Second
)

// listenerAppName marks the listening connection in pg_stat_activity, so that
// anyone looking at the database can tell what the connection is for.
const listenerAppName = "kinm-listener"

// Listener carries change notifications between processes over a Postgres
// LISTEN/NOTIFY channel.
//
// Every kinm write announces its table on statements.NotifyChannel from inside
// the writing transaction. One dedicated connection per process listens on that
// channel and calls the broadcast function each Strategy registered, which is the
// same wake up the in process write path already uses. A process therefore learns
// about another process's write as soon as it commits, instead of the next time
// its watch polls.
//
// The connection has to be its own, because a connection from the database/sql
// pool cannot stay blocked waiting for a notification.
type Listener struct {
	dsn string

	lock   sync.Mutex
	tables map[string][]*registration
	timers map[string]*debounce

	connected atomic.Bool
	cancel    context.CancelFunc
	done      chan struct{}
}

type registration struct {
	broadcast func()
}

type debounce struct {
	last  time.Time
	timer *time.Timer
}

// NewListener returns a listener for dsn. It does nothing until Start is called.
func NewListener(dsn string) *Listener {
	return &Listener{
		dsn:    dsn,
		tables: map[string][]*registration{},
		timers: map[string]*debounce{},
	}
}

// Start opens the listening connection in the background and keeps it open until
// ctx is done or Close is called. It does not wait for the connection. Until one is
// up, Connected reports false and watches stay on their short poll.
// Calling it a second time does nothing.
func (l *Listener) Start(ctx context.Context) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.done != nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	l.done = make(chan struct{})

	go func() {
		defer close(l.done)
		l.run(ctx)
	}()
}

// Close stops the listener and waits for its connection to be released.
func (l *Listener) Close() {
	// Read the two under the lock and let go of it again before waiting. The
	// listener goroutine takes the same lock to deliver a notification, so holding
	// it here until that goroutine exits would be a deadlock.
	l.lock.Lock()
	cancel, done := l.cancel, l.done
	l.lock.Unlock()

	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// Connected reports whether notifications are being delivered right now. When
// they are not, polling is the only way a watch can see another process's writes,
// so it uses the short interval.
func (l *Listener) Connected() bool {
	return l.connected.Load()
}

// Register asks for broadcast to be called whenever another process writes to
// table. The returned function unregisters it.
func (l *Listener) Register(table string, broadcast func()) func() {
	reg := &registration{broadcast: broadcast}

	l.lock.Lock()
	l.tables[table] = append(l.tables[table], reg)
	l.lock.Unlock()

	return func() {
		l.lock.Lock()
		defer l.lock.Unlock()

		if i := slices.Index(l.tables[table], reg); i >= 0 {
			l.tables[table] = slices.Delete(l.tables[table], i, i+1)
		}
		if len(l.tables[table]) == 0 {
			delete(l.tables, table)
			if d := l.timers[table]; d != nil && d.timer != nil {
				d.timer.Stop()
			}
			delete(l.timers, table)
		}
	}
}

func (l *Listener) run(ctx context.Context) {
	// Once this loop ends nothing is listening, so watches have to go back to
	// polling to see other processes.
	defer l.connected.Store(false)

	backoff := listenerMinBackoff
	var reported bool

	for ctx.Err() == nil {
		err := l.listen(ctx)
		if ctx.Err() != nil {
			return
		}

		if l.connected.Swap(false) {
			// The connection was up, so watches are waiting on the long poll and
			// every notification from here on is lost. Wake them so they list once
			// and then go back to the short poll.
			klog.Errorf("kinm change listener lost its connection, watches are polling again: %v", err)
			l.broadcastAll()
			backoff, reported = listenerMinBackoff, true
		} else if !reported {
			klog.Errorf("kinm change listener could not connect, watches will poll until it does: %v", err)
			reported = true
		} else {
			klog.V(4).Infof("kinm change listener still cannot connect, retrying in %v: %v", backoff, err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, listenerMaxBackoff)
	}
}

// listen holds one connection for as long as it lasts, and returns the error
// that ended it.
func (l *Listener) listen(ctx context.Context) error {
	config, err := pgx.ParseConfig(l.dsn)
	if err != nil {
		return err
	}
	config.RuntimeParams["application_name"] = listenerAppName

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// LISTEN takes no bind parameters. The channel name is a constant rather than
	// user input, so there is nothing to bind.
	if _, err = conn.Exec(ctx, "LISTEN "+statements.NotifyChannel); err != nil {
		return err
	}

	if err = l.probe(ctx, conn); err != nil {
		return fmt.Errorf("notification probe failed: %w", err)
	}

	klog.Infof("kinm change listener connected, watches poll every %v as a safety net", watchPollInterval)
	l.connected.Store(true)

	// Postgres only delivers a notification to a connection that was already
	// listening when the write committed, so any write made before now, or while a
	// previous connection was down, was missed. One broadcast per table costs one
	// list each and covers all of them.
	l.broadcastAll()

	for {
		waitCtx, cancel := context.WithTimeout(ctx, listenerKeepalive)
		n, err := conn.WaitForNotification(waitCtx)
		cancel()

		switch {
		case err == nil:
			l.notify(n.Payload)
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(waitCtx.Err(), context.DeadlineExceeded):
			// A timed out wait does not break the connection, so check it. The
			// check needs its own deadline, since a connection dropping packets
			// rather than refusing them would block here for as long as TCP takes
			// to give up, which is the state this check exists to catch. Any
			// failure ends the connection rather than reusing one whose reply may
			// still be in flight.
			pingCtx, cancelPing := context.WithTimeout(ctx, listenerPingTimeout)
			err = conn.Ping(pingCtx)
			cancelPing()
			if err != nil {
				return err
			}
		default:
			return err
		}
	}
}

// probe checks that a notification sent on this connection comes back on it.
// LISTEN succeeding is not enough on its own, because a connection pooler in
// between can accept the statement and then never deliver anything. Without the
// check a setup like that would look healthy and every watch would sit on the long
// poll, which is the one outcome worse than not making this change at all.
func (l *Listener) probe(ctx context.Context, conn *pgx.Conn) error {
	payload := "probe." + strconv.FormatUint(rand.Uint64(), 36)
	if _, err := conn.Exec(ctx, "SELECT pg_notify($1, $2)", statements.NotifyChannel, payload); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, listenerProbeTimeout)
	defer cancel()

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if n.Payload == payload {
			return nil
		}
		// Another process wrote something while the probe was in flight. It is a
		// real notification and it still counts.
		l.notify(n.Payload)
	}
}

// notify wakes table's watchers, at most once every notifyDebounce.
//
// The first notification after a quiet moment goes through immediately, so a
// change made in another process is seen right away. The ones behind it collapse
// into a single later wake up, which stops a burst of writes in one process from
// making every other process list once per write. Combining notifications is
// always safe, because a broadcast only asks a watch to list again.
func (l *Listener) notify(table string) {
	l.lock.Lock()
	if _, ok := l.tables[table]; !ok {
		// Another process wrote a table this one does not serve.
		l.lock.Unlock()
		return
	}

	d := l.timers[table]
	if d == nil {
		d = &debounce{}
		l.timers[table] = d
	}

	if wait := notifyDebounce - time.Since(d.last); wait > 0 {
		if d.timer == nil {
			d.timer = time.AfterFunc(wait, func() { l.fire(table) })
		}
		l.lock.Unlock()
		return
	}

	d.last = time.Now()
	l.lock.Unlock()
	l.broadcast(table)
}

// fire delivers the wake up that notify held back.
func (l *Listener) fire(table string) {
	l.lock.Lock()
	d := l.timers[table]
	if d == nil {
		l.lock.Unlock()
		return
	}
	d.timer, d.last = nil, time.Now()
	l.lock.Unlock()

	l.broadcast(table)
}

// broadcast calls every registration for table with the lock released, so a
// registration is free to take locks of its own.
func (l *Listener) broadcast(table string) {
	l.lock.Lock()
	regs := slices.Clone(l.tables[table])
	l.lock.Unlock()

	for _, reg := range regs {
		reg.broadcast()
	}
}

// broadcastAll wakes every registered table once. The listener calls it when the
// connection is lost or regained, because it cannot know what it missed and has to
// assume everything changed.
func (l *Listener) broadcastAll() {
	l.lock.Lock()
	tables := slices.Collect(maps.Keys(l.tables))
	l.lock.Unlock()

	for _, table := range tables {
		l.broadcast(table)
	}
}
