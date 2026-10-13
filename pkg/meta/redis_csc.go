//go:build !noredis
// +build !noredis

/*
 * JuiceFS, Copyright 2025 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/push"
)

// cachedEntry maps a name to an inode. Attributes of the inode are cached in inodeCache only,
// where they are invalidated on change.
type cachedEntry struct {
	ino  Ino
	term uint64
}

func (e *cachedEntry) isMark() bool {
	return e.ino == 0
}

type cachedDirEntry struct {
	name string
	ino  Ino
	typ  uint8
}

// recentNames are the names in a directory looked up recently, with the time of the last lookup.
type recentNames struct {
	sync.Mutex
	names map[string]time.Time
}

const maxRecentNames = 256 // per directory

type entryRef struct {
	parent Ino
	name   string
}

// cachedDir is a complete listing of a directory, valid only while term matches the entry term of the directory.
type cachedDir struct {
	term    uint64
	version int64 // version of the changelog the listing reflects, -1 if unknown
	entries []cachedDirEntry
}

// redisCache support bcast mode client-side cache
// cache attrs, entries and directory listings only, chunks are already cached in OpenCache
type redisCache struct {
	cli     *redis.Client // client whose commands are served from the cache
	tracker *redis.Client // dedicated client for the tracking connection
	prefix  string
	cap     int
	expiry  time.Duration
	preload int

	mu           sync.Mutex
	subscription *redis.PubSub
	done         chan struct{}
	wg           sync.WaitGroup

	// Liveness of the tracking connection: cached data is used only if a PING sent less than
	// stale() ago was answered, so that all invalidations sent before it have been applied.
	// The bound adapts to the round-trip times measured by PINGs, within [minStale, maxStale].
	pingInterval time.Duration
	minStale     time.Duration
	maxStale     time.Duration
	minDeadAfter time.Duration
	start        time.Time
	confirmed    atomic.Int64 // send time (since start) of the latest answered PING
	staleBound   atomic.Int64
	rtts         [30]time.Duration // recent round-trip times, only used by the tracking goroutine
	rttIdx       int

	// Refresh-ahead: attrs and names used within refreshWindow are fetched again in batches when invalidated,
	// so that the next use does not have to wait for Redis.
	refresh       bool
	refreshWindow time.Duration
	refreshDelay  time.Duration // wait for more invalidations to batch
	refreshBatch  int
	recentInodes  *expirable.LRU[Ino, struct{}]
	recentDirs    *expirable.LRU[Ino, *recentNames]
	pendingMu     sync.Mutex
	pendingInodes map[Ino]struct{}
	pendingNames  map[entryRef]struct{}
	refreshCh     chan struct{}
	changelog     func() bool        // whether the volume has the changelog enabled
	patching      map[Ino]*cachedDir // invalidated listings to patch with the changelog, guarded by pendingMu

	inodeCache *expirable.LRU[Ino, []byte]
	entryCache *expirable.LRU[string, *cachedEntry]
	entryTerms *expirable.LRU[Ino, uint64]
	dirCache   *expirable.LRU[Ino, *cachedDir]
	dirEntries atomic.Int64 // total entries held by dirCache, bounded by cap
}

func newRedisCache(prefix string, cap int, expiry time.Duration, preload int) *redisCache {
	logger.Infof("Initializing Redis client-side cache with size %d and expiry %+v", cap, expiry)
	c := &redisCache{
		prefix:       prefix,
		cap:          cap,
		expiry:       expiry,
		preload:      preload,
		done:         make(chan struct{}),
		pingInterval: time.Second,
		minStale:     3 * time.Second,
		maxStale:     30 * time.Second,
		minDeadAfter: 10 * time.Second,
		start:        time.Now(),

		refresh:       true,
		refreshWindow: 30 * time.Second,
		refreshDelay:  10 * time.Millisecond,
		refreshBatch:  1000,
		pendingInodes: make(map[Ino]struct{}),
		pendingNames:  make(map[entryRef]struct{}),
		patching:      make(map[Ino]*cachedDir),
		refreshCh:     make(chan struct{}, 1),
		inodeCache:    expirable.NewLRU[Ino, []byte](cap, nil, expiry),
		entryCache:    expirable.NewLRU[string, *cachedEntry](cap, nil, expiry),
		entryTerms:    expirable.NewLRU[Ino, uint64](0, nil, 10*expiry), // no size limit, cleanup only based on expiry
	}
	c.dirCache = expirable.NewLRU[Ino, *cachedDir](0, func(_ Ino, d *cachedDir) {
		c.dirEntries.Add(-int64(len(d.entries)))
	}, expiry)
	c.recentInodes = expirable.NewLRU[Ino, struct{}](cap, nil, c.refreshWindow)
	c.recentDirs = expirable.NewLRU[Ino, *recentNames](cap, nil, c.refreshWindow)
	c.staleBound.Store(int64(c.minStale))
	c.confirmed.Store(-int64(c.maxStale)) // not live until the first PING is answered
	return c
}

// live reports whether cached data can be used: all invalidations sent by Redis before a recent PING
// have been applied, so cached data is less than stale() behind. Writes of this client are applied immediately.
func (c *redisCache) live() bool {
	return time.Since(c.start)-time.Duration(c.confirmed.Load()) < c.stale()
}

func (c *redisCache) stale() time.Duration {
	return time.Duration(c.staleBound.Load())
}

// deadAfter is how long the tracking connection may receive nothing before it is reconnected.
func (c *redisCache) deadAfter() time.Duration {
	return max(c.minDeadAfter, 3*c.stale())
}

// observeRTT adapts the staleness bound to the latency of the tracking connection, so that PONGs
// delayed by a slow link do not make the cache unusable between them.
func (c *redisCache) observeRTT(rtt time.Duration) {
	c.rtts[c.rttIdx%len(c.rtts)] = rtt
	c.rttIdx++
	var maxRTT time.Duration
	for _, r := range c.rtts {
		maxRTT = max(maxRTT, r)
	}
	c.staleBound.Store(int64(min(max(c.minStale, c.pingInterval+2*maxRTT), c.maxStale)))
}

func (c *redisCache) confirm(sent int64) {
	for {
		old := c.confirmed.Load()
		if sent <= old || c.confirmed.CompareAndSwap(old, sent) {
			return
		}
	}
}

// fits reports whether a directory with n entries can be cached without evicting everything else.
func (c *redisCache) fits(n int) bool {
	return c != nil && n <= c.cap
}

// getDir returns the cached listing of parent if no change to it has been seen since it was cached.
func (c *redisCache) getDir(parent Ino) ([]cachedDirEntry, bool) {
	if !c.live() {
		return nil, false
	}
	d, ok := c.dirCache.Get(parent)
	if !ok || d.term != c.entryTerm(parent) {
		return nil, false
	}
	return d.entries, true
}

// addDir caches a complete listing of parent read under term. It is dropped if parent changed meanwhile.
func (c *redisCache) addDir(parent Ino, term uint64, entries []*Entry) {
	c.addDirAt(parent, term, -1, entries)
}

// addDirAt is addDir for a listing read after the given version of the changelog, so that it can be patched.
func (c *redisCache) addDirAt(parent Ino, term uint64, version int64, entries []*Entry) {
	if !c.fits(len(entries)) || c.entryTerm(parent) != term {
		return
	}
	d := &cachedDir{term: term, version: version, entries: make([]cachedDirEntry, len(entries))}
	for i, e := range entries {
		d.entries[i] = cachedDirEntry{name: string(e.Name), ino: e.Inode, typ: e.Attr.Typ}
	}
	c.storeDir(parent, d)
}

func (c *redisCache) storeDir(parent Ino, d *cachedDir) {
	if !c.fits(len(d.entries)) {
		return
	}
	c.dirCache.Remove(parent) // Add does not call onEvict when replacing
	c.dirCache.Add(parent, d)
	c.dirEntries.Add(int64(len(d.entries)))
	for c.dirEntries.Load() > int64(c.cap) {
		if _, _, ok := c.dirCache.RemoveOldest(); !ok {
			break
		}
	}
}

func (c *redisCache) invalidateDir(parent Ino) {
	c.bumpEntryTerm(parent)
	if d, ok := c.dirCache.Peek(parent); ok && d.version >= 0 && c.patchable() {
		c.queuePatch(parent, d)
	}
	c.dirCache.Remove(parent)
}

// getAttr returns the cached attr of inode. On a miss it leaves a mark so that the result
// of the following fetch can be cached by addAttr, unless an invalidation arrives in between.
func (c *redisCache) getAttr(inode Ino, mark bool) ([]byte, bool) {
	if data, ok := c.inodeCache.Get(inode); ok && len(data) > 0 && c.live() {
		c.touchInode(inode)
		return data, true
	}
	if mark {
		c.inodeCache.AddIf(inode, inodeMark, func(oldVal []byte, exists bool) bool {
			return !exists
		})
	}
	return nil, false
}

func (c *redisCache) addAttr(inode Ino, data []byte) {
	_, _ = c.inodeCache.AddIf(inode, data, func(oldVal []byte, exists bool) bool {
		return exists && len(oldVal) == 0
	})
}

func (c *redisCache) touchInode(inode Ino) {
	if c.refresh {
		c.recentInodes.Add(inode, struct{}{})
	}
}

func (c *redisCache) touchName(parent Ino, name string) {
	if !c.refresh {
		return
	}
	rn, ok := c.recentDirs.Get(parent)
	if !ok {
		rn = &recentNames{names: make(map[string]time.Time)}
	}
	c.recentDirs.Add(parent, rn) // extend its lifetime
	now := time.Now()
	rn.Lock()
	defer rn.Unlock()
	if _, ok := rn.names[name]; !ok && len(rn.names) >= maxRecentNames {
		for n, t := range rn.names {
			if now.Sub(t) > c.refreshWindow {
				delete(rn.names, n)
			}
		}
		if len(rn.names) >= maxRecentNames {
			return
		}
	}
	rn.names[name] = now
}

// queueInode and queueNames are called on invalidation, they must not block.
func (c *redisCache) queueInode(inode Ino) {
	if _, ok := c.recentInodes.Peek(inode); !ok {
		return
	}
	c.pendingMu.Lock()
	if len(c.pendingInodes) < c.cap {
		c.pendingInodes[inode] = struct{}{}
	}
	c.pendingMu.Unlock()
	c.signalRefresh()
}

func (c *redisCache) queueNames(parent Ino) {
	rn, ok := c.recentDirs.Peek(parent)
	if !ok {
		return
	}
	now := time.Now()
	rn.Lock()
	c.pendingMu.Lock()
	for name, t := range rn.names {
		if now.Sub(t) <= c.refreshWindow && len(c.pendingNames) < c.cap {
			c.pendingNames[entryRef{parent, name}] = struct{}{}
		}
	}
	c.pendingMu.Unlock()
	rn.Unlock()
	c.signalRefresh()
}

func (c *redisCache) signalRefresh() {
	select {
	case c.refreshCh <- struct{}{}:
	default:
	}
}

func (c *redisCache) takePending(n int) (inodes []Ino, names []entryRef) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for ino := range c.pendingInodes {
		if len(inodes) >= n {
			break
		}
		inodes = append(inodes, ino)
		delete(c.pendingInodes, ino)
	}
	for ref := range c.pendingNames {
		if len(inodes)+len(names) >= n {
			break
		}
		names = append(names, ref)
		delete(c.pendingNames, ref)
	}
	return
}

// refresher fetches invalidated attrs and names that were used recently, in batches.
func (c *redisCache) refresher() {
	defer c.wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-c.done
		cancel()
	}()
	for {
		select {
		case <-c.done:
			return
		case <-c.refreshCh:
		}
		select {
		case <-c.done:
			return
		case <-time.After(c.refreshDelay):
		}
		for {
			inodes, names := c.takePending(c.refreshBatch)
			dirs := c.takePatching(c.refreshBatch)
			if len(inodes)+len(names)+len(dirs) == 0 {
				break
			}
			if err := c.refreshAttrs(ctx, inodes); err != nil {
				logger.Debugf("Redis client-side cache: refresh attrs: %v", err)
			}
			if err := c.refreshNames(ctx, names); err != nil {
				logger.Debugf("Redis client-side cache: refresh names: %v", err)
			}
			if len(dirs) > 0 {
				if err := c.patchDirs(ctx, dirs); err != nil {
					logger.Debugf("Redis client-side cache: patch listings: %v", err)
				}
			}
		}
	}
}

func (c *redisCache) refreshAttrs(ctx context.Context, inodes []Ino) error {
	var keys []string
	var fetch []Ino
	for _, ino := range inodes {
		// the mark is removed by an invalidation during the fetch, so a stale result is not cached
		if _, ok := c.getAttr(ino, true); !ok {
			keys = append(keys, c.prefix+"i"+ino.String())
			fetch = append(fetch, ino)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	rs, err := c.cli.MGet(ctx, keys...).Result()
	if err != nil {
		return err
	}
	for i, r := range rs {
		if a, ok := r.(string); ok {
			c.addAttr(fetch[i], []byte(a))
		}
	}
	return nil
}

func (c *redisCache) refreshNames(ctx context.Context, names []entryRef) error {
	if len(names) == 0 {
		return nil
	}
	terms := make([]uint64, len(names))
	cmds := make([]*redis.StringCmd, len(names))
	pipe := c.cli.Pipeline()
	for i, ref := range names {
		// same as doLookup: entries read under an older term are not cached
		term := c.entryTerm(ref.parent)
		terms[i] = term
		c.entryCache.AddIf(c.entryName(ref.parent, ref.name), &cachedEntry{term: term}, func(oldEntry *cachedEntry, exists bool) bool {
			return !exists || oldEntry.term < term
		})
		cmds[i] = pipe.HGet(ctx, c.prefix+"d"+ref.parent.String(), ref.name)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	for i, ref := range names {
		buf, err := cmds[i].Bytes()
		if err != nil || len(buf) != 9 {
			continue // removed
		}
		ce := &cachedEntry{ino: Ino(binary.BigEndian.Uint64(buf[1:])), term: terms[i]}
		parent, term := ref.parent, terms[i]
		_, _ = c.entryCache.AddIf(c.entryName(ref.parent, ref.name), ce, func(oldEntry *cachedEntry, exists bool) bool {
			return exists && oldEntry.isMark() && oldEntry.term == term && c.entryTerm(parent) == term
		})
	}
	return nil
}

func (c *redisCache) init(cli redis.UniversalClient) error {
	ctx := context.Background()
	var err error
	if rc, ok := cli.(*redis.Client); ok {
		c.cli = rc
	} else if cc, ok := cli.(*redis.ClusterClient); ok {
		// For cluster mode, we should get the master node for our key
		if c.cli, err = cc.MasterForKey(ctx, c.prefix); err != nil {
			return err
		}
	}
	// A dedicated client, so that every connection it makes (including reconnects inside PubSub)
	// purges the cache and enables tracking.
	opt := *c.cli.Options()
	opt.OnConnect = c.onInvalidateConnect
	c.tracker = redis.NewClient(&opt)
	// handle PUSH notifications for invalidation in c.HandlePushNotification
	if err = c.tracker.RegisterPushNotificationHandler("invalidate", c, true); err != nil {
		_ = c.tracker.Close()
		return err
	}
	sub, err := c.subscribe(ctx)
	if err != nil {
		_ = c.tracker.Close()
		return err
	}
	c.subscription = sub
	// handle client cmd to avoid race conditions
	c.cli.AddHook(c)
	c.wg.Add(1)
	go c.track(sub)
	if c.refresh {
		c.wg.Add(1)
		go c.refresher()
	}
	return nil
}

// under the RESP3 protocol, "__redis__:invalidate" actually has no effect.
// we use Pubsub channel to simplify connection management and receiving PUSH messages.
func (c *redisCache) subscribe(ctx context.Context) (*redis.PubSub, error) {
	sub := c.tracker.Subscribe(ctx)
	if err := sub.Subscribe(ctx, "__redis__:invalidate"); err != nil {
		_ = sub.Close()
		return nil, err
	}
	return sub, nil
}

// track keeps the tracking connection alive, and confirms that invalidations are up to date by sending
// PINGs on it: a PONG is received only after all invalidations sent before it have been handled.
func (c *redisCache) track(sub *redis.PubSub) {
	defer c.wg.Done()
	ctx := context.Background()
	for {
		err := c.watch(ctx, sub)
		select {
		case <-c.done:
			return
		default:
		}
		logger.Warnf("Redis client-side cache: reconnect tracking connection: %v", err)
		_ = sub.Close()
		for {
			if sub, err = c.subscribe(ctx); err == nil {
				break
			}
			logger.Warnf("Redis client-side cache: subscribe: %v", err)
			select {
			case <-c.done:
				return
			case <-time.After(c.pingInterval):
			}
		}
		c.mu.Lock()
		closed := c.subscription == nil
		if !closed {
			c.subscription = sub
		}
		c.mu.Unlock()
		if closed {
			_ = sub.Close()
			return
		}
	}
}

// watch handles messages on the tracking connection until it is closed or considered dead.
func (c *redisCache) watch(ctx context.Context, sub *redis.PubSub) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(c.pingInterval)
		defer ticker.Stop()
		for {
			_ = sub.Ping(ctx, strconv.FormatInt(int64(time.Since(c.start)), 10))
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	for {
		// invalidations are handled by HandlePushNotification before the next message is returned
		msg, err := sub.ReceiveTimeout(ctx, c.deadAfter())
		if err != nil {
			return err
		}
		if pong, ok := msg.(*redis.Pong); ok {
			if sent, err := strconv.ParseInt(pong.Payload, 10, 64); err == nil {
				c.observeRTT(time.Since(c.start) - time.Duration(sent))
				c.confirm(sent)
			}
		}
	}
}

const (
	keyTypOther = iota
	keyTypInode
	keyTypEntry
)

func (c *redisCache) parse(key string) int {
	if strings.HasPrefix(key, c.prefix+"i") {
		return keyTypInode
	}
	if strings.HasPrefix(key, c.prefix+"d") {
		return keyTypEntry
	}
	return keyTypOther
}

func (c *redisCache) entryName(parent Ino, name string) string {
	return fmt.Sprintf("%d%d%s", parent, os.PathSeparator, name)
}

func (c *redisCache) entryTerm(parent Ino) uint64 {
	term, ok := c.entryTerms.Get(parent)
	if !ok {
		return 0
	}
	c.entryTerms.Add(parent, term)
	return term
}

func (c *redisCache) bumpEntryTerm(parent Ino) uint64 {
	term, _ := c.entryTerms.Get(parent)
	c.entryTerms.Add(parent, term+1)
	return term + 1
}

func (c *redisCache) HandlePushNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) != 2 || notification[0] == nil || notification[1] == nil {
		return nil
	}
	if typ, ok := notification[0].(string); !ok || typ != "invalidate" {
		return nil
	}
	iKeys, ok := notification[1].([]interface{})
	if !ok {
		return nil
	}
	var key string
	for _, iKey := range iKeys {
		key, ok = iKey.(string)
		if !ok {
			continue
		}
		typ := c.parse(key)
		switch typ {
		case keyTypInode:
			inodeStr := key[len(c.prefix)+1:]
			inode, err := strconv.ParseUint(inodeStr, 10, 64)
			if err == nil {
				c.inodeCache.Remove(Ino(inode))
				c.queueInode(Ino(inode))
			}
		case keyTypEntry:
			parentStr := key[len(c.prefix)+1:]
			parent, err := strconv.ParseUint(parentStr, 10, 64)
			if err == nil {
				c.invalidateDir(Ino(parent))
				c.queueNames(Ino(parent))
			}
		}
	}
	return nil
}

func (c *redisCache) DialHook(next redis.DialHook) redis.DialHook { return nil }

var inodeMark []byte

func (c *redisCache) beforeProcess(cmd redis.Cmder, skip bool) bool {
	name, args := cmd.Name(), cmd.Args()
	var key string
	var ok bool
	if len(args) < 2 {
		return true
	}
	if key, ok = args[1].(string); !ok {
		return true
	}
	typ := c.parse(key)

	if name == "get" && typ == keyTypInode {
		num, err := strconv.ParseUint(key[len(c.prefix)+1:], 10, 64)
		if err == nil {
			inode := Ino(num)
			c.touchInode(inode)
			if data, ok := c.inodeCache.Get(inode); ok {
				if !skip && len(data) > 0 && c.live() {
					rsp := cmd.(*redis.StringCmd)
					rsp.SetErr(nil)
					rsp.SetVal(bytesToString(data))
					return false
				}
			}
			c.inodeCache.AddIf(inode, inodeMark, func(oldVal []byte, exists bool) bool {
				return !exists
			})
			// request to Redis server
		}
	}
	return true
}

func (c *redisCache) afterProcess(cmd redis.Cmder) {
	name, args := cmd.Name(), cmd.Args()
	var key string
	var ok bool
	if len(args) < 2 {
		return
	}
	if key, ok = args[1].(string); !ok {
		return
	}
	typ := c.parse(key)

	switch name {
	case "get":
		if typ == keyTypInode {
			if data, err := cmd.(*redis.StringCmd).Bytes(); err == nil {
				num, err := strconv.ParseUint(key[len(c.prefix)+1:], 10, 64)
				if err != nil {
					return
				}
				_, _ = c.inodeCache.AddIf(Ino(num), data, func(oldVal []byte, exists bool) bool {
					return exists && len(oldVal) == 0
				})
			}
		}
	default:
		c.invalidate(cmd)
	}
}

// invalidate drops cached data touched by a write of this client, without waiting for
// the invalidation message from Redis. Dropping is always safe, so it is done even if the write failed.
func (c *redisCache) invalidate(cmd redis.Cmder) {
	args := cmd.Args()
	if len(args) < 2 {
		return
	}
	var keys []interface{}
	switch cmd.Name() {
	case "set", "setnx", "hset", "hsetnx", "hdel":
		keys = args[1:2]
	case "del", "unlink":
		keys = args[1:]
	default:
		return
	}
	for _, k := range keys {
		key, ok := k.(string)
		if !ok {
			continue
		}
		switch c.parse(key) {
		case keyTypInode:
			if num, err := strconv.ParseUint(key[len(c.prefix)+1:], 10, 64); err == nil {
				_ = c.inodeCache.Remove(Ino(num))
			}
		case keyTypEntry:
			parentStr := key[len(c.prefix)+1:]
			if num, err := strconv.ParseUint(parentStr, 10, 64); err == nil {
				c.invalidateDir(Ino(num))
			}
			switch cmd.Name() {
			case "hdel":
				for i := 2; i < len(args); i++ {
					_ = c.entryCache.Remove(fmt.Sprintf("%s%d%s", parentStr, os.PathSeparator, args[i]))
				}
			case "hset", "hsetnx":
				for i := 2; i < len(args); i += 2 {
					_ = c.entryCache.Remove(fmt.Sprintf("%s%d%s", parentStr, os.PathSeparator, args[i]))
				}
			}
		}
	}
}

// ProcessHook serves reads from the cache, except in transactions: a read after WATCH must see the
// latest value, otherwise a transaction based on a stale value would succeed.
func (c *redisCache) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	// go-redis builds a new chain of hooks for every transaction, so this is per transaction.
	var inTx atomic.Bool
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "watch" {
			inTx.Store(true)
		}
		if inTx.Load() {
			err := next(ctx, cmd)
			c.invalidate(cmd)
			return err
		}
		if !c.beforeProcess(cmd, false) {
			return nil
		}
		err := next(ctx, cmd)
		c.afterProcess(cmd)
		return err
	}
}

// ProcessPipelineHook only invalidates: reads in pipelines (including MULTI/EXEC) are not served from or added to the cache.
func (c *redisCache) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			c.invalidate(cmd)
		}
		return err
	}
}

func (c *redisCache) close() {
	c.mu.Lock()
	sub := c.subscription
	c.subscription = nil
	c.mu.Unlock()
	if sub == nil {
		return
	}
	close(c.done)
	_ = sub.Close() // may be closed already if track is reconnecting
	c.wg.Wait()
	if err := c.tracker.Close(); err != nil {
		logger.Warnf("failed closing Redis cache tracking client: %v", err)
	}
}

func (c *redisCache) onInvalidateConnect(ctx context.Context, cn *redis.Conn) error {
	// clear all caches on reconnect
	c.inodeCache.Purge()
	c.entryCache.Purge()
	c.dirCache.Purge()
	c.entryTerms.Purge()
	// use the pubsub connection to handle tracking and invalidate
	_ = cn.Do(ctx, "CLIENT", "TRACKING", "OFF").Err()
	if err := cn.Do(ctx, "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", c.prefix+"i", "PREFIX", c.prefix+"d").Err(); err != nil {
		logger.Warnf("Failed to enable Redis client-side caching on new connection: %v", err)
		return err
	}
	return nil
}

func (m *redisMeta) preloadCache() {
	if m.cache == nil {
		return
	}
	if m.cache.preload <= 0 {
		return
	}
	start := time.Now()
	ctx := Background()
	attr := &Attr{}
	if eno := m.doGetAttr(ctx, m.root, attr); eno != 0 {
		logger.Warnf("failed to get root inode %d attribute: %d", m.root, eno)
		return
	}

	term := m.cache.entryTerm(m.root)
	var entries []*Entry
	if eno := m.doReaddir(ctx, m.root, 1, &entries, m.cache.preload); eno != 0 {
		logger.Warnf("failed to read root %d directory: %d", m.root, eno)
		return
	}
	for _, entry := range entries {
		m.cache.entryCache.Add(m.cache.entryName(m.root, string(entry.Name)), &cachedEntry{
			ino:  entry.Inode,
			term: term,
		})
	}
	logger.Infof("preload %d inodes in %v", m.cache.inodeCache.Len(), time.Since(start))
}

func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
