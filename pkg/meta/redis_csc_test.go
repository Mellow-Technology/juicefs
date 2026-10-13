//go:build !noredis

package meta

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/push"
	"github.com/stretchr/testify/require"
)

func mockRedisCSCMeta(t *testing.T) *redisMeta {
	m, err := newRedisMeta("redis", "127.0.0.1:6379/10?client-cache=true", testConfig())
	require.NoError(t, err, "failed to create redis meta")
	require.Equal(t, "redis", m.Name(), "meta name should be redis")
	return m.(*redisMeta)
}

func TestRedisCache(t *testing.T) {
	ctx := context.Background()
	m := mockRedisCSCMeta(t)
	_ = m.rdb.FlushAll(ctx)
	defer m.Shutdown()
	defer m.cache.close()

	var err error
	t.Run("invalidation handling", func(t *testing.T) {
		cache := m.cache
		ino := Ino(100)
		attr := &Attr{Typ: TypeFile, Mode: 0644}
		cache.inodeCache.Add(ino, attr.Marshal())
		if _, ok := cache.inodeCache.Get(ino); !ok {
			t.Fatal("inode should be in cache")
		}

		err = m.rdb.Set(ctx, m.inodeKey(ino), m.marshal(&Attr{Mode: 0755}), 0).Err()
		require.NoError(t, err, "failed to set key %d", ino)
		dumIno := Ino(101)
		err = m.rdb.Set(ctx, m.inodeKey(dumIno), m.marshal(&Attr{Mode: 0755}), 0).Err()
		require.NoError(t, err, "failed to set key %d", dumIno)
		time.Sleep(3 * time.Second)
		if _, ok := cache.inodeCache.Get(Ino(100)); ok {
			t.Fatal("inode should be invalidated and removed from cache")
		}

		cache.entryCache.Add(cache.entryName(101, "file"), &cachedEntry{})
		m.rdb.HSet(ctx, m.entryKey(100), "file", "content").Err()
	})
	t.Run("invalidations resume after the tracking connection reconnects", func(t *testing.T) {
		other := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 10})
		defer other.Close()
		ino := Ino(280)
		require.NoError(t, other.Set(ctx, m.inodeKey(ino), m.marshal(&Attr{Typ: TypeFile}), 0).Err())
		require.NoError(t, other.Do(ctx, "CLIENT", "KILL", "TYPE", "pubsub").Err())
		require.Eventually(t, func() bool {
			// cache the attr, then change it from another client
			m.cache.inodeCache.Remove(ino)
			if err := m.rdb.Get(ctx, m.inodeKey(ino)).Err(); err != nil {
				return false
			}
			if _, ok := m.cache.inodeCache.Get(ino); !ok {
				return false
			}
			if err := other.Set(ctx, m.inodeKey(ino), m.marshal(&Attr{Typ: TypeFile}), 0).Err(); err != nil {
				return false
			}
			time.Sleep(200 * time.Millisecond)
			_, ok := m.cache.inodeCache.Get(ino)
			return !ok
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("cache expiration", func(t *testing.T) {
		shortExpiry := 50 * time.Millisecond
		cache := newRedisCache("jfs", 1000, shortExpiry, 0)
		attr := &Attr{Typ: TypeFile, Mode: 0644}
		cache.inodeCache.Add(Ino(102), attr.Marshal())
		time.Sleep(3 * shortExpiry)
		if _, ok := cache.inodeCache.Get(Ino(102)); ok {
			t.Fatal("inode should be expired")
		}
	})

	t.Run("inode hook", func(t *testing.T) {
		cache := m.cache
		ino := Ino(103)
		attr := &Attr{Typ: TypeFile, Length: 10}
		cache.inodeCache.Add(ino, attr.Marshal())

		data, err := m.rdb.Get(ctx, m.inodeKey(ino)).Bytes()
		require.NoError(t, err, "failed to get inode")
		attr2 := &Attr{}
		attr2.Unmarshal(data)
		attr2.Full = false
		require.Equal(t, *attr, *attr2)

		attr3 := &Attr{Typ: TypeFile, Length: 20}
		err = m.rdb.Set(ctx, m.inodeKey(ino), attr3.Marshal(), 0).Err()
		require.NoError(t, err)
		_, ok := cache.inodeCache.Get(ino)
		require.False(t, ok)
	})

	t.Run("cached lookup returns current attrs", func(t *testing.T) {
		other := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 10})
		defer other.Close()
		parent, ino := Ino(300), Ino(301)
		require.NoError(t, other.Set(ctx, m.inodeKey(ino), m.marshal(&Attr{Typ: TypeFile, Mode: 0644, Length: 301}), 0).Err())
		require.NoError(t, other.HSet(ctx, m.entryKey(parent), "f", m.packEntry(TypeFile, ino)).Err())
		require.Eventually(t, func() bool { return m.cache.entryTerm(parent) > 0 }, 3*time.Second, 10*time.Millisecond)
		var found Ino
		var attr Attr
		require.Equal(t, syscall.Errno(0), m.doLookup(Background(), parent, "f", &found, &attr))
		require.Equal(t, uint64(301), attr.Length)
		_, ok := m.cache.entryCache.Get(m.cache.entryName(parent, "f"))
		require.True(t, ok)

		// another client changes the file but not the directory
		require.NoError(t, other.Set(ctx, m.inodeKey(ino), m.marshal(&Attr{Typ: TypeFile, Mode: 0600, Length: 1}), 0).Err())
		require.Eventually(t, func() bool {
			return m.doLookup(Background(), parent, "f", &found, &attr) == 0 && attr.Length == 1
		}, 3*time.Second, 10*time.Millisecond)
		require.Equal(t, ino, found)
		require.Equal(t, uint16(0600), attr.Mode)
	})

	t.Run("entry hook", func(t *testing.T) {
		cache := m.cache
		ino := Ino(104)
		gen := cache.entryTerm(ino)
		name1, name2 := cache.entryName(ino, "f1"), cache.entryName(ino, "f2")
		cache.entryCache.Add(name1, &cachedEntry{term: gen})
		cache.entryCache.Add(name2, &cachedEntry{term: gen})

		err := m.rdb.HSet(ctx, m.entryKey(ino), "f1", "c1", "f2", "c2").Err()
		require.NoError(t, err)

		_, ok := cache.entryCache.Get(name1)
		require.False(t, ok)
		_, ok = cache.entryCache.Get(name2)
		require.False(t, ok)

		cache.entryCache.Add(name1, &cachedEntry{term: gen})
		cache.entryCache.Add(name2, &cachedEntry{term: gen})
		err = m.rdb.HDel(ctx, m.entryKey(ino), "f1", "f2").Err()
		require.NoError(t, err)

		_, ok = cache.entryCache.Get(name1)
		require.False(t, ok)
		_, ok = cache.entryCache.Get(name2)
		require.False(t, ok)
	})

	t.Run("reads in transactions bypass the cache", func(t *testing.T) {
		ino := Ino(340)
		fresh, stale := m.marshal(&Attr{Typ: TypeFile, Length: 2}), m.marshal(&Attr{Typ: TypeFile, Length: 1})
		require.NoError(t, m.rdb.Set(ctx, m.inodeKey(ino), fresh, 0).Err())
		time.Sleep(100 * time.Millisecond) // let the invalidation pass
		// an invalidation that has not arrived yet
		m.cache.inodeCache.Add(ino, stale)
		v, err := m.rdb.Get(ctx, m.inodeKey(ino)).Bytes()
		require.NoError(t, err)
		require.Equal(t, stale, v)

		err = m.rdb.Watch(ctx, func(tx *redis.Tx) error {
			v, err := tx.Get(ctx, m.inodeKey(ino)).Bytes()
			require.NoError(t, err)
			require.Equal(t, fresh, v)
			return nil
		}, m.inodeKey(ino))
		require.NoError(t, err)
	})

	t.Run("entry generation invalidation", func(t *testing.T) {
		cache := m.cache
		parent := Ino(105)
		name := cache.entryName(parent, "f")
		gen := cache.entryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{ino: 106, term: gen})

		err := cache.HandlePushNotification(ctx, push.NotificationHandlerContext{}, []interface{}{"invalidate", []interface{}{m.entryKey(parent)}})
		require.NoError(t, err)
		require.Equal(t, gen+1, cache.entryTerm(parent))

		entry, ok := cache.entryCache.Get(name)
		require.True(t, ok)
		require.NotEqual(t, cache.entryTerm(parent), entry.term)
	})

	t.Run("entry term expires after idle", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 10*time.Millisecond, 0)
		parent := Ino(106)

		require.Equal(t, uint64(1), cache.bumpEntryTerm(parent))
		time.Sleep(11 * cache.expiry)
		require.Equal(t, uint64(0), cache.entryTerm(parent))
	})

	t.Run("entry term refreshes on access", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 10*time.Millisecond, 0)
		parent := Ino(107)

		require.Equal(t, uint64(1), cache.bumpEntryTerm(parent))
		time.Sleep(6 * cache.expiry)
		require.Equal(t, uint64(1), cache.entryTerm(parent))
		time.Sleep(6 * cache.expiry)
		require.Equal(t, uint64(1), cache.entryTerm(parent))
	})

	t.Run("entry mark refill requires current generation", func(t *testing.T) {
		cache := m.cache
		parent := Ino(107)
		name := cache.entryName(parent, "f")
		gen := cache.entryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{term: gen})

		cache.bumpEntryTerm(parent)
		added, _ := cache.entryCache.AddIf(name, &cachedEntry{ino: 108, term: gen}, func(oldEntry *cachedEntry, exists bool) bool {
			return exists && oldEntry.isMark() && oldEntry.term == gen && cache.entryTerm(parent) == gen
		})
		require.False(t, added)

		entry, ok := cache.entryCache.Get(name)
		require.True(t, ok)
		require.True(t, entry.isMark())
	})

	t.Run("entry mark refill requires existing mark", func(t *testing.T) {
		cache := m.cache
		parent := Ino(109)
		name := cache.entryName(parent, "f")
		gen := cache.entryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{term: gen})
		cache.entryCache.Remove(name)

		added, _ := cache.entryCache.AddIf(name, &cachedEntry{ino: 110, term: gen}, func(oldEntry *cachedEntry, exists bool) bool {
			return exists && oldEntry.isMark() && oldEntry.term == gen && cache.entryTerm(parent) == gen
		})
		require.False(t, added)
		_, ok := cache.entryCache.Get(name)
		require.False(t, ok)
	})

	t.Run("stale entry is not used while entry term is active", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 20*time.Millisecond, 0)
		parent := Ino(110)
		name := cache.entryName(parent, "f")
		cache.entryCache.Add(name, &cachedEntry{ino: 111, term: cache.entryTerm(parent)})

		cache.bumpEntryTerm(parent)
		entry, ok := cache.entryCache.Get(name)
		require.True(t, ok)
		require.False(t, entry.term >= cache.entryTerm(parent) && !entry.isMark())
	})

	t.Run("old entry expires before idle entry term", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 10*time.Millisecond, 0)
		parent := Ino(112)
		name := cache.entryName(parent, "f")
		cache.entryCache.Add(name, &cachedEntry{ino: 113, term: cache.entryTerm(parent)})
		cache.bumpEntryTerm(parent)

		time.Sleep(2 * cache.expiry)
		_, ok := cache.entryCache.Get(name)
		require.False(t, ok)
		require.Equal(t, uint64(1), cache.entryTerm(parent))
	})

	t.Run("active entry term outlives repeated stale entry checks", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 10*time.Millisecond, 0)
		parent := Ino(114)
		name := cache.entryName(parent, "f")
		cache.entryCache.Add(name, &cachedEntry{ino: 115, term: cache.entryTerm(parent)})
		cache.bumpEntryTerm(parent)

		for range 3 {
			time.Sleep(4 * cache.expiry)
			entry, ok := cache.entryCache.Get(name)
			if ok {
				require.False(t, entry.term >= cache.entryTerm(parent) && !entry.isMark())
			} else {
				require.Equal(t, uint64(1), cache.entryTerm(parent))
			}
		}
		require.Equal(t, uint64(1), cache.entryTerm(parent))
	})

	t.Run("concurrent stale refills cannot overwrite newer mark", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, time.Second, 0)
		parent := Ino(116)
		name := cache.entryName(parent, "f")
		oldTerm := cache.entryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{term: oldTerm})
		newTerm := cache.bumpEntryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{term: newTerm})

		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(ino Ino) {
				defer wg.Done()
				cache.entryCache.AddIf(name, &cachedEntry{ino: ino, term: oldTerm}, func(oldEntry *cachedEntry, exists bool) bool {
					return exists && oldEntry.isMark() && oldEntry.term == oldTerm && cache.entryTerm(parent) == oldTerm
				})
			}(Ino(117 + i))
		}
		wg.Wait()

		entry, ok := cache.entryCache.Get(name)
		require.True(t, ok)
		require.True(t, entry.isMark())
		require.Equal(t, newTerm, entry.term)
	})

	// other writes as a different client, so they reach m only through invalidation messages
	other := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 10})
	defer other.Close()
	names := func(es []*Entry) []string {
		var ns []string
		for _, e := range es {
			ns = append(ns, string(e.Name))
		}
		return ns
	}
	mkdir := func(parent Ino, children map[string]Ino) {
		for name, ino := range children {
			require.NoError(t, other.HSet(ctx, m.entryKey(parent), name, m.packEntry(TypeFile, ino)).Err())
			require.NoError(t, other.Set(ctx, m.inodeKey(ino), m.marshal(&Attr{Typ: TypeFile, Mode: 0644, Length: uint64(ino)}), 0).Err())
		}
		require.Eventually(t, func() bool { return m.cache.entryTerm(parent) > 0 }, 3*time.Second, 10*time.Millisecond)
	}

	t.Run("dir listing is cached and invalidated by other clients", func(t *testing.T) {
		parent := Ino(200)
		mkdir(parent, map[string]Ino{"a": 201, "b": 202})

		var entries []*Entry
		require.Equal(t, syscall.Errno(0), m.doReaddir(Background(), parent, 0, &entries, -1))
		require.ElementsMatch(t, []string{"a", "b"}, names(entries))
		_, ok := m.cache.getDir(parent)
		require.True(t, ok)

		// served from cache, and callers cannot modify the cached listing
		entries[0].Name[0] = 'x'
		entries = nil
		require.Equal(t, syscall.Errno(0), m.doReaddir(Background(), parent, 0, &entries, -1))
		require.ElementsMatch(t, []string{"a", "b"}, names(entries))

		require.NoError(t, other.HSet(ctx, m.entryKey(parent), "c", m.packEntry(TypeFile, 203)).Err())
		require.Eventually(t, func() bool {
			_, ok := m.cache.getDir(parent)
			return !ok
		}, 3*time.Second, 10*time.Millisecond)
		entries = nil
		require.Equal(t, syscall.Errno(0), m.doReaddir(Background(), parent, 0, &entries, -1))
		require.ElementsMatch(t, []string{"a", "b", "c"}, names(entries))
	})

	t.Run("dir listing is invalidated synchronously by own transactions", func(t *testing.T) {
		parent := Ino(210)
		mkdir(parent, map[string]Ino{"a": 211})
		h := m.newDirHandler(parent, false, nil)
		entries, st := h.List(Background(), 0)
		require.Equal(t, syscall.Errno(0), st)
		require.ElementsMatch(t, []string{"a"}, names(entries))
		_, ok := m.cache.getDir(parent)
		require.True(t, ok)

		err := m.rdb.Watch(ctx, func(tx *redis.Tx) error {
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.HDel(ctx, m.entryKey(parent), "a")
				pipe.HSet(ctx, m.entryKey(parent), "b", m.packEntry(TypeFile, 212))
				return nil
			})
			return err
		}, m.entryKey(parent))
		require.NoError(t, err)
		// no waiting for the invalidation message
		_, ok = m.cache.getDir(parent)
		require.False(t, ok)
		entries, st = m.newDirHandler(parent, false, nil).List(Background(), 0)
		require.Equal(t, syscall.Errno(0), st)
		require.ElementsMatch(t, []string{"b"}, names(entries))
	})

	t.Run("pipeline hook invalidates writes without waiting for redis", func(t *testing.T) {
		cache := newRedisCache("", 100, time.Second, 0)
		hook := cache.ProcessPipelineHook(func(ctx context.Context, cmds []redis.Cmder) error { return nil })
		ls := []*Entry{{Inode: 271, Name: []byte("a"), Attr: &Attr{Typ: TypeFile}}}
		cache.addDir(270, 0, ls)
		cache.addDir(272, 0, ls)
		cache.inodeCache.Add(271, []byte("attr"))

		require.NoError(t, hook(ctx, []redis.Cmder{
			redis.NewIntCmd(ctx, "hset", m.entryKey(270), "b", "x"),
			redis.NewStatusCmd(ctx, "set", m.inodeKey(271), "attr2"),
		}))
		_, ok := cache.getDir(270)
		require.False(t, ok)
		_, ok = cache.getAttr(271, false)
		require.False(t, ok)
		_, ok = cache.getDir(272)
		require.True(t, ok)

		require.NoError(t, hook(ctx, []redis.Cmder{redis.NewIntCmd(ctx, "del", m.inodeKey(273), m.entryKey(272))}))
		_, ok = cache.getDir(272)
		require.False(t, ok)
	})

	t.Run("readdir plus caches attrs", func(t *testing.T) {
		parent := Ino(220)
		mkdir(parent, map[string]Ino{"a": 221, "b": 222})
		var entries []*Entry
		require.Equal(t, syscall.Errno(0), m.doReaddir(Background(), parent, 1, &entries, -1))
		for _, e := range entries {
			require.Equal(t, uint64(e.Inode), e.Attr.Length)
			_, ok := m.cache.getAttr(e.Inode, false)
			require.True(t, ok)
		}

		require.NoError(t, other.Set(ctx, m.inodeKey(221), m.marshal(&Attr{Typ: TypeFile, Mode: 0644, Length: 1}), 0).Err())
		require.Eventually(t, func() bool {
			entries = nil
			require.Equal(t, syscall.Errno(0), m.doReaddir(Background(), parent, 1, &entries, -1))
			for _, e := range entries {
				if e.Inode == 221 {
					return e.Attr.Length == 1
				}
			}
			return false
		}, 3*time.Second, 10*time.Millisecond)
	})

	t.Run("partial listing is not cached", func(t *testing.T) {
		parent := Ino(230)
		mkdir(parent, map[string]Ino{"a": 231, "b": 232})
		var entries []*Entry
		require.Equal(t, syscall.Errno(0), m.doReaddir(Background(), parent, 0, &entries, 1))
		require.Len(t, entries, 1)
		_, ok := m.cache.getDir(parent)
		require.False(t, ok)
	})

	t.Run("dir listing read before a change is not cached", func(t *testing.T) {
		cache := newRedisCache("jfs", 100, time.Second, 0)
		parent := Ino(240)
		term := cache.entryTerm(parent)
		cache.invalidateDir(parent)
		cache.addDir(parent, term, []*Entry{{Inode: 241, Name: []byte("a"), Attr: &Attr{Typ: TypeFile}}})
		_, ok := cache.getDir(parent)
		require.False(t, ok)
	})

	t.Run("dir cache is bounded by total entries", func(t *testing.T) {
		cache := newRedisCache("jfs", 10, time.Second, 0)
		list := func(n int) []*Entry {
			es := make([]*Entry, n)
			for i := range es {
				es[i] = &Entry{Inode: Ino(300 + i), Name: []byte{byte('a' + i)}, Attr: &Attr{Typ: TypeFile}}
			}
			return es
		}
		cache.addDir(250, 0, list(6))
		cache.addDir(251, 0, list(6))
		_, ok := cache.getDir(250)
		require.False(t, ok)
		_, ok = cache.getDir(251)
		require.True(t, ok)
		require.Equal(t, int64(6), cache.dirEntries.Load())

		cache.addDir(251, 0, list(4)) // replace
		require.Equal(t, int64(4), cache.dirEntries.Load())
		cache.addDir(252, 0, list(11))
		_, ok = cache.getDir(252)
		require.False(t, ok)
		cache.invalidateDir(251)
		require.Equal(t, int64(0), cache.dirEntries.Load())
	})

	t.Run("dir cache is purged on reconnect", func(t *testing.T) {
		// a listing cached before any invalidation has term 0, which entry terms restart from after a reconnect
		parent := Ino(260)
		require.Equal(t, uint64(0), m.cache.entryTerm(parent))
		m.cache.addDir(parent, 0, []*Entry{{Inode: 261, Name: []byte("a"), Attr: &Attr{Typ: TypeFile}}})
		_, ok := m.cache.getDir(parent)
		require.True(t, ok)

		conn := m.cache.cli.Conn()
		defer conn.Close()
		require.NoError(t, m.cache.onInvalidateConnect(ctx, conn))
		_, ok = m.cache.getDir(parent)
		require.False(t, ok)
	})
}
