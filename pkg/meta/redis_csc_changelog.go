//go:build !noredis
// +build !noredis

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
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
	"fmt"
	"math"
	"strconv"
	"strings"
)

// With the changelog enabled on the volume, a cached directory listing that is invalidated is patched
// with the changelog since it was read, instead of reading the whole directory again. Only changes of
// single names are patched; a listing is dropped (and read again on next use) whenever the result is
// not certainly correct.

const maxPatchLog = 100000 // entries of the changelog read in one round

// opsWithoutEntries are changelog operations that do not change any directory entry.
var opsWithoutEntries = map[string]bool{
	"DELETESLICE": true, "INIT_ENABLE_DIRSTATS": true, "INIT_ENABLE_USERGROUPQUOTA": true, "NEWSESSION": true,
	"INCR_COUNTER": true, "SET": true, "TRUNCATE": true, "FALLOCATE": true, "SETATTR": true, "ACCESS": true,
	"CLEANSESSION": true, "DELSUSTAINED": true, "WRITE": true, "COPYFILERANGE": true, "DIRSTAT": true,
	"DELCHUNK": true, "CLEANUP_DELAYED_SLICES": true, "COMPACTCHUNK": true, "CLEANUP_TRASH_SLICES": true,
	"REPAIRDIR": true, "SETXATTR": true, "REMOVEXATTR": true, "SETQUOTA": true, "DELQUOTA": true, "CLEANUP": true,
	"SETFACL": true, "LOADDUMPEDACLS": true, "STORETOKEN": true, "UPDATETOKEN": true, "DELETETOKENS": true,
}

func (c *redisCache) patchable() bool {
	return c.refresh && c.changelog != nil && c.changelog()
}

// queuePatch keeps an invalidated listing to be patched. It must not block.
func (c *redisCache) queuePatch(parent Ino, d *cachedDir) {
	c.pendingMu.Lock()
	if _, ok := c.patching[parent]; !ok && len(c.patching) < c.cap {
		c.patching[parent] = d // an older listing replays more of the changelog, which is also correct
	}
	c.pendingMu.Unlock()
	c.signalRefresh()
}

func (c *redisCache) takePatching(n int) map[Ino]*cachedDir {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	dirs := make(map[Ino]*cachedDir)
	for p, d := range c.patching {
		if len(dirs) >= n {
			break
		}
		dirs[p] = d
		delete(c.patching, p)
	}
	return dirs
}

// dirPatch is a listing being patched.
type dirPatch struct {
	version int64
	entries []cachedDirEntry
	idx     map[string]int
	touched bool
	drop    bool
}

func newDirPatch(d *cachedDir) *dirPatch {
	x := &dirPatch{version: d.version, entries: make([]cachedDirEntry, len(d.entries)), idx: make(map[string]int, len(d.entries))}
	copy(x.entries, d.entries)
	for i, e := range x.entries {
		x.idx[e.name] = i
	}
	return x
}

func (x *dirPatch) set(name string, ino Ino, typ uint8) {
	x.touched = true
	if i, ok := x.idx[name]; ok {
		x.entries[i] = cachedDirEntry{name: name, ino: ino, typ: typ}
		return
	}
	x.idx[name] = len(x.entries)
	x.entries = append(x.entries, cachedDirEntry{name: name, ino: ino, typ: typ})
}

func (x *dirPatch) del(name string) {
	x.touched = true
	i, ok := x.idx[name]
	if !ok {
		return
	}
	last := len(x.entries) - 1
	if i != last {
		x.entries[i] = x.entries[last]
		x.idx[x.entries[i].name] = i
	}
	x.entries = x.entries[:last]
	delete(x.idx, name)
}

// patchDirs brings listings up to date with the changelog, all in one round trip.
func (c *redisCache) patchDirs(ctx context.Context, dirs map[Ino]*cachedDir) error {
	minVersion := int64(math.MaxInt64)
	limit := 64
	terms := make(map[Ino]uint64, len(dirs))
	for p, d := range dirs {
		minVersion = min(minVersion, d.version)
		// reading more of the changelog than the listings themselves costs more than reading them again
		limit += len(d.entries)
		terms[p] = c.entryTerm(p)
	}
	limit = min(limit, maxPatchLog)
	pipe := c.cli.TxPipeline()
	lastCmd := pipe.Get(ctx, c.prefix+"txnLastLog")
	logCmd := pipe.LRange(ctx, c.prefix+"txnLog", -int64(limit), -1)
	hlens := make(map[Ino]func() int64, len(dirs))
	for p := range dirs {
		hlens[p] = pipe.HLen(ctx, c.prefix+"d"+p.String()).Val
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	last, err := lastCmd.Int64()
	if err != nil {
		return err
	}
	logs := logCmd.Val()
	first := last - int64(len(logs)) + 1 // version of logs[0]
	if minVersion+1 < first {
		return nil // the changelog since then was trimmed, or is longer than the listings
	}

	patches := make(map[Ino]*dirPatch, len(dirs))
	for p, d := range dirs {
		patches[p] = newDirPatch(d)
	}
	for i := minVersion + 1 - first; i < int64(len(logs)); i++ {
		if err := applyLog(patches, first+i, logs[i]); err != nil {
			logger.Debugf("Redis client-side cache: drop %d listings: %s", len(dirs), err)
			return nil
		}
	}
	for p, x := range patches {
		// a directory is queued after an invalidation: no change for it in the changelog means a change
		// that was not logged, and a different number of entries means a change that was missed
		if x.drop || !x.touched || int64(len(x.entries)) != hlens[p]() {
			continue
		}
		d := &cachedDir{term: terms[p], version: last, entries: x.entries}
		if c.entryTerm(p) == d.term {
			c.storeDir(p, d)
		} else {
			c.queuePatch(p, d) // changed again meanwhile
		}
	}
	return nil
}

// applyLog applies one changelog entry of the given version to the listings that do not include it yet.
func applyLog(patches map[Ino]*dirPatch, version int64, entry string) error {
	op, args, result, err := parseLogEntry(entry)
	if err != nil {
		return err
	}
	get := func(p Ino) *dirPatch {
		if x, ok := patches[p]; ok && x.version < version {
			return x
		}
		return nil
	}
	drop := func(p Ino) {
		if x := get(p); x != nil {
			x.drop = true
		}
	}
	argIno := func(i int) (Ino, error) {
		if i >= len(args) {
			return 0, fmt.Errorf("short %s: %s", op, entry)
		}
		v, err := strconv.ParseUint(args[i], 10, 64)
		return Ino(v), err
	}
	argName := func(i int) (string, error) {
		if i >= len(args) {
			return "", fmt.Errorf("short %s: %s", op, entry)
		}
		return logDecode(args[i])
	}
	var errs []error
	ino := func(i int) Ino {
		v, err := argIno(i)
		errs = append(errs, err)
		return v
	}
	name := func(i int) string {
		v, err := argName(i)
		errs = append(errs, err)
		return v
	}
	check := func() error {
		for _, err := range errs {
			if err != nil {
				return fmt.Errorf("parse %s: %w", entry, err)
			}
		}
		return nil
	}

	switch op {
	case "CREATE": // parent, name, uid, gid, type, ...: inode
		parent, n, typ := ino(0), name(1), ino(4)
		res, err := strconv.ParseUint(result, 10, 64)
		errs = append(errs, err)
		if err := check(); err != nil {
			return err
		}
		if x := get(parent); x != nil {
			x.set(n, Ino(res), uint8(typ))
		}
	case "UNLINK", "RMDIR": // parent, name, trash, ...: inode
		parent, n, trash := ino(0), name(1), ino(2)
		if err := check(); err != nil {
			return err
		}
		if x := get(parent); x != nil {
			x.del(n)
		}
		drop(trash)
	case "MOVE": // parentSrc, nameSrc, parentDst, nameDst, flags, dino, trash, opened: inode
		ps, ns, pd, nd, flags, trash := ino(0), name(1), ino(2), name(3), ino(4), ino(6)
		res, err := strconv.ParseUint(result, 10, 64)
		errs = append(errs, err)
		if err := check(); err != nil {
			return err
		}
		drop(trash)
		if flags != 0 { // exchange, whiteout, ...
			drop(ps)
			drop(pd)
			return nil
		}
		var typ uint8
		var known bool
		if x := get(ps); x != nil {
			if i, ok := x.idx[ns]; ok {
				typ, known = x.entries[i].typ, true
			}
			x.del(ns)
		}
		if x := get(pd); x != nil {
			if known {
				x.set(nd, Ino(res), typ)
			} else {
				x.drop = true // the type is not in the changelog
			}
		}
	case "UNLINKBATCH": // parent, names..., trash, updateParent: results
		p := ino(0)
		if err := check(); err != nil {
			return err
		}
		drop(p)
		if len(args) >= 3 {
			if trash, err := strconv.ParseUint(args[len(args)-2], 10, 64); err == nil {
				drop(Ino(trash))
				return nil
			}
		}
		return fmt.Errorf("parse %s", entry)
	case "LINK", "CLONE", "ATTACH": // inode/srcIno/dstIno, parent, name, ...
		p := ino(1)
		if err := check(); err != nil {
			return err
		}
		drop(p)
	case "CLONEBATCH": // dstParent, ...
		p := ino(0)
		if err := check(); err != nil {
			return err
		}
		drop(p)
	default:
		if !opsWithoutEntries[op] {
			return fmt.Errorf("unknown operation %s", op)
		}
	}
	return nil
}

// parseLogEntry parses "sec.nsec|OP(args):result|(sid,txn)". Names in args are escaped by logEncode,
// so they contain no ',', '(' or ')', but they may contain '|'.
func parseLogEntry(entry string) (op string, args []string, result string, err error) {
	i, j := strings.IndexByte(entry, '|'), strings.LastIndexByte(entry, '|')
	if i < 0 || j <= i {
		return "", nil, "", fmt.Errorf("invalid changelog entry: %s", entry)
	}
	s := entry[i+1 : j]
	open, end := strings.IndexByte(s, '('), strings.LastIndexByte(s, ')')
	if open < 0 || end < open {
		return "", nil, "", fmt.Errorf("invalid changelog entry: %s", entry)
	}
	op = s[:open]
	args = strings.Split(s[open+1:end], ",")
	if rest := s[end+1:]; strings.HasPrefix(rest, ":") {
		result = rest[1:]
	}
	return
}

// logDecode reverses logEncode.
func logDecode(s string) (string, error) {
	if !strings.Contains(s, "%") {
		return s, nil
	}
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			b = append(b, s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", fmt.Errorf("invalid escape in %q", s)
		}
		v, err := strconv.ParseUint(s[i+1:i+3], 16, 8)
		if err != nil {
			return "", fmt.Errorf("invalid escape in %q", s)
		}
		b = append(b, byte(v))
		i += 2
	}
	return string(b), nil
}
