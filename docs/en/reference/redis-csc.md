# Redis Client-Side Caching Support in JuiceFS

Starting with version 6.0, Redis provides [Client-Side Caching](https://redis.io/docs/latest/develop/reference/client-side-caching) which allows clients to maintain local caches of data in a faster and more efficient way. JuiceFS includes full support for this feature, offering significant performance improvements for metadata operations.

## How it works

Redis Client-Side Caching (CSC) works by:

1. The client enables tracking mode with `CLIENT TRACKING ON BCAST`
2. The client caches data locally after reading it from Redis
3. Redis notifies the client when cached keys are modified by any client
4. The client invalidates those keys in its local cache

This results in reduced network traffic, lower latency, and higher throughput.

## Configuration

JuiceFS supports Redis CSC through the following options in the metadata URL:

```shell
--meta-url="redis://localhost/1?client-cache=true" # Enable client-side caching (always BCAST mode) 
--meta-url="redis://localhost/1?client-cache=true&client-cache-size=500" # Set cache size (default 12800) 
--meta-url="redis://localhost/1?client-cache=true&client-cache-expire=60s" # Set cache expiration (default: 60s)
```

### Options

- `client-cache`: Enables client-side caching in BCAST mode (set to any value except "false")
- `client-cache-size`: Maximum cache size (default: 12800)
- `client-cache-expire`: Cache expiration time (default: 60s)
- `client-cache-preload`: Number of file objects under the root directory preloaded after mounting. (default: 0)
- `client-cache-refresh`: Refresh recently used attributes and names when they are changed by other clients (default: true, set to `false` to disable)

When client-side caching is enabled, JuiceFS caches:

1. **Inode attributes**: File/directory metadata like permissions, size, timestamps
2. **Directory entries**: Name to inode mappings for faster lookups
3. **Directory listings**: Complete listings of directories, so that repeated `ls` / `readdir` calls don't go to Redis. Together with cached attributes, `ls -l` on a cached directory needs no round trip at all. Directories with more entries than `client-cache-size` are not cached

> **Note:** Redis Client Side Cache requires Redis server version 6.0 or higher. Using this feature with older Redis versions will result in errors.

### Preloading Cache

When client-side caching is enabled and `client-cache-preload` is set, JuiceFS will preload the file-object attributes and entries under the root directory after mounting. This lazy preloading happens in the background and helps to:

1. Warm up the cache for common operations
2. Reduce latency for initial file system operations
3. Provide better performance from the moment the file system is mounted

The preloading process intelligently prioritizes the most important inodes by:

1. Starting with the root directory
2. Loading the most frequently accessed top-level directories and files
3. Recursively exploring important subdirectories

The preloading process runs in a background goroutine with fail-safe mechanisms and won't block or affect normal file system operations.

## Modes

JuiceFS uses BCAST mode for simplicity and reliability:

- **BCAST mode**: All keys accessed by the client are tracked and notifications are sent for any changes.

BCAST mode provides the simplest implementation while ensuring cache coherence across all clients.

## Consistency

Cached data is kept up to date by invalidation messages, not by expiration:

- Changes made by the client itself are applied to its cache immediately.
- Changes made by other clients are applied when their invalidation messages arrive.
- The client sends a `PING` on the tracking connection every second. Since its reply arrives only after all earlier invalidation messages, cached data is used only while a recently sent `PING` has been answered; otherwise requests go to Redis. "Recently" adapts to the latency measured by these `PING`s: 1 second plus twice the largest recent round-trip time, at least 3 and at most 30 seconds. A tracking connection that receives nothing for 3 times that long (at least 10 seconds) is reconnected, and the cache is cleared on every reconnect.

So cached data is never more than a few seconds behind (up to 30 seconds on very slow links), even if the connection to Redis is lost, and `client-cache-expire` only bounds memory usage of rarely used entries. It can be set much longer (e.g. `10m`) when Redis is far away.

### Refresh-ahead

An invalidation message only names the changed key, so the next use of it has to wait for Redis. To avoid that, attributes read and names looked up within the last 30 seconds are fetched again in the background when they are invalidated, batched into a single `MGET` (attributes) and a single pipeline of `HGET` (names) every 10 milliseconds. Directory listings are not refreshed ahead, since a change of one entry requires reading the whole directory again, unless the changelog is enabled on the volume (`juicefs config META-URL --changelog`): then a changed listing is patched with the changelog since it was read, in one round trip for all changed directories. Only creating, removing and renaming single entries is patched; other changes, or a patched listing whose size differs from the directory in Redis, drop the listing so that it is read again on next use. Disable it with `client-cache-refresh=false` if the extra reads matter more than the latency, e.g. for many clients sharing frequently changed files.

## Requirements

- Redis server version 6.0 or higher
- JuiceFS with CSC support enabled

## Performance Considerations

1. The default 12800 cache size should be sufficient for most workloads (12800 entries each for attr and entry caches, and 12800 entries in total for directory listings)
2. For very large filesystems with millions of files, you may benefit from increasing the cache size
3. The cache is most effective for metadata-heavy workloads with many repeated operations
4. For very write-heavy workloads, consider disabling CSC as invalidation traffic may offset benefits

## Troubleshooting

If you experience crashes or instability with CSC enabled:

1. Update to the latest JuiceFS version which contains important fixes for CSC
2. Try reducing the cache size with `client-cache-size`
3. Check Redis server logs for any memory or client tracking issues
4. Make sure your Redis server version is 6.0 or higher
5. If problems persist, disable CSC by removing the `client-cache` parameter

JuiceFS includes robust error handling for various Redis CSC-specific responses to ensure stable operation even when Redis sends unexpected response formats due to client tracking.

## References

- [Redis Client-Side Caching Documentation](https://redis.io/docs/latest/develop/reference/client-side-caching)
