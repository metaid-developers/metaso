# Production Nginx Feed Cache

## 2026-05-25

The `www.show.now` gateway on `47.52.110.56` runs Nginx inside the `nginx` Docker container. The active config is mounted from `/mnt/nginx`.

Changes applied to the `show.now` / `www.show.now` HTTPS server block:

- Added a 3 second Nginx micro-cache for `GET` and `HEAD` requests under `/man/social/buzz/hot`.
- Added the `X-ShowNow-Cache` response header for cache visibility.
- Added `application/json` to the gzip types for this server block.
- Kept cache keys scoped to the full scheme, method, host, and URI.
- Bypassed cache when an `Authorization` header is present.

The cache zone is declared in `/mnt/nginx/nginx.conf`:

```nginx
proxy_cache_path /var/cache/nginx/show_now_cache levels=1:2 keys_zone=show_now_cache:10m max_size=128m inactive=30s use_temp_path=off;
```

Backup created before the successful reload:

```text
/mnt/nginx/backups/metaso-20260525-164742
```

Verification:

- `docker exec nginx nginx -t` passed before reload.
- `Content-Encoding: gzip` is present for the hot feed.
- Repeated requests return `X-ShowNow-Cache: HIT`.
- `size=30` hot feed response size dropped from about `89 KB` uncompressed to about `17.7 KB` compressed.
- Gateway access logs show cache-hit requests with `request_time` around `0.000s` and no upstream response time.

## 2026-05-25 Recommended Feed First Page

The recommended feed is the default home feed. Recent gateway logs showed the
backend response time was already low, usually around `0.01s` to `0.08s`, so the
remaining user-visible latency is mostly public network, TLS, and transfer time.
A conservative cache was still added to reduce duplicate first-page bursts.

Changes applied to the `show.now` / `www.show.now` HTTPS server block:

- Added a 1 second Nginx micro-cache for `/man/social/buzz/recommended`.
- Cached only first-page requests where `lastId` is empty.
- Bypassed cache for cursor pages where `lastId` is non-empty.
- Kept cache keys scoped to the full scheme, method, host, and URI, including
  `userAddress`.
- Bypassed cache when an `Authorization` header is present.
- Added the `X-ShowNow-Cache` response header for cache visibility.

The first-page-only bypass map is declared in `/mnt/nginx/nginx.conf`:

```nginx
map $arg_lastId $show_now_recommended_cache_bypass {
    default 1;
    "" 0;
}
```

Backup created before the successful reload:

```text
/mnt/nginx/backups/metaso-20260525-191055
```

Verification:

- `docker exec nginx nginx -t` passed before reload.
- First-page recommended requests returned `X-ShowNow-Cache: MISS`, then `HIT`
  within the 1 second TTL.
- Cursor-page recommended requests returned `X-ShowNow-Cache: BYPASS`.
- `Content-Encoding: gzip` remained present.
