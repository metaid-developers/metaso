# Production Nginx Hot Feed Cache

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
