# Buzz Recommended and Hot Feed Performance Baseline

Date: 2026-05-25 08:37:13 CST

Environment:
- Production API host: `https://www.show.now`
- Client location: local workstation
- Tooling: `curl --compressed`
- Pagination method: sequentially request the next page using `data.lastId` from the previous response.

## Recommended Feed

Start URL:

```text
https://www.show.now/man/social/buzz/recommended?size=10&lastId=&userAddress=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ
```

Summary:

| Metric | Value |
| --- | ---: |
| Pages sampled | 10 |
| Average | 4.208 s |
| Average excluding first page | 2.588 s |
| Min | 1.373 s |
| P50 | 2.376 s |
| P90 | 4.378 s |
| Max | 18.793 s |

Raw samples:

| Page | HTTP | Time Total | Size | Items | Next LastId |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 200 | 18.792981 s | 25693 B | 9 | `6a136cb5eba543f2e48b0b47` |
| 2 | 200 | 3.587256 s | 41922 B | 9 | `6a136794eba543f2e48b0aeb` |
| 3 | 200 | 4.377811 s | 36234 B | 9 | `6a135cc5eba543f2e48b0a4f` |
| 4 | 200 | 3.053798 s | 30924 B | 9 | `6a1358a2eba543f2e48b0a1f` |
| 5 | 200 | 2.704469 s | 37145 B | 9 | `6a135563eba543f2e48b09f1` |
| 6 | 200 | 1.967529 s | 28849 B | 9 | `6a1353f7eba543f2e48b09d4` |
| 7 | 200 | 2.180284 s | 29532 B | 9 | `6a13518deba543f2e48b09b5` |
| 8 | 200 | 2.375520 s | 34028 B | 9 | `6a13518deba543f2e48b09ad` |
| 9 | 200 | 1.373193 s | 37058 B | 9 | `6a134df9eba543f2e48b0981` |
| 10 | 200 | 1.670340 s | 37073 B | 9 | `6a134c58eba543f2e48b0951` |

## Hot Feed

Start URL:

```text
https://www.show.now/man/social/buzz/hot?size=10&lastId=6a1313d648c8b483df360d89
```

Summary for non-empty pages:

| Metric | Value |
| --- | ---: |
| Non-empty pages sampled | 6 |
| Average | 0.490 s |
| Average excluding first page | 0.417 s |
| Min | 0.305 s |
| P50 | 0.477 s |
| P90 | 0.855 s |
| Max | 0.855 s |

The seventh request returned an empty list, so there were only 6 non-empty pages available from the provided cursor during this run.

Raw samples:

| Page | HTTP | Time Total | Size | Items | Next LastId |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 200 | 0.854826 s | 37765 B | 10 | `6a124f1e48c8b483df3600a3` |
| 2 | 200 | 0.477215 s | 19576 B | 6 | `6a124eea48c8b483df360047` |
| 3 | 200 | 0.480632 s | 9829 B | 4 | `6a124e4048c8b483df360042` |
| 4 | 200 | 0.342444 s | 6048 B | 3 | `6a124e1448c8b483df360040` |
| 5 | 200 | 0.478759 s | 4018 B | 2 | `6a124d4448c8b483df36003c` |
| 6 | 200 | 0.304712 s | 2069 B | 1 | `6a12483248c8b483df360025` |
| 7 | 200 | 0.236671 s | 70 B | 0 | empty |

## Initial Reading

The recommended feed is the larger performance problem in this sample. Its first page took 18.793 seconds, and the remaining pages still averaged 2.588 seconds. The hot feed was slower than the optimized newest feed target but much less severe in this cursor range, with non-empty pages averaging 0.490 seconds.
