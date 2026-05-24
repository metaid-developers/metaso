# Buzz Newest Pagination Performance Baseline

Date: 2026-05-25

Target endpoint:

```text
https://www.show.now/man/social/buzz/newest?size=10&lastId=6a1313d648c8b483df360d89
```

Method:

- Sequentially requested 10 pages.
- Each next request used `data.lastId` from the previous response.
- Recorded HTTP status, total elapsed time, response size, item count, and next cursor.

## Node Fetch Run

This run used a single Node process with sequential `fetch` calls.

| Page | Request `lastId` | Status | Total ms | Body bytes | Items | Next `lastId` |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | `6a1313d648c8b483df360d89` | 200 | 3894 | 20881 | 10 | `6a1312da48c8b483df360d41` |
| 2 | `6a1312da48c8b483df360d41` | 200 | 1063 | 58422 | 8 | `6a13071a48c8b483df360c9e` |
| 3 | `6a13071a48c8b483df360c9e` | 200 | 685 | 41317 | 9 | `6a12fb9648c8b483df360bd6` |
| 4 | `6a12fb9648c8b483df360bd6` | 200 | 634 | 55122 | 8 | `6a12f35a48c8b483df360b30` |
| 5 | `6a12f35a48c8b483df360b30` | 200 | 604 | 22120 | 6 | `6a12e76f48c8b483df360ab4` |
| 6 | `6a12e76f48c8b483df360ab4` | 200 | 593 | 27936 | 8 | `6a12e51248c8b483df360a60` |
| 7 | `6a12e51248c8b483df360a60` | 200 | 674 | 24838 | 9 | `6a12de2f48c8b483df3609e2` |
| 8 | `6a12de2f48c8b483df3609e2` | 200 | 637 | 22399 | 8 | `6a12d3c248c8b483df36094f` |
| 9 | `6a12d3c248c8b483df36094f` | 200 | 616 | 31016 | 10 | `6a12d39e48c8b483df360922` |
| 10 | `6a12d39e48c8b483df360922` | 200 | 665 | 22372 | 10 | `6a12cb6148c8b483df36089c` |

Summary:

- Pages: 10
- Successful responses: 10
- Average: 1007 ms
- Minimum: 593 ms
- P50: 637 ms
- P90: 1063 ms
- Maximum: 3894 ms

## Curl Timing Run

This run used one `curl` invocation per page and recorded curl timing fields.

| Page | Request `lastId` | Status | DNS ms | Connect ms | TLS ms | TTFB ms | Total ms | Download bytes | Items | Next `lastId` |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | `6a1313d648c8b483df360d89` | 200 | 6 | 105 | 221 | 7949 | 7950 | 20881 | 10 | `6a1312da48c8b483df360d41` |
| 2 | `6a1312da48c8b483df360d41` | 200 | 9 | 27 | 67 | 853 | 877 | 58422 | 8 | `6a13071a48c8b483df360c9e` |
| 3 | `6a13071a48c8b483df360c9e` | 200 | 4 | 22 | 62 | 807 | 807 | 41317 | 9 | `6a12fb9648c8b483df360bd6` |
| 4 | `6a12fb9648c8b483df360bd6` | 200 | 7 | 25 | 64 | 785 | 803 | 55122 | 8 | `6a12f35a48c8b483df360b30` |
| 5 | `6a12f35a48c8b483df360b30` | 200 | 3 | 21 | 63 | 818 | 818 | 22120 | 6 | `6a12e76f48c8b483df360ab4` |
| 6 | `6a12e76f48c8b483df360ab4` | 200 | 3 | 20 | 56 | 726 | 726 | 27936 | 8 | `6a12e51248c8b483df360a60` |
| 7 | `6a12e51248c8b483df360a60` | 200 | 6 | 22 | 59 | 796 | 796 | 24838 | 9 | `6a12de2f48c8b483df3609e2` |
| 8 | `6a12de2f48c8b483df3609e2` | 200 | 4 | 22 | 62 | 1109 | 1109 | 22399 | 8 | `6a12d3c248c8b483df36094f` |
| 9 | `6a12d3c248c8b483df36094f` | 200 | 3 | 19 | 57 | 1105 | 1108 | 31016 | 10 | `6a12d39e48c8b483df360922` |
| 10 | `6a12d39e48c8b483df360922` | 200 | 3 | 22 | 62 | 861 | 862 | 22372 | 10 | `6a12cb6148c8b483df36089c` |

Summary:

- Pages: 10
- Average: 1586 ms
- Average excluding first page: 878 ms
- Minimum: 726 ms
- P50: 818 ms
- P90: 1109 ms
- Maximum: 7950 ms
