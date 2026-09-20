# my-kick-tracker

Tracks public Kick channel profiles, username changes, social links, follower counts, and public
chat messages into `kick_tracker.db`. The site (`index.html`, `users.html`) does not download the
database any more - it reads everything through the fast `/v1/api` JSON endpoints (see below).

## How it runs

* `.github/workflows/chat-collector.yml` (the "auto cycle"): one long-lived worker per run. It polls
  `streamers.txt` every minute, pushes `kick_tracker.db` + `logz.txt` every 15 minutes, refreshes
  every tracked profile about every 2 hours, and stops itself around 5.5h so the next queued run
  takes over. Only one worker writes the database at a time (concurrency group `kick-db-writer`).
* `.github/workflows/full-refresh.yml`: manual escape hatch that refreshes every tracked channel.

## Scripts

| script | what it does |
| --- | --- |
| `npm run check-streamers` | one poll of `streamers.txt` (profiles + live chat) |
| `npm run refresh-all` | refresh profile data for every tracked channel |
| `npm run prune` | compact `kick_tracker.db` (see below) |
| `npm run archive` | move the oldest chat messages into `archive/` shards (never deletes) |
| `npm run monitor` | local continuous polling loop |
| `npm run serve` | serve the site and the API locally on port 3000 |

## API (`/v1/api`)

The server exposes a fast JSON API backed by SQLite:

* `GET /v1/api/stats` - tracked channels / chat users / chat messages counters (powers the home page).
* `GET /v1/api/user/:handle` - one channel's profile, username history, social history, follower
  history and latest chat messages (powers the profile card on the home page).
* `GET /v1/api/users/latest` - the newest 100 tracked channels (what `/users` shows).

### Anti-scraping design

* There is **no pagination**: any `?limit=`, `?offset=`, `?page=` etc. on the bulk endpoint
  returns `400`. The 100-row cap is a server constant no parameter can raise.
* The bulk endpoint requires a short-lived **signed token** (HMAC, rotates every 10 minutes)
  that is only handed out embedded in the `/users` HTML page itself - there is no token
  endpoint to hit. Plain scripted requests get `403`.
* Row order is **deterministically shuffled per token window**, so even a valid consumer
  cannot walk the table the same way twice or reconstruct a stable full ordering.
* **Resting time**: every endpoint is per-IP rate limited with escalating cooldowns -
  exceeding the limit earns a `429` + `Retry-After`, and each violation makes the next
  rest longer.
* Profile lookups **read across every archive shard automatically**: one API call unions the
  main database with all `archive/kick_tracker-NNN.db` shards (de-duplicated, newest first,
  capped at 500 messages), so archived history is always served. `/v1/api/stats` reports
  `archived_messages` alongside the main-database counters. Readonly SQLite connections are
  cached, and `chat_messages(sender_user_id)` is indexed in the main DB and in every shard
  so shard counts and message lookups stay fast as the archive grows.

## Database maintenance (why `npm run prune` / `npm run archive` exist)

`kick_tracker.db` is committed on every push, and **GitHub rejects any push that contains a
file larger than 100 MiB**. On 2026-09-15 the file crossed that limit, so every auto-cycle push was
rejected by GitHub while the worker kept collecting (the job still reported success), and the site
silently stopped updating.

Maintenance now keeps the file far below the limit **without throwing history away**:

1. slims legacy chat payloads (Kick reply `metadata` used to duplicate entire quoted messages),
2. deletes chat messages past the retention window (skipped entirely when retention is unlimited),
3. **archives** the oldest chat messages into `archive/kick_tracker-NNN.db` shard files instead of
   deleting them - each shard rolls over at ~90 MiB, so the archive can grow essentially forever
   while every committed file stays under GitHub's 100 MiB push limit. Fragmented shards are
   merged back together in numbered order (until the next one would push the merged file over
   ~90 MiB) on every maintenance run and via `npm run merge-shards`; a shard is only removed
   after 100% of its rows are verified present in the merge target,
4. enforces the hard size cap by deleting the oldest messages **only as a fallback** if archiving
   itself failed, then VACUUMs.

Both workflows refuse to push any database file that is still at or above 100 MiB, and commit
`archive/*.db` shards alongside the main database.

| env var | default | meaning |
| --- | --- | --- |
| `CHAT_RETENTION_DAYS` | `7` | how long saved chat messages are kept; `0` or `unlimited` = keep forever (archiving still applies) |
| `CHAT_MAX_DB_MB` | `70` | size cap; growing past it moves the oldest messages into `archive/` shards |
| `CHAT_LOG_MAX_BYTES` | `5242880` | rotate `logz.txt` once it grows past this |
| `CHAT_LOG_KEEP_LINES` | `20000` | lines kept when `logz.txt` rotates |

Channel profiles, username history, social history, and follower snapshots are kept indefinitely;
only chat messages move to archive shards. Each poll run appends a single summary line to
`logz.txt` (e.g. `[Chat][summary] added 6 chatters in 1 poll and 77 messages (... -> ...)`)
instead of one line per channel, and the worker's commit messages carry the same summary for
the pushed window (`+X chatters, +Y messages in N polls (start -> finish)`).