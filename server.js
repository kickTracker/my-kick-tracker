import { createServer } from 'http';
import { readFile } from 'fs/promises';
import fs from 'fs';
import { spawn } from 'child_process';
import { createHmac, randomBytes, timingSafeEqual } from 'crypto';
import { resolve, extname, join } from 'path';
import { fileURLToPath } from 'url';
import sqlite3 from 'sqlite3';

const __filename = fileURLToPath(import.meta.url);
const __dirname = resolve(__filename, '..');
const PORT = Number(process.env.PORT || 3000);
const refreshLimitPath = join(__dirname, '.refresh-rate-limit.json');
const refreshLimits = fs.existsSync(refreshLimitPath)
  ? JSON.parse(fs.readFileSync(refreshLimitPath, 'utf8'))
  : {};

// --- API anti-scrape: signed rotating token -----------------------------------
// The bulk users endpoint never accepts limit/offset/page parameters. Instead it
// requires a short-lived HMAC token that is only handed out embedded in the
// /users HTML page, so plain scripted requests against the endpoint get nothing.
// The token rotates every window, and the row order is deterministically
// shuffled per window, so even a valid consumer cannot walk the table the same
// way twice or reconstruct a stable full ordering.
const apiSecretPath = join(__dirname, '.api-secret');
let apiSecret;
if (fs.existsSync(apiSecretPath)) {
  apiSecret = fs.readFileSync(apiSecretPath, 'utf8').trim();
}
if (!apiSecret) {
  apiSecret = randomBytes(32).toString('hex');
  try {
    fs.writeFileSync(apiSecretPath, apiSecret, { mode: 0o600 });
  } catch (error) {
    console.warn('[API] Could not persist API secret:', error.message);
  }
}

const TOKEN_WINDOW_MS = 10 * 60 * 1000; // token rotates every 10 minutes
const USERS_LATEST_MAX = 100;           // hard internal cap; no query param can raise it
const PROFILE_MESSAGES_MAX = 500;       // hard cap on chat messages per profile lookup

function tokenForWindow(ip, windowIndex) {
  return createHmac('sha256', apiSecret).update(`${ip}|${windowIndex}`).digest('hex');
}

function hasValidApiToken(req, ip) {
  const provided = String(req.headers['x-api-token'] || '');
  if (!provided) return false;
  // Accept the current window plus the two previous ones, so a page left open
  // for up to ~30 minutes still works after the token rotated.
  const windowIndex = Math.floor(Date.now() / TOKEN_WINDOW_MS);
  return [0, -1, -2].some(offset => {
    const expected = tokenForWindow(ip, windowIndex + offset);
    const a = Buffer.from(provided);
    const b = Buffer.from(expected);
    return a.length === b.length && timingSafeEqual(a, b);
  });
}

// Deterministic per-window shuffle: same seed -> same order within a window.
function shuffledByWindow(rows, windowIndex) {
  let seed = Number(windowIndex) >>> 0;
  const next = () => {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return seed / 0x100000000;
  };
  const copy = rows.slice();
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(next() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

// --- API "resting time": per-IP rate limiting with escalating cooldowns -------
const RESTING_TIME = {
  'users-latest': { max: 2, windowMs: 60 * 1000, baseCooldownMs: 5 * 60 * 1000 },
  'user-lookup': { max: 30, windowMs: 60 * 1000, baseCooldownMs: 60 * 1000 },
  'stats': { max: 30, windowMs: 60 * 1000, baseCooldownMs: 60 * 1000 }
};

const rateState = new Map();
setInterval(() => {
  const cutoff = Date.now() - 60 * 60 * 1000;
  for (const [key, state] of rateState) {
    if ((state.windowStart < cutoff) && (state.cooldownUntil < cutoff)) rateState.delete(key);
  }
}, 10 * 60 * 1000).unref();

function checkRestingTime(ip, bucket) {
  const config = RESTING_TIME[bucket];
  const now = Date.now();
  const key = `${bucket}|${ip}`;
  const state = rateState.get(key) || { count: 0, windowStart: now, cooldownUntil: 0, strikes: 0 };

  if (state.cooldownUntil > now) {
    rateState.set(key, state);
    return { ok: false, retryAfterSeconds: Math.ceil((state.cooldownUntil - now) / 1000) };
  }

  if (now - state.windowStart > config.windowMs) {
    state.count = 0;
    state.windowStart = now;
  }

  state.count += 1;
  if (state.count > config.max) {
    state.strikes += 1;
    state.count = 0;
    state.windowStart = now;
    // Escalating "resting time": each violation makes the next rest longer.
    state.cooldownUntil = now + config.baseCooldownMs * state.strikes;
    rateState.set(key, state);
    return { ok: false, retryAfterSeconds: Math.ceil((state.cooldownUntil - now) / 1000) };
  }

  rateState.set(key, state);
  return { ok: true };
}

const allowedPaths = new Set(['/','/index.html','/users','/users.html','/privacy.html','/terms.html']);
const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.webp': 'image/webp',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8',
  '.webmanifest': 'application/manifest+json'
};

async function serveFile(filePath, res) {
  try {
    const absolutePath = resolve(__dirname, filePath);
    const file = await readFile(absolutePath);
    const extension = extname(filePath).toLowerCase();
    setSecurityHeaders(res);
    res.writeHead(200, { 'Content-Type': mimeTypes[extension] || 'application/octet-stream' });
    res.end(file);
  } catch (error) {
    res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(await getNotFoundPage());
  }
}

function setSecurityHeaders(res) {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
}

function sendJson(res, statusCode, payload) {
  setSecurityHeaders(res);
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(payload));
}

function getClientIp(req) {
  // Behind a reverse proxy (nginx/Cloudflare) remoteAddress is the proxy IP for
  // everyone, which would merge all users into one token + one rate bucket.
  // Opt in with TRUST_PROXY=1 ONLY when such a proxy actually fronts the app
  // (a client can spoof X-Forwarded-For to rotate its own rate bucket).
  if (process.env.TRUST_PROXY === '1') {
    const forwarded = String(req.headers['x-forwarded-for'] || '');
    const firstHop = forwarded.split(',')[0].trim();
    if (firstHop) return firstHop;
  }
  return (req.socket.remoteAddress || 'unknown').replace(/^::ffff:/, '');
}

function getUtcDate() {
  return new Date().toISOString().slice(0, 10);
}

function saveRefreshLimits() {
  const temporaryPath = `${refreshLimitPath}.tmp`;
  fs.writeFileSync(temporaryPath, JSON.stringify(refreshLimits, null, 2));
  fs.renameSync(temporaryPath, refreshLimitPath);
}

function getChannelLastUpdated(handle) {
  return new Promise((resolveQuery, reject) => {
    const database = new sqlite3.Database(join(__dirname, 'kick_tracker.db'), sqlite3.OPEN_READONLY, (error) => {
      if (error) reject(error);
    });

    database.get(`
      SELECT c.last_updated, c.current_slug
      FROM channels c
      WHERE LOWER(c.current_slug) = ? OR LOWER(c.current_username) = ?
        OR EXISTS (
          SELECT 1 FROM username_history h
          WHERE h.channel_id = c.id
            AND (LOWER(h.slug) = ? OR LOWER(h.username) = ?)
        )
      LIMIT 1
    `, [handle, handle, handle, handle], (error, row) => {
      database.close();
      if (error) reject(error);
      else resolveQuery(row || null);
    });
  });
}

function getStats() {
  return new Promise((resolvePromise, reject) => {
    const database = new sqlite3.Database(join(__dirname, 'kick_tracker.db'), sqlite3.OPEN_READONLY, (error) => {
      if (error) reject(error);
    });

    database.all(`
      SELECT 'channels' AS metric, COUNT(*) AS value FROM channels
      UNION ALL
      SELECT 'chat_users' AS metric, COUNT(*) AS value FROM chat_users
    `, (error, rows) => {
      database.close();
      if (error) reject(error);
      else {
        const stats = { channels: 0, chat_users: 0 };
        for (const row of rows) {
          if (row.metric === 'channels') stats.channels = Number(row.value) || 0;
          else if (row.metric === 'chat_users') stats.chat_users = Number(row.value) || 0;
        }
        resolvePromise(stats);
      }
    });
  });
}

async function handleStats(req, res) {
  if (req.method !== 'GET') {
    sendJson(res, 405, { error: 'Stats require GET.' });
    return;
  }
  try {
    const stats = await getStats();
    sendJson(res, 200, stats);
  } catch (error) {
    sendJson(res, 500, { error: 'Could not read stats.' });
  }
}

// --- /v1/api ------------------------------------------------------------------
// Readonly connections are cheap to reuse: SQLite reads fresh file state on
// every query (no open transaction), so caching them adds no staleness risk.
// Opening the ~70MB main DB takes ~800ms, which would dominate every request.
const readonlyDatabaseCache = new Map();

function openReadonlyDatabaseAt(path) {
  const cached = readonlyDatabaseCache.get(path);
  if (cached) return Promise.resolve(cached);
  return new Promise((resolvePromise, reject) => {
    const database = new sqlite3.Database(path, sqlite3.OPEN_READONLY, (error) => {
      if (error) reject(error);
      else {
        readonlyDatabaseCache.set(path, database);
        resolvePromise(database);
      }
    });
  });
}

function openReadonlyDatabase() {
  return openReadonlyDatabaseAt(join(__dirname, 'kick_tracker.db'));
}

// Every shard DB in archive/ holds chat messages that were moved out of the
// main database. API reads union the main DB + all shards so a single API call
// always sees the full history, no matter how many shards exist.
const archiveShardListCache = { paths: [], expiresAt: 0 };

function getArchiveShardPaths() {
  const archiveDir = join(__dirname, 'archive');
  const now = Date.now();
  // Directory listing on the request path: memoize briefly so busy endpoints
  // don't hit the filesystem on every call. New shards appear within 10s.
  if (archiveShardListCache.expiresAt > now) return archiveShardListCache.paths;
  try {
    const paths = fs.readdirSync(archiveDir)
      .filter(name => /^kick_tracker-\d+\.db$/.test(name))
      .sort()
      .map(name => join(archiveDir, name));
    archiveShardListCache.paths = paths;
    archiveShardListCache.expiresAt = now + 10 * 1000;
    return paths;
  } catch {
    archiveShardListCache.paths = [];
    archiveShardListCache.expiresAt = now + 10 * 1000;
    return [];
  }
}

// Runs the same query against the main DB and every shard, then merges the
// rows with `merge` (or plain concat) and picks with `pick`.
async function queryAcrossShards(sql, params, { merge, pick } = {}) {
  const paths = [join(__dirname, 'kick_tracker.db'), ...getArchiveShardPaths()];
  const results = [];
  for (const path of paths) {
    try {
      const database = await openReadonlyDatabaseAt(path);
      results.push(await queryAll(database, sql, params));
    } catch (error) {
      // A missing/corrupt shard must never take the whole API call down.
      console.warn(`[API] Could not read ${path}:`, error.message);
    }
  }
  if (merge) return merge(results);
  return results.flat();
}

function queryAll(database, sql, params = []) {
  return new Promise((resolvePromise, reject) => {
    database.all(sql, params, (error, rows) => {
      if (error) reject(error);
      else resolvePromise(rows || []);
    });
  });
}

function queryGet(database, sql, params = []) {
  return new Promise((resolvePromise, reject) => {
    database.get(sql, params, (error, row) => {
      if (error) reject(error);
      else resolvePromise(row || null);
    });
  });
}

// Latest tracked channels. Order is shuffled per token window and capped at a
// hard internal constant, so there is no pagination surface to scrape.
async function getLatestUsers() {
  const database = await openReadonlyDatabase();
  try {
    const totalRow = await queryGet(database, 'SELECT COUNT(*) AS total FROM channels');
    const rows = await queryAll(database, `
      SELECT c.id, c.current_slug, c.current_username, c.followers_count, c.last_updated,
             (SELECT MIN(h.id) FROM username_history h WHERE h.channel_id = c.id) AS added_order
      FROM channels c
      ORDER BY added_order DESC, c.id DESC
      LIMIT ?
    `, [USERS_LATEST_MAX]);
    return { total: Number(totalRow?.total) || 0, rows };
  } finally {
    // Connection stays cached; do not close it.
  }
}

async function handleV1UsersLatest(req, res, ip) {
  if (req.method !== 'GET') {
    sendJson(res, 405, { error: 'GET only.' });
    return;
  }

  // No pagination. Ever. Any attempt to pass limit/offset/page/cursor is a 400.
  const paginationKeys = ['limit', 'offset', 'page', 'cursor', 'per_page', 'pagesize'];
  const hasPagination = [...new URL(req.url, 'http://x').searchParams.keys()]
    .some(key => paginationKeys.includes(key.toLowerCase()));
  if (hasPagination) {
    sendJson(res, 400, { error: 'Pagination is not supported.' });
    return;
  }

  // The token is only distributed embedded in the /users HTML page.
  if (!hasValidApiToken(req, ip)) {
    sendJson(res, 403, { error: 'Invalid or expired token.' });
    return;
  }

  const rest = checkRestingTime(ip, 'users-latest');
  if (!rest.ok) {
    res.setHeader('Retry-After', String(rest.retryAfterSeconds));
    sendJson(res, 429, { error: 'Resting time. Try again later.', retry_after: rest.retryAfterSeconds });
    return;
  }

  try {
    const { total, rows } = await getLatestUsers();
    const windowIndex = Math.floor(Date.now() / TOKEN_WINDOW_MS);
    sendJson(res, 200, {
      total,
      count: rows.length,
      window: windowIndex,
      users: shuffledByWindow(rows, windowIndex).map(row => ({
        id: row.id,
        slug: row.current_slug,
        username: row.current_username,
        followers: row.followers_count,
        last_updated: row.last_updated
      }))
    });
  } catch (error) {
    sendJson(res, 500, { error: 'Could not read users.' });
  }
}

// Single-channel lookup. Open but rate limited with resting time.
async function handleV1User(req, res, handle, ip) {
  if (req.method !== 'GET') {
    sendJson(res, 405, { error: 'GET only.' });
    return;
  }
  if (!/^[a-z0-9_-]{1,50}$/.test(handle)) {
    sendJson(res, 400, { error: 'Invalid handle.' });
    return;
  }

  const rest = checkRestingTime(ip, 'user-lookup');
  if (!rest.ok) {
    res.setHeader('Retry-After', String(rest.retryAfterSeconds));
    sendJson(res, 429, { error: 'Resting time. Try again later.', retry_after: rest.retryAfterSeconds });
    return;
  }

  try {
    const database = await openReadonlyDatabase();
    try {
      const channel = await queryGet(database, `
        SELECT c.* FROM channels c
        WHERE LOWER(c.current_slug) = ? OR LOWER(c.current_username) = ?
          OR EXISTS (
            SELECT 1 FROM username_history h
            WHERE h.channel_id = c.id AND (LOWER(h.slug) = ? OR LOWER(h.username) = ?)
          )
        LIMIT 1
      `, [handle, handle, handle, handle]);
      if (!channel) {
        sendJson(res, 404, { error: 'Unknown channel.' });
        return;
      }
      const history = await queryAll(database, `
        SELECT slug, username, detected_at FROM username_history
        WHERE channel_id = ? ORDER BY detected_at ASC
      `, [channel.id]);
      const socials = await queryAll(database, `
        SELECT field_name, old_value, new_value, detected_at FROM socials_history
        WHERE channel_id = ? ORDER BY detected_at DESC
      `, [channel.id]);
      const followers = await queryAll(database, `
        SELECT followers_count, recorded_at FROM follower_history
        WHERE channel_id = ? ORDER BY recorded_at ASC
      `, [channel.id]);
      // Latest messages sent by this channel's user across all chats, newest
      // first. Reads the main DB AND every archive shard, hard-capped server
      // side (no pagination surface). Message rows are unique across shards
      // (INSERT OR IGNORE during archiving), so the merge de-dups as a safety
      // net. Shards only hold chat_messages, so the channel_slug join is done
      // afterwards against the main DB.
      const shardSafeSql = `
        SELECT m.chat_id, m.content, m.message_type, m.created_at, m.saved_at, m.raw_payload,
               m.sender_slug, m.sender_username
        FROM chat_messages m
        WHERE m.sender_user_id = ?
        ORDER BY COALESCE(m.created_at, m.saved_at) DESC
        LIMIT ?
      `;
      const messageRows = await queryAcrossShards(shardSafeSql, [channel.user_id || channel.id, PROFILE_MESSAGES_MAX], {
        merge: (perDatabaseRows) => {
          const seen = new Set();
          const merged = [];
          for (const rows of perDatabaseRows) {
            for (const row of rows) {
              const key = `${row.chat_id}|${row.created_at}|${row.saved_at}|${row.content}`;
              if (seen.has(key)) continue;
              seen.add(key);
              merged.push(row);
            }
          }
          merged.sort((a, b) => {
            const av = String(a.created_at || a.saved_at || '');
            const bv = String(b.created_at || b.saved_at || '');
            return bv.localeCompare(av);
          });
          return merged.slice(0, PROFILE_MESSAGES_MAX);
        }
      });
      // Attach channel_slug (which chat the message was sent in) from the main DB.
      if (messageRows.length > 0) {
        const chatIds = [...new Set(messageRows.map(row => row.chat_id).filter(Boolean))];
        const chatSlugRows = await queryAll(database, `
          SELECT id, current_slug FROM channels WHERE id IN (${chatIds.map(() => '?').join(',')})
        `, chatIds);
        const slugById = new Map(chatSlugRows.map(row => [row.id, row.current_slug]));
        for (const row of messageRows) row.channel_slug = slugById.get(row.chat_id) || null;
      }
      const totalMessages = Number((await queryGet(database, `
        SELECT COUNT(*) AS total FROM chat_messages WHERE sender_user_id = ?
      `, [channel.user_id || channel.id]))?.total) || 0;
      let archivedMessages = 0;
      if (getArchiveShardPaths().length > 0) {
        const shardTotals = await queryAcrossShards(`
          SELECT COUNT(*) AS total FROM chat_messages WHERE sender_user_id = ?
        `, [channel.user_id || channel.id]);
        // queryAcrossShards already includes the main DB's count; subtract it.
        archivedMessages = Math.max(0, shardTotals.reduce((sum, row) => sum + (Number(row?.total) || 0), 0) - totalMessages);
      }
      sendJson(res, 200, {
        ...channel,
        username_history: history,
        socials_history: socials,
        follower_history: followers,
        chat_messages: messageRows,
        chat_messages_total: totalMessages + archivedMessages,
        chat_messages_archived: archivedMessages
      });
    } finally {
      // Connection stays cached; do not close it.
    }
  } catch (error) {
    sendJson(res, 500, { error: 'Could not read the channel.' });
  }
}

async function handleV1Stats(req, res, ip) {
  if (req.method !== 'GET') {
    sendJson(res, 405, { error: 'GET only.' });
    return;
  }
  const rest = checkRestingTime(ip, 'stats');
  if (!rest.ok) {
    res.setHeader('Retry-After', String(rest.retryAfterSeconds));
    sendJson(res, 429, { error: 'Resting time. Try again later.', retry_after: rest.retryAfterSeconds });
    return;
  }
  try {
    const database = await openReadonlyDatabase();
    try {
      const rows = await queryAll(database, `
        SELECT 'channels' AS metric, COUNT(*) AS value FROM channels
        UNION ALL
        SELECT 'chat_users' AS metric, COUNT(*) AS value FROM chat_users
        UNION ALL
        SELECT 'chat_messages' AS metric, COUNT(*) AS value FROM chat_messages
      `);
      const stats = { channels: 0, chat_users: 0, chat_messages: 0, archived_messages: 0 };
      for (const row of rows) stats[row.metric] = Number(row.value) || 0;
      // Chat messages living in archive shards are part of the total history.
      const shardPaths = getArchiveShardPaths();
      for (const shardPath of shardPaths) {
        try {
          const shardDatabase = await openReadonlyDatabaseAt(shardPath);
          const shardTotal = await queryGet(shardDatabase, 'SELECT COUNT(*) AS total FROM chat_messages');
          stats.archived_messages += Number(shardTotal?.total) || 0;
        } catch (error) {
          console.warn(`[API] Could not read ${shardPath}:`, error.message);
        }
      }
      sendJson(res, 200, stats);
    } finally {
      // Connection stays cached; do not close it.
    }
  } catch (error) {
    sendJson(res, 500, { error: 'Could not read stats.' });
  }
}

// Serve the users page with the current API token injected, so the token is
// only ever handed to real page loads and never via a separate token endpoint.
let usersHtmlCache = null;
async function serveUsersPage(res, ip) {
  try {
    if (!usersHtmlCache) {
      usersHtmlCache = await readFile(resolve(__dirname, 'users.html'), 'utf8');
    }
    const token = tokenForWindow(ip, Math.floor(Date.now() / TOKEN_WINDOW_MS));
    const html = usersHtmlCache.replace(
      '</head>',
      `<meta name="api-token" content="${token}" />\n</head>`
    );
    setSecurityHeaders(res);
    // The page carries an IP-bound API token: it must never be cached by an
    // intermediate (CDN/proxy), or one visitor could receive another's token.
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(html);
  } catch (error) {
    res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(await getNotFoundPage());
  }
}

function runRefresh(handle) {
  return new Promise((resolveProcess, reject) => {
    const child = spawn(process.execPath, [join(__dirname, 'tracker.js'), '--refresh', handle], {
      cwd: __dirname,
      stdio: ['ignore', 'ignore', 'pipe']
    });
    let errorOutput = '';
    child.stderr.on('data', chunk => { errorOutput += chunk.toString(); });
    child.on('error', reject);
    child.on('close', code => {
      if (code === 0) resolveProcess();
      else reject(new Error(errorOutput.trim() || `Refresh exited with code ${code}`));
    });
  });
}

async function handleRefresh(req, res, requestUrl) {
  if (req.method !== 'POST') {
    sendJson(res, 405, { error: 'Refresh requires POST.' });
    return;
  }

  const handle = (requestUrl.searchParams.get('user') || '').trim().toLowerCase().replace(/^@/, '');
  if (!/^[a-z0-9_-]{1,50}$/.test(handle)) {
    sendJson(res, 400, { error: 'Enter a valid Kick handle.' });
    return;
  }

  let channel;
  try {
    channel = await getChannelLastUpdated(handle);
  } catch (error) {
    sendJson(res, 500, { error: 'Could not inspect the database.' });
    return;
  }

  if (!channel) {
    sendJson(res, 404, { error: 'Search this handle first so it can be refreshed.' });
    return;
  }

  const updatedAt = channel.last_updated ? new Date(`${channel.last_updated.replace(' ', 'T')}Z`) : null;
  if (updatedAt && Number.isFinite(updatedAt.getTime()) && Date.now() - updatedAt.getTime() < 60 * 60 * 1000) {
    sendJson(res, 200, { ok: false, reason: 'fresh', message: 'This channel was fetched less than an hour ago.' });
    return;
  }

  const ip = getClientIp(req);
  const today = getUtcDate();
  if (refreshLimits[ip] === today) {
    sendJson(res, 429, { error: 'This IP has already used its refresh for today.' });
    return;
  }

  try {
    await runRefresh(channel.current_slug || handle);
    refreshLimits[ip] = today;
    saveRefreshLimits();
    sendJson(res, 200, { ok: true, message: 'Fresh data saved. Reloading the database snapshot.' });
  } catch (error) {
    sendJson(res, 502, { error: 'The Kick refresh failed.', detail: error.message });
  }
}

async function getNotFoundPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>404 - Page Not Found</title>
  <style>
    :root {
      --bg: #07090e;
      --card: rgba(15, 19, 28, 0.82);
      --border: rgba(255,255,255,0.08);
      --green: #53fc18;
      --text: #f0f4f8;
      --muted: #8a99ad;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: var(--bg);
      color: var(--text);
      font-family: Arial, Helvetica, sans-serif;
    }
    .card {
      width: min(90vw, 560px);
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 20px;
      padding: 42px 28px;
      text-align: center;
      box-shadow: 0 30px 60px rgba(0,0,0,0.5);
    }
    .code {
      font-size: clamp(3rem, 8vw, 6rem);
      font-weight: 800;
      color: var(--green);
      letter-spacing: 0.08em;
      margin-bottom: 12px;
    }
    h1 {
      margin: 0 0 12px;
      font-size: clamp(1.5rem, 3vw, 2.3rem);
    }
    p {
      margin: 0 0 24px;
      color: var(--muted);
      line-height: 1.6;
    }
    a {
      display: inline-block;
      background: var(--green);
      color: #000;
      text-decoration: none;
      font-weight: 700;
      padding: 12px 20px;
      border-radius: 12px;
    }
  </style>
</head>
<body>
  <div class="card">
    <div class="code">404</div>
    <h1>Page Not Found</h1>
    <p>The page you requested does not exist or is not available on this site.</p>
    <a href="/">Return Home</a>
  </div>
</body>
</html>`;
}

const server = createServer(async (req, res) => {
  try {
    const requestUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathname = decodeURIComponent(requestUrl.pathname);

    if (pathname === '/api/refresh') {
      await handleRefresh(req, res, requestUrl);
      return;
    }

    if (pathname === '/api/stats') {
      await handleStats(req, res);
      return;
    }

    // --- /v1/api ---
    if (pathname === '/v1/api/stats') {
      await handleV1Stats(req, res, getClientIp(req));
      return;
    }

    if (pathname === '/v1/api/users/latest') {
      await handleV1UsersLatest(req, res, getClientIp(req));
      return;
    }

    if (pathname.startsWith('/v1/api/user/')) {
      const handle = pathname.slice('/v1/api/user/'.length).trim().toLowerCase().replace(/^@/, '');
      await handleV1User(req, res, handle, getClientIp(req));
      return;
    }

    if (pathname === '/' || pathname === '/index.html') {
      await serveFile('index.html', res);
      return;
    }

    if (pathname === '/users' || pathname === '/users.html') {
      await serveUsersPage(res, getClientIp(req));
      return;
    }

    // /kick_tracker.db is intentionally NOT served: the site reads everything
    // through /v1/api, and a public bulk download would bypass every
    // anti-scraping protection on the API.

    if (pathname === '/privacy.html' || pathname === '/terms.html') {
      await serveFile(pathname.slice(1), res);
      return;
    }

    // Favicon & PWA asset files
    const staticAssetFiles = ['favicon.svg', 'favicon-16x16.png', 'favicon-32x32.png', 'favicon-180.png', 'favicon-192.png', 'favicon-512.png', 'site.webmanifest'];
    if (staticAssetFiles.some(f => pathname === `/${f}`)) {
      await serveFile(pathname.slice(1), res);
      return;
    }

    if (pathname === '/404' || pathname === '/404.html') {
      res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(await getNotFoundPage());
      return;
    }

    if (pathname.includes('..')) {
      res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(await getNotFoundPage());
      return;
    }

    if (allowedPaths.has(pathname)) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(await getNotFoundPage());
      return;
    }

    res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(await getNotFoundPage());
  } catch (error) {
    res.writeHead(500, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(await getNotFoundPage());
  }
});

server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
