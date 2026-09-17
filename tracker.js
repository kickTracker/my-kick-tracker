import puppeteer from 'puppeteer';
import sqlite3 from 'sqlite3';

const db = new sqlite3.Database('./kick_tracker.db');

// Promisified DB helpers
function runQuery(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function getQuery(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function allQuery(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

// Database Initializer
async function initDb() {
  try {
    // 1. Core channels table: create only if missing, never drop existing data.
    await runQuery(`
      CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        current_slug TEXT UNIQUE,
        current_username TEXT,
        followers_count INTEGER DEFAULT 0,
        is_banned INTEGER DEFAULT 0,
        verified INTEGER DEFAULT 0,
        subscription_enabled INTEGER DEFAULT 0,
        vod_enabled INTEGER DEFAULT 0,
        livestream_title TEXT,
        bio TEXT,
        instagram TEXT,
        twitter TEXT,
        youtube TEXT,
        discord TEXT,
        tiktok TEXT,
        facebook TEXT,
        profile_pic TEXT,
        raw_payload TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Safely migrate existing tables: only add columns that do not exist yet.
    const existingColumns = await allQuery('PRAGMA table_info(channels)');
    const existingColumnNames = new Set(existingColumns.map(col => col.name));
    const columnsToAdd = [
      { name: 'bio', type: 'TEXT' },
      { name: 'instagram', type: 'TEXT' },
      { name: 'twitter', type: 'TEXT' },
      { name: 'youtube', type: 'TEXT' },
      { name: 'discord', type: 'TEXT' },
      { name: 'tiktok', type: 'TEXT' },
      { name: 'facebook', type: 'TEXT' },
      { name: 'profile_pic', type: 'TEXT' },
      { name: 'subscription_enabled', type: 'INTEGER DEFAULT 0' },
      { name: 'vod_enabled', type: 'INTEGER DEFAULT 0' }
    ];

    for (const col of columnsToAdd) {
      if (!existingColumnNames.has(col.name)) {
        await runQuery(`ALTER TABLE channels ADD COLUMN ${col.name} ${col.type}`);
      }
    }

    // 2. Historical Username Tracker Table: keep old history forever.
    await runQuery(`
      CREATE TABLE IF NOT EXISTS username_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id INTEGER,
        slug TEXT,
        username TEXT,
        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(channel_id) REFERENCES channels(id)
      )
    `);

    // 3. Historical Socials Tracker Table: keep all previous social changes.
    await runQuery(`
      CREATE TABLE IF NOT EXISTS socials_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id INTEGER,
        field_name TEXT,
        old_value TEXT,
        new_value TEXT,
        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(channel_id) REFERENCES channels(id)
      )
    `);

    // 4. Historical follower snapshots: keep every scrape for trend graphs.
    await runQuery(`
      CREATE TABLE IF NOT EXISTS follower_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id INTEGER,
        followers_count INTEGER DEFAULT 0,
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(channel_id) REFERENCES channels(id)
      )
    `);

    await runQuery(`
      CREATE TABLE IF NOT EXISTS chat_users (
        user_id INTEGER PRIMARY KEY,
        current_slug TEXT,
        current_username TEXT,
        first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        raw_identity TEXT
      )
    `);

    await runQuery(`
      CREATE TABLE IF NOT EXISTS chat_messages (
        message_id TEXT PRIMARY KEY,
        chat_id INTEGER NOT NULL,
        sender_user_id INTEGER,
        sender_slug TEXT,
        sender_username TEXT,
        content TEXT,
        message_type TEXT,
        created_at TIMESTAMP,
        raw_payload TEXT,
        saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Retention windows and the size cap walk chat_messages in saved_at order,
    // so keep a cheap index for the prunes done by runChatStorageMaintenance().
    await runQuery(`
      CREATE INDEX IF NOT EXISTS idx_chat_messages_saved_at ON chat_messages (saved_at)
    `);

    // Profile lookups filter messages by sender; without this index every API
    // user lookup was a full table scan over the whole message history.
    await runQuery(`
      CREATE INDEX IF NOT EXISTS idx_chat_messages_sender ON chat_messages (sender_user_id)
    `);

    await runQuery(`
      INSERT INTO follower_history (channel_id, followers_count, recorded_at)
      SELECT c.id, COALESCE(c.followers_count, 0), CURRENT_TIMESTAMP
      FROM channels c
      WHERE NOT EXISTS (
        SELECT 1 FROM follower_history h WHERE h.channel_id = c.id
      )
    `);

    await repairCurrentFollowerSnapshots();

    await runQuery(`
      UPDATE channels
      SET is_banned = NULL
      WHERE is_banned = 0
        AND (
          raw_payload IS NULL
          OR raw_payload NOT LIKE '%"is_banned"%'
        )
    `);
  } catch (error) {
    console.error('[DB] Initialization failed:', error.message);
    throw error;
  }
}

function getPayloadFollowerCount(rawPayload) {
  if (!rawPayload) return null;
  try {
    const payload = JSON.parse(rawPayload);
    const value = payload.followersCount ?? payload.followers_count ?? payload.follower_count;
    const count = Number.parseInt(String(value ?? '').replace(/[^0-9]/g, ''), 10);
    return Number.isFinite(count) ? count : null;
  } catch (error) {
    return null;
  }
}

async function repairCurrentFollowerSnapshots() {
  // One-time cleanup: collapse pre-existing spam (many rows per day) down to
  // the first snapshot per channel per day, so the DB shrinks and graphs stay clean.
  await runQuery(`
    DELETE FROM follower_history
    WHERE id NOT IN (
      SELECT MIN(id)
      FROM follower_history
      GROUP BY channel_id, DATE(recorded_at)
    )
  `);

  // One-time shrink: replace fat legacy channels.raw_payload blobs (~14KB avg)
  // with the slim form (~300B) going forward. Keeps is_banned-gated repair working
  // because the slim payload still contains the is_banned key.
  const fatChannels = await allQuery(`
    SELECT id, raw_payload FROM channels WHERE LENGTH(raw_payload) > 2000
  `);
  for (const channel of fatChannels) {
    let slim = null;
    try {
      const p = JSON.parse(channel.raw_payload);
      const u = p.user || {};
      const ls = p.livestream || null;
      slim = JSON.stringify({
        is_banned: p.is_banned ?? null,
        is_live: ls ? true : (p.is_live ?? null),
        verified: p.verified ? true : undefined,
        subscription_enabled: Boolean(p.subscription_enabled || p.is_affiliate),
        vod_enabled: Boolean(p.vod_enabled),
        vod_settings: p.vod_settings ? { enabled: Boolean(p.vod_settings.enabled) } : undefined,
        followersCount: Number.parseInt(String(p.followersCount ?? p.followers_count ?? 0), 10) || 0,
        followers_count: Number.parseInt(String(p.followersCount ?? p.followers_count ?? 0), 10) || 0,
        user: { profile_pic: u.profile_pic || null },
        livestream: ls ? {
          is_live: true,
          session_title: ls.session_title || null,
          title: ls.title || null,
          viewer_count: ls.viewer_count ?? null,
        } : null,
      });
    } catch {
      continue;
    }
    if (slim) await runQuery('UPDATE channels SET raw_payload = ? WHERE id = ?', [slim, channel.id]);
  }

  const channels = await allQuery(`
    SELECT id, followers_count, raw_payload
    FROM channels
    WHERE raw_payload IS NOT NULL
  `);

  for (const channel of channels) {
    const payloadCount = getPayloadFollowerCount(channel.raw_payload);
    if (payloadCount === null || payloadCount <= 0) continue;

    if (Number(channel.followers_count) !== payloadCount) {
      await runQuery('UPDATE channels SET followers_count = ? WHERE id = ?', [payloadCount, channel.id]);
    }

    const latestSnapshot = await getQuery(`
      SELECT followers_count, recorded_at
      FROM follower_history
      WHERE channel_id = ?
      ORDER BY recorded_at DESC, id DESC
      LIMIT 1
    `, [channel.id]);

    const today = new Date().toISOString().slice(0, 10);
    const latestDay = latestSnapshot?.recorded_at ? String(latestSnapshot.recorded_at).slice(0, 10) : null;

    if (!latestSnapshot || (Number(latestSnapshot.followers_count) !== payloadCount && latestDay !== today)) {
      await runQuery('INSERT INTO follower_history (channel_id, followers_count) VALUES (?, ?)', [channel.id, payloadCount]);
    }
  }
}

async function processChannelPayload(data) {
  if (!data || !data.id) return null;

  const channelId = data.id;
  const userObj = data.user || {};
  const userId = data.user_id || userObj.id || null;
  const newSlug = data.slug;
  const newUsername = userObj.username || newSlug;
  const followersCount = Number.parseInt(String(data.followersCount ?? data.followers_count ?? 0), 10) || 0;
  const isBanned = data.is_banned === undefined || data.is_banned === null ? null : (data.is_banned ? 1 : 0);
  const verified = data.verified ? 1 : 0;
  
  // Subscription / Monetized status check
  const subscriptionEnabled = (data.subscription_enabled || data.is_affiliate) ? 1 : 0;
  
  // Extract vod_enabled from data object or fallback check
  const vodEnabled = (data.vod_enabled === true || (data.vod_enabled !== false && data.vod_enabled !== 0)) ? 1 : 0;

  const livestreamTitle = data.livestream ? data.livestream.session_title : null;
  // Slim payload: keep only what the frontend/tracker actually read.
  // Drops the fat junk: previous_livestreams (~60%), playback_url (1KB JWT),
  // recent_categories banners/descriptions, subscriber_badges, ascending_links,
  // media/offline_banner srcsets, chatroom slow-mode flags.
  // NOTE: is_affiliate is already folded into subscriptionEnabled above, so it
  // is intentionally not kept here.
  const rawPayload = JSON.stringify({
    is_banned: data.is_banned ?? null,
    is_live: data.livestream ? true : (data.is_live ?? null),
    verified: data.verified ? true : undefined,
    subscription_enabled: Boolean(data.subscription_enabled || data.is_affiliate),
    vod_enabled: Boolean(data.vod_enabled),
    vod_settings: data.vod_settings ? { enabled: Boolean(data.vod_settings.enabled) } : undefined,
    followersCount,
    followers_count: followersCount,
    user: { profile_pic: userObj.profile_pic || null },
    livestream: data.livestream ? {
      is_live: true,
      session_title: data.livestream.session_title || null,
      title: data.livestream.title || null,
      viewer_count: data.livestream.viewer_count ?? null,
    } : null,
  });

  const bio = userObj.bio || "";
  const instagram = userObj.instagram || "";
  const twitter = userObj.twitter || "";
  const youtube = userObj.youtube || "";
  const discord = userObj.discord || "";
  const tiktok = userObj.tiktok || "";
  const facebook = userObj.facebook || "";
  const profilePic = userObj.profile_pic || "";

  const existing = await getQuery('SELECT * FROM channels WHERE id = ?', [channelId]);

  if (!existing) {
    await runQuery(`
      INSERT INTO channels (
        id, user_id, current_slug, current_username, followers_count,
        is_banned, verified, subscription_enabled, vod_enabled, livestream_title, bio, instagram, twitter,
        youtube, discord, tiktok, facebook, profile_pic, raw_payload
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      channelId, userId, newSlug, newUsername, followersCount,
      isBanned, verified, subscriptionEnabled, vodEnabled, livestreamTitle, bio, instagram, twitter,
      youtube, discord, tiktok, facebook, profilePic, rawPayload
    ]);

    await runQuery(`
      INSERT INTO username_history (channel_id, slug, username)
      VALUES (?, ?, ?)
    `, [channelId, newSlug, newUsername]);

    console.log(`[+] Tracked new channel: @${newSlug} (ID: ${channelId})`);
  } else {
    // Check for username / slug changes
    if (existing.current_slug !== newSlug || existing.current_username !== newUsername) {
      console.log(`[!] Handle change detected for ID ${channelId}: @${existing.current_slug} -> @${newSlug}`);
      await runQuery(`
        INSERT INTO username_history (channel_id, slug, username)
        VALUES (?, ?, ?)
      `, [channelId, newSlug, newUsername]);
    }

    // Check for social profile changes
    const socialFields = [
      { name: 'bio', val: bio },
      { name: 'instagram', val: instagram },
      { name: 'twitter', val: twitter },
      { name: 'youtube', val: youtube },
      { name: 'discord', val: discord },
      { name: 'tiktok', val: tiktok },
      { name: 'facebook', val: facebook }
    ];

    for (const field of socialFields) {
      const oldVal = existing[field.name] || "";
      if (oldVal !== field.val) {
        console.log(`[!] ${field.name} change for @${newSlug}: "${oldVal}" -> "${field.val}"`);
        await runQuery(`
          INSERT INTO socials_history (channel_id, field_name, old_value, new_value)
          VALUES (?, ?, ?, ?)
        `, [channelId, field.name, oldVal, field.val]);
      }
    }

    await runQuery(`
      UPDATE channels
      SET current_slug = ?, current_username = ?, followers_count = ?,
          is_banned = ?, verified = ?, subscription_enabled = ?, vod_enabled = ?, livestream_title = ?, bio = ?,
          instagram = ?, twitter = ?, youtube = ?, discord = ?,
          tiktok = ?, facebook = ?, profile_pic = ?, raw_payload = ?,
          last_updated = CURRENT_TIMESTAMP
      WHERE id = ?
    `, [
      newSlug, newUsername, followersCount, isBanned, verified, subscriptionEnabled, vodEnabled,
      livestreamTitle, bio, instagram, twitter, youtube, discord,
      tiktok, facebook, profilePic, rawPayload, channelId
    ]);
  }

  // Follower snapshot: at most 1 row per channel per day, and only when the count changed.
  // This keeps graph history without spamming ~2,400 identical rows/day.
  const latestSnapshot = await getQuery(`
    SELECT followers_count, recorded_at
    FROM follower_history
    WHERE channel_id = ?
    ORDER BY recorded_at DESC, id DESC
    LIMIT 1
  `, [channelId]);

  const today = new Date().toISOString().slice(0, 10);
  const latestDay = latestSnapshot?.recorded_at ? String(latestSnapshot.recorded_at).slice(0, 10) : null;
  const countChanged = !latestSnapshot || Number(latestSnapshot.followers_count) !== followersCount;

  if (!latestSnapshot || (countChanged && latestDay !== today)) {
    await runQuery(`
      INSERT INTO follower_history (channel_id, followers_count)
      VALUES (?, ?)
    `, [channelId, followersCount]);
  }

  return channelId;
}

function isLiveChannelPayload(data) {
  const livestream = data?.livestream;
  const liveTitle = data?.livestream_title || livestream?.session_title || livestream?.title;
  const liveFlag = data?.is_live ?? data?.isLive ?? livestream?.is_live ?? livestream?.isLive;
  return liveFlag === true || liveFlag === 1 || Boolean(liveTitle) || Boolean(livestream && liveFlag !== false && liveFlag !== 0);
}

// Kick sends a reply as a fat `metadata` JSON *string* that re-serialises the
// quoted message (sender identity, badges, emote images...) and repeats itself
// for every reply level. On top of that the tracker used to store the same
// quoted message a second time as a nested `original_message` object, which made
// an average reply row ~6.8 KB instead of ~0.4 KB. That duplication alone was
// ~75 MB of kick_tracker.db and pushed the file over GitHub's 100 MiB per-file
// push limit, which silently broke every auto-cycle push (see the note on
// runChatStorageMaintenance).
// The site only renders id / content / created_at / sender.username|slug plus the
// reply chain (index.html: getReplyChain + parseReplyMetadata), so only those
// survive here.
// Safety net only: the site renders the whole embedded reply thread (~30 levels
// deep in real data), so this is deliberately generous. Cycles are already
// stopped by the `seen` id set, matching index.html getReplyChain().
const CHAT_REPLY_MAX_DEPTH = 200;

function pickChatReplySource(message) {
  if (!message || typeof message !== 'object') return null;
  if (message.original_message) return message.original_message;

  const metadata = message.metadata;
  if (metadata && typeof metadata === 'object') return metadata.original_message || null;
  if (typeof metadata === 'string') {
    try {
      return JSON.parse(metadata)?.original_message || null;
    } catch {
      return null;
    }
  }

  return null;
}

function slimChatMessage(message, depth = 0, seen = new Set()) {
  if (!message || typeof message !== 'object') return null;
  const id = message.id || null;
  if (!id || seen.has(id)) return null;
  seen.add(id);

  const sender = message.sender || {};
  const slim = {
    id,
    content: message.content || '',
    created_at: message.created_at || null,
    sender: {
      id: Number(sender.id ?? message.user_id) || null,
      username: sender.username || null,
      slug: sender.slug || null,
    },
  };

  const replySource = depth < CHAT_REPLY_MAX_DEPTH ? pickChatReplySource(message) : null;
  const slimReply = replySource ? slimChatMessage(replySource, depth + 1, seen) : null;
  if (slimReply) slim.original_message = slimReply;

  return slim;
}

async function saveChatHistory(chatId, historyPayload) {
  const messages = Array.isArray(historyPayload?.data?.messages) ? historyPayload.data.messages : [];
  let savedMessages = 0;
  let discoveredUsers = 0;

  const uniqueSenders = new Map();
  for (const message of messages) {
    const sender = message.sender || {};
    const senderId = Number(sender.id || message.user_id) || null;
    if (senderId && !uniqueSenders.has(senderId)) uniqueSenders.set(senderId, sender);
  }

  for (const senderId of uniqueSenders.keys()) {
    const existingUser = await getQuery('SELECT user_id FROM chat_users WHERE user_id = ?', [senderId]);
    if (!existingUser) {
      // Only look up brand-new chatters against the channels API. Re-fetching
      // every known chatter on every poll caused Kick API 429 rate limits.
      const sender = uniqueSenders.get(senderId);
      await refreshChatUserChannel(sender, senderId);
      discoveredUsers += 1;
    }
  }

  for (const message of messages) {
    const sender = message.sender || {};
    const senderId = Number(sender.id || message.user_id) || null;
    if (senderId) {
      const existingUser = await getQuery('SELECT user_id FROM chat_users WHERE user_id = ?', [senderId]);
      await runQuery(`
        INSERT INTO chat_users (user_id, current_slug, current_username, raw_identity, last_seen_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
          current_slug = excluded.current_slug,
          current_username = excluded.current_username,
          raw_identity = excluded.raw_identity,
          last_seen_at = CURRENT_TIMESTAMP
      `, [senderId, sender.slug || null, sender.username || sender.slug || null, JSON.stringify(sender)]);
      if (!existingUser) discoveredUsers += 1;
    }

    if (!message.id) continue;
    // Slim reply-aware payload: everything the site's chat renderer needs and
    // nothing else (identity/badges/metadata echoes are dropped).
    const slimMessage = slimChatMessage(message);
    if (!slimMessage) continue;
    const slimChatPayload = JSON.stringify(slimMessage);
    const result = await runQuery(`
      INSERT OR IGNORE INTO chat_messages (
        message_id, chat_id, sender_user_id, sender_slug, sender_username,
        content, message_type, created_at, raw_payload
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      message.id,
      chatId,
      senderId,
      sender.slug || null,
      sender.username || sender.slug || null,
      message.content || '',
      message.type || 'message',
      message.created_at || null,
      slimChatPayload
    ]);
    if (result.changes > 0) savedMessages += 1;
  }

  return { savedMessages, discoveredUsers };
}

async function refreshChatUserChannel(sender, senderId) {
  const existingChannel = await getQuery(`
    SELECT id, current_slug, current_username FROM channels
    WHERE user_id = ? OR LOWER(current_slug) = LOWER(?)
    LIMIT 1
  `, [senderId, sender.slug || '']);
  if (!sender.slug) return Boolean(existingChannel);

  try {
    await throttleKickRequest();
    const response = await fetch(`https://kick.com/api/v1/channels/${encodeURIComponent(sender.slug)}`, {
      headers: { 'User-Agent': 'KickIntel Tracker/1.0', Accept: 'application/json' }
    });
    if (!response.ok) return false;
    const payload = await response.json();
    if (!payload?.id) return false;
    await processChannelPayload(payload);
    if (!existingChannel) console.log(`[CHAT] Added chatter as tracked channel: @${payload.slug || sender.slug}`);
    return Boolean(existingChannel);
  } catch (error) {
    console.warn(`[CHAT] Could not add chatter @${sender.slug} as a channel:`, error.message);
    return false;
  }
}

async function fetchLiveChatHistory(data) {
  if (!isLiveChannelPayload(data)) return { savedMessages: 0, discoveredUsers: 0 };

  const chatId = Number(
    data.id || data.chat_id || data.chatroom_id || data.chatroom?.id || data.livestream?.chat_id || data.livestream?.chatroom_id || data.livestream?.chatroom?.id
  );
  if (!chatId) return { savedMessages: 0, discoveredUsers: 0 };

  const response = await fetch(`https://web.kick.com/api/v1/chat/${encodeURIComponent(chatId)}/history`, {
    headers: { 'User-Agent': 'KickIntel Tracker/1.0', Accept: 'application/json' }
  });
  if (!response.ok) throw new Error(`Chat history returned ${response.status}`);
  const payload = await response.json();
  return saveChatHistory(chatId, payload);
}

const CHAT_LOG_PATH = './logz.txt';
const CHAT_LOG_MAX_BYTES = Number.parseInt(process.env.CHAT_LOG_MAX_BYTES ?? '5242880', 10);
const CHAT_LOG_KEEP_LINES = Number.parseInt(process.env.CHAT_LOG_KEEP_LINES ?? '20000', 10);

// logz.txt is committed on every push too, so trim it instead of letting it
// grow forever (it was already past 2.4 MB when the pushes started failing).
function rotateChatLogIfNeeded() {
  try {
    if (fs.statSync(CHAT_LOG_PATH).size <= CHAT_LOG_MAX_BYTES) return;
    const kept = fs.readFileSync(CHAT_LOG_PATH, 'utf8')
      .split('\n')
      .filter(line => line.length > 0)
      .slice(-CHAT_LOG_KEEP_LINES)
      .join('\n');
    fs.writeFileSync(CHAT_LOG_PATH, `${new Date().toISOString()} [Chat][Log] rotated - kept the newest ${CHAT_LOG_KEEP_LINES} lines\n${kept}\n`, 'utf8');
  } catch (error) {
    console.warn('[Chat] Could not rotate logz.txt:', error.message);
  }
}

function appendChatLog(tag, message) {
  const timestamp = new Date().toISOString();
  fs.appendFileSync(CHAT_LOG_PATH, `${timestamp} [Chat][${tag}] ${message}\n`, 'utf8');
  rotateChatLogIfNeeded();
}

// --- Run summary (replaces per-channel log spam) ------------------------------
// Every poll run appends ONE summary line instead of a line per channel:
//   [summary] added 42 chatters in 1 poll and 318 messages (start -> finish UTC)
// Counts also accumulate in .chat-tally.json (gitignored) so the GitHub Actions
// worker can put the same summary into its commit messages until the next push.
const CHAT_TALLY_PATH = './.chat-tally.json';

function readChatTally() {
  try {
    const tally = JSON.parse(fs.readFileSync(CHAT_TALLY_PATH, 'utf8'));
    if (tally && typeof tally === 'object') {
      return {
        start: tally.start || null,
        finish: tally.finish || null,
        polls: Number(tally.polls) || 0,
        chatters: Number(tally.chatters) || 0,
        messages: Number(tally.messages) || 0
      };
    }
  } catch { /* fresh tally */ }
  return { start: null, finish: null, polls: 0, chatters: 0, messages: 0 };
}

function writeChatTally(tally) {
  try {
    fs.writeFileSync(CHAT_TALLY_PATH, JSON.stringify(tally), 'utf8');
  } catch (error) {
    console.warn('[Chat] Could not persist run tally:', error.message);
  }
}

// One summary line per poll run, and one rolling tally for the worker window.
function recordRunSummary(runStats, runStartIso) {
  const runFinishIso = new Date().toISOString();
  appendChatLog(
    'summary',
    `added ${runStats.chatters} chatters in 1 poll and ${runStats.messages} messages ` +
    `(${runStartIso} -> ${runFinishIso} UTC)`
  );

  const tally = readChatTally();
  tally.start = tally.start || runStartIso;
  tally.finish = runFinishIso;
  tally.polls += 1;
  tally.chatters += runStats.chatters;
  tally.messages += runStats.messages;
  writeChatTally(tally);
}

async function collectAndLogChat(data, fallbackTag) {
  const tag = data?.slug || fallbackTag || String(data?.id || 'unknown');
  const live = isLiveChannelPayload(data);

  if (!live) {
    return { savedMessages: 0, discoveredUsers: 0 };
  }

  try {
    const result = await fetchLiveChatHistory(data);
    // Per-channel lines removed on purpose: run summaries replace the spam.
    return result;
  } catch (error) {
    appendChatLog(tag, `error - ${error.message}`);
    throw error;
  }
}

async function getKnownTrackedHandles() {
  const rows = await allQuery(`
    SELECT DISTINCT LOWER(current_slug) AS slug, LOWER(current_username) AS username
    FROM channels
    WHERE current_slug IS NOT NULL OR current_username IS NOT NULL
    UNION
    SELECT DISTINCT LOWER(slug), LOWER(username)
    FROM username_history
    WHERE slug IS NOT NULL OR username IS NOT NULL
  `);

  const tracked = new Set();

  for (const row of rows) {
    if (row.slug) tracked.add(String(row.slug).trim().replace(/^@/, ''));
    if (row.username) tracked.add(String(row.username).trim().replace(/^@/, ''));
  }

  return tracked;
}

import fs from 'fs';

const BACKUP_KEEP_COUNT = 2;
const BACKUP_MIN_INTERVAL_MS = 7 * 24 * 60 * 60 * 1000; // once a week

function getBackupFiles() {
  if (!fs.existsSync('./backups')) return [];
  return fs.readdirSync('./backups')
    .filter(name => name.startsWith('kick_tracker-') && name.endsWith('.db'))
    .map(name => {
      const fullPath = `./backups/${name}`;
      try {
        return { name, fullPath, mtimeMs: fs.statSync(fullPath).mtimeMs };
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .sort((a, b) => b.mtimeMs - a.mtimeMs); // newest first
}

function pruneOldBackups() {
  const files = getBackupFiles();
  const extras = files.slice(BACKUP_KEEP_COUNT);
  for (const file of extras) {
    try {
      fs.unlinkSync(file.fullPath);
      console.log(`[DB] Pruned old backup: ${file.fullPath}`);
    } catch (error) {
      console.warn(`[DB] Could not prune backup ${file.fullPath}:`, error.message);
    }
  }
}

function createDatabaseBackup() {
  if (!fs.existsSync('./kick_tracker.db')) return null;

  fs.mkdirSync('./backups', { recursive: true });

  const existing = getBackupFiles();

  // Only back up once a week: skip if the newest backup is fresh.
  if (existing.length > 0 && (Date.now() - existing[0].mtimeMs) < BACKUP_MIN_INTERVAL_MS) {
    pruneOldBackups(); // still enforce the 2-file limit on every run
    return null;
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const backupPath = `./backups/kick_tracker-${timestamp}.db`;
  fs.copyFileSync('./kick_tracker.db', backupPath);
  console.log(`[DB] Backup created: ${backupPath}`);

  pruneOldBackups(); // keep only the current + latest backup
  return backupPath;
}

async function refreshAllTrackedUsers() {
  const rows = await allQuery(`
    SELECT current_slug
    FROM channels
    WHERE current_slug IS NOT NULL AND current_slug != ''
    ORDER BY id ASC
  `);
  console.log(`[REFRESH-ALL] Refreshing ${rows.length} tracked users (profile data only, no chat)...`);

  let ok = 0;
  let failed = 0;
  for (const row of rows) {
    const tag = String(row.current_slug).toLowerCase().replace(/^@/, '').trim();
    if (!tag) continue;
    try {
      const payload = await fetchChannelByTag(tag);
      await processChannelPayload(payload);
      ok += 1;
    } catch (error) {
      failed += 1;
      console.warn(`[REFRESH-ALL] @${tag}: ${error.message}`);
    }
    // Small pause to stay friendly to the Kick API.
    await new Promise(resolve => setTimeout(resolve, 400));
  }

  // Compact the tracked database before this run commits it to git.
  await runChatStorageMaintenance();

  console.log(`[REFRESH-ALL] Done. Updated: ${ok}, failed: ${failed}.`);
}

// --- Chat storage maintenance -------------------------------------------------
// kick_tracker.db is committed to git on every push, so it must stay well below
// GitHub's hard per-file limit ("GitHub blocks files larger than 100 MiB",
// 104 857 600 bytes). On 2026-09-15 the file reached 104 435 712 bytes (99.6 MiB)
// and then crossed that limit a few minutes later, at which point GitHub started
// rejecting EVERY push. The worker kept collecting, the job still finished
// "successfully" (pushes are non-fatal on purpose), and nothing reached
// origin/main again - which looks exactly like "the auto cycle can't push".
// This maintenance pass keeps the tracked file comfortably small:
//   1. migrates legacy fat chat payloads to the slim shape above,
//   2. drops chat messages past the retention window,
//   3. enforces a hard size cap (oldest messages first) with a VACUUM.
const GITHUB_FILE_SIZE_LIMIT_BYTES = 100 * 1024 * 1024;
const DATABASE_PATH = './kick_tracker.db';
const MAINTENANCE_STATE_PATH = './.chat-maintenance.json';
const CHAT_RETENTION_DAYS = Number.parseFloat(process.env.CHAT_RETENTION_DAYS ?? '7');
// `CHAT_RETENTION_DAYS=0` (or `unlimited`) disables age-based pruning entirely,
// so chat history is kept forever; the size cap + archiving still apply.
const CHAT_RETENTION_UNLIMITED = !Number.isFinite(CHAT_RETENTION_DAYS) || CHAT_RETENTION_DAYS <= 0;
const CHAT_MAX_DB_BYTES = Math.max(1, Number.parseFloat(process.env.CHAT_MAX_DB_MB ?? '70')) * 1024 * 1024;
const PAYLOAD_MIGRATION_INTERVAL_MS = 15 * 60 * 1000;
const VACUUM_MIN_RECLAIM_BYTES = 4 * 1024 * 1024;
// Shard archive: when the main DB grows past CHAT_MAX_DB_BYTES, oldest messages
// are moved into archive/ shard files instead of being deleted. Each shard is
// rolled over well below GitHub's 100 MiB per-file limit.
const ARCHIVE_DIR = './archive';
const ARCHIVE_SHARD_MAX_BYTES = 90 * 1024 * 1024;

function getDatabaseSizeBytes() {
  try {
    return fs.statSync(DATABASE_PATH).size;
  } catch {
    return 0;
  }
}

function formatMiB(bytes) {
  return `${(bytes / 1048576).toFixed(1)} MiB`;
}

function readMaintenanceState() {
  try {
    return JSON.parse(fs.readFileSync(MAINTENANCE_STATE_PATH, 'utf8'));
  } catch {
    return {};
  }
}

function writeMaintenanceState(state) {
  try {
    fs.writeFileSync(MAINTENANCE_STATE_PATH, JSON.stringify(state), 'utf8');
  } catch (error) {
    console.warn('[DB] Could not persist maintenance state:', error.message);
  }
}

async function getReclaimableBytes() {
  try {
    const pageCount = Number((await getQuery('PRAGMA page_count'))?.page_count) || 0;
    const freePages = Number((await getQuery('PRAGMA freelist_count'))?.freelist_count) || 0;
    const pageSize = Number((await getQuery('PRAGMA page_size'))?.page_size) || 4096;
    return Math.min(freePages * pageSize, pageCount * pageSize);
  } catch {
    return 0;
  }
}

async function vacuumDatabase() {
  await runQuery('VACUUM');
}

async function migrateLegacyChatPayloads() {
  const rows = await allQuery(`
    SELECT rowid AS row_id, raw_payload
    FROM chat_messages
    WHERE raw_payload LIKE '%"metadata"%' OR LENGTH(raw_payload) > 400
  `);

  let migrated = 0;
  // Batched inside transactions on purpose: 77k single-row commits means one
  // journal write each, which took minutes and would make the per-poll and
  // pre-push maintenance calls useless.
  await runQuery('BEGIN');
  try {
    for (const row of rows) {
      let slim = null;
      try {
        slim = JSON.stringify(slimChatMessage(JSON.parse(row.raw_payload)));
      } catch {
        continue;
      }
      if (!slim || slim === 'null' || slim.length >= String(row.raw_payload).length) continue;
      await runQuery('UPDATE chat_messages SET raw_payload = ? WHERE rowid = ?', [slim, row.row_id]);
      migrated += 1;
      if (migrated % 10000 === 0) {
        await runQuery('COMMIT');
        await runQuery('BEGIN');
      }
    }
    await runQuery('COMMIT');
  } catch (error) {
    try { await runQuery('COMMIT'); } catch { /* transaction already closed */ }
    throw error;
  }

  return migrated;
}

async function pruneChatMessagesByAge(retentionDays) {
  if (!Number.isFinite(retentionDays) || retentionDays <= 0) return 0;
  const result = await runQuery(
    "DELETE FROM chat_messages WHERE saved_at < datetime('now', ?)",
    [`-${retentionDays} day`]
  );
  return result?.changes || 0;
}

async function enforceChatSizeCap(maxBytes) {
  let deleted = 0;
  let size = getDatabaseSizeBytes();

  for (let pass = 0; pass < 4 && size > maxBytes; pass += 1) {
    const total = Number((await getQuery('SELECT COUNT(*) AS total FROM chat_messages'))?.total) || 0;
    if (total === 0) break;

    const bytesPerRow = Math.max(1, Math.floor(size / total));
    const overBy = size - maxBytes;
    const batch = Math.min(total, Math.ceil(overBy / bytesPerRow) + 500);

    const result = await runQuery(`
      DELETE FROM chat_messages WHERE rowid IN (
        SELECT rowid FROM chat_messages ORDER BY saved_at ASC, rowid ASC LIMIT ?
      )
    `, [batch]);
    if (!result?.changes) break;

    deleted += result.changes;
    await vacuumDatabase();
    size = getDatabaseSizeBytes();
  }

  return deleted;
}

async function archiveOldestMessages(maxBytes) {
  // Instead of deleting history when the tracked database grows past the cap,
  // move the oldest chat messages into shard files (archive/kick_tracker-NNN.db).
  // Each shard stays well below GitHub's 100 MiB per-file push limit, so the
  // archive can grow essentially forever without ever blocking a push.
  let archived = 0;
  let size = getDatabaseSizeBytes();
  if (size <= maxBytes) return 0;

  fs.mkdirSync(ARCHIVE_DIR, { recursive: true });

  const state = readMaintenanceState();
  let shardName = typeof state.archiveShard === 'string' && /^kick_tracker-\d+\.db$/.test(state.archiveShard)
    ? state.archiveShard
    : null;

  if (!shardName) {
    const existing = fs.readdirSync(ARCHIVE_DIR)
      .map(name => /^kick_tracker-(\d+)\.db$/.exec(name))
      .filter(Boolean)
      .map(match => Number(match[1]))
      .sort((a, b) => b - a);
    shardName = `kick_tracker-${String((existing[0] || 0) + 1).padStart(3, '0')}.db`;
  }

  // The path is generated locally and validated above, so it is safe to inline.
  const shardPath = `${ARCHIVE_DIR}/${shardName}`;
  const safePath = shardPath.replace(/'/g, "''");

  await runQuery(`ATTACH DATABASE '${safePath}' AS archive_shard`);
  try {
    await runQuery(`
      CREATE TABLE IF NOT EXISTS archive_shard.chat_messages (
        message_id TEXT PRIMARY KEY,
        chat_id INTEGER NOT NULL,
        sender_user_id INTEGER,
        sender_slug TEXT,
        sender_username TEXT,
        content TEXT,
        message_type TEXT,
        created_at TIMESTAMP,
        raw_payload TEXT,
        saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await runQuery(`
      CREATE INDEX IF NOT EXISTS archive_shard.idx_chat_messages_sender
      ON chat_messages (sender_user_id)
    `);

    for (let pass = 0; pass < 200 && size > maxBytes; pass += 1) {
      const shardBytes = fs.existsSync(shardPath) ? fs.statSync(shardPath).size : 0;
      if (shardBytes >= ARCHIVE_SHARD_MAX_BYTES) {
        // This shard is full: roll over to the next numbered shard.
        const existing = fs.readdirSync(ARCHIVE_DIR)
          .map(name => /^kick_tracker-(\d+)\.db$/.exec(name))
          .filter(Boolean)
          .map(match => Number(match[1]))
          .sort((a, b) => b - a);
        shardName = `kick_tracker-${String((existing[0] || 0) + 1).padStart(3, '0')}.db`;
        await runQuery(`DETACH DATABASE archive_shard`);
        await runQuery(`ATTACH DATABASE '${`${ARCHIVE_DIR}/${shardName}`.replace(/'/g, "''")}' AS archive_shard`);
        await runQuery(`
          CREATE TABLE IF NOT EXISTS archive_shard.chat_messages (
            message_id TEXT PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            sender_user_id INTEGER,
            sender_slug TEXT,
            sender_username TEXT,
            content TEXT,
            message_type TEXT,
            created_at TIMESTAMP,
            raw_payload TEXT,
            saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )
        `);
        await runQuery(`
          CREATE INDEX IF NOT EXISTS archive_shard.idx_chat_messages_sender
          ON chat_messages (sender_user_id)
        `);
      }

      const total = Number((await getQuery('SELECT COUNT(*) AS total FROM chat_messages'))?.total) || 0;
      if (total === 0) break;

      const bytesPerRow = Math.max(1, Math.floor(size / total));
      const overBy = size - maxBytes;
      const batch = Math.min(total, Math.max(2000, Math.ceil(overBy / bytesPerRow) + 1000));

      const rows = await allQuery(`
        SELECT rowid AS row_id, * FROM chat_messages ORDER BY saved_at ASC, rowid ASC LIMIT ?
      `, [batch]);
      if (!rows.length) break;

      await runQuery('BEGIN');
      try {
        for (const row of rows) {
          await runQuery(`
            INSERT OR IGNORE INTO archive_shard.chat_messages (
              message_id, chat_id, sender_user_id, sender_slug, sender_username,
              content, message_type, created_at, raw_payload, saved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `, [
            row.message_id, row.chat_id, row.sender_user_id, row.sender_slug,
            row.sender_username, row.content, row.message_type, row.created_at,
            row.raw_payload, row.saved_at
          ]);
          await runQuery('DELETE FROM chat_messages WHERE rowid = ?', [row.row_id]);
        }
        await runQuery('COMMIT');
      } catch (error) {
        try { await runQuery('ROLLBACK'); } catch { /* no open transaction */ }
        throw error;
      }

      archived += rows.length;
      await vacuumDatabase();
      size = getDatabaseSizeBytes();
    }
  } finally {
    // Only detach if still attached (a shard rollover detaches first).
    try {
      await runQuery(`DETACH DATABASE archive_shard`);
    } catch { /* already detached */ }
  }

  writeMaintenanceState({ ...readMaintenanceState(), archiveShard: shardName });
  if (archived > 0) {
    console.log(`[DB] archived ${archived} message(s) into archive shards (latest: ${shardName}); main DB is now ${formatMiB(size)}`);
  }
  return archived;
}

async function runChatStorageMaintenance({ force = false, quiet = false } = {}) {
  const startedAt = Date.now();
  const before = getDatabaseSizeBytes();
  const state = readMaintenanceState();

  let migrated = 0;
  // The legacy-payload scan is a full table scan, so only pay for it on a
  // forced run (npm run prune) or every PAYLOAD_MIGRATION_INTERVAL_MS.
  if (force || (startedAt - (Number(state.payloadMigrationAt) || 0)) >= PAYLOAD_MIGRATION_INTERVAL_MS) {
    migrated = await migrateLegacyChatPayloads();
    writeMaintenanceState({ ...state, payloadMigrationAt: startedAt });
  }

  const aged = CHAT_RETENTION_UNLIMITED ? 0 : await pruneChatMessagesByAge(CHAT_RETENTION_DAYS);
  const reclaimable = await getReclaimableBytes();
  if ((migrated > 0 || aged > 0) && reclaimable >= VACUUM_MIN_RECLAIM_BYTES) {
    await vacuumDatabase();
  }

  let archived = 0;
  let capped = 0;
  if (getDatabaseSizeBytes() > CHAT_MAX_DB_BYTES) {
    // First choice: move the oldest messages into archive shards (nothing is
    // lost). Deletion only happens if archiving itself failed.
    try {
      archived = await archiveOldestMessages(CHAT_MAX_DB_BYTES);
    } catch (error) {
      console.warn('[DB] archiving failed, falling back to deletion:', error.message);
      try { await vacuumDatabase(); } catch { /* ignore */ }
    }
    if (getDatabaseSizeBytes() > CHAT_MAX_DB_BYTES) {
      capped = await enforceChatSizeCap(CHAT_MAX_DB_BYTES);
    }
  }

  const after = getDatabaseSizeBytes();
  if (!quiet && (migrated > 0 || aged > 0 || archived > 0 || capped > 0)) {
    console.log(
      `[DB] chat storage: slimmed ${migrated} legacy payload(s), ` +
      (CHAT_RETENTION_UNLIMITED ? 'retention unlimited, ' : `pruned ${aged} message(s) past ${CHAT_RETENTION_DAYS}d retention, `) +
      `archived ${archived} message(s) to shards, pruned ${capped} message(s) over the ${formatMiB(CHAT_MAX_DB_BYTES)} cap; ` +
      `${formatMiB(before)} -> ${formatMiB(after)}`
    );
  }

  return { migrated, aged, archived, capped, before, after, overPushLimit: after > GITHUB_FILE_SIZE_LIMIT_BYTES };
}

async function main() {
  await initDb();

  if (process.argv[2] === '--backfill-snapshots') {
    console.log('[DB] Missing current follower snapshots backfilled without fetching targets.');
    db.close();
    return;
  }

  if (process.argv[2] === '--prune') {
    const result = await runChatStorageMaintenance({ force: true });
    const status = result.overPushLimit
      ? 'OVER GitHub\'s 100 MiB per-file push limit'
      : 'within GitHub\'s 100 MiB per-file push limit';
    console.log(`[DB] kick_tracker.db is ${formatMiB(result.after)} (${status})`);
    db.close();
    if (result.overPushLimit) process.exitCode = 1;
    return;
  }

  if (process.argv[2] === '--archive') {
    // Force the archive pass: move the oldest messages into shard files until
    // the tracked database is back under the size cap. Never deletes messages.
    const result = await runChatStorageMaintenance({ force: true });
    console.log(`[DB] archive pass done: ${result.archived} message(s) moved to shards; kick_tracker.db is ${formatMiB(result.after)}`);
    db.close();
    return;
  }

  if (process.argv[2] === '--monitor') {
    await monitorTargets();
    return;
  }

  if (process.argv[2] === '--check-streamers') {
    await checkStreamerTagsOnce();
    db.close();
    return;
  }

  if (process.argv[2] === '--refresh-all') {
    await refreshAllTrackedUsers();
    db.close();
    return;
  }

  const refreshTarget = process.argv[2] === '--refresh' ? process.argv[3] : null;
  const targetsFile = fs.existsSync('./targets.txt') ? fs.readFileSync('./targets.txt', 'utf8') : '';
  const rawTargets = refreshTarget
    ? [refreshTarget]
    : targetsFile.split('\n').map(t => t.trim()).filter(t => t.length > 0);
  const trackedHandles = await getKnownTrackedHandles();

  const targets = [];
  const seenTargets = new Set();

  for (const target of rawTargets) {
    const normalized = target.toLowerCase().replace(/^@/, '').trim();
    if (!normalized) continue;
    if (!refreshTarget && trackedHandles.has(normalized)) {
      console.log(`[skip] Already tracked in database: ${target}`);
      continue;
    }
    if (seenTargets.has(normalized)) {
      continue;
    }

    seenTargets.add(normalized);
    targets.push(target);
  }

  if (targets.length === 0) {
    console.log("No targets found in targets.txt");
    db.close();
    return;
  }

  createDatabaseBackup();

  if (refreshTarget) {
    try {
      console.log(`Fetching target: ${refreshTarget}...`);
      const response = await fetch(`https://kick.com/api/v1/channels/${encodeURIComponent(refreshTarget)}`, {
        headers: { 'User-Agent': 'KickIntel Tracker/1.0' }
      });
      if (!response.ok) throw new Error(`Kick API returned ${response.status}`);
      const data = await response.json();
      if (!data || !data.id) throw new Error('Kick returned an invalid channel payload');
      await processChannelPayload(data);
      try {
        const chatResult = await collectAndLogChat(data, refreshTarget);
        if (chatResult.savedMessages || chatResult.discoveredUsers) {
          console.log(`[CHAT] Saved ${chatResult.savedMessages} messages and discovered ${chatResult.discoveredUsers} users for @${data.slug}`);
        }
      } catch (chatError) {
        console.warn(`[CHAT] Could not save live chat for @${data.slug}:`, chatError.message);
      }
      db.close();
      console.log('Target refresh finished successfully.');
      return;
    } catch (error) {
      db.close();
      console.error(`[X] Error refreshing target ${refreshTarget}:`, error.message);
      process.exitCode = 1;
      return;
    }
  }

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

  for (const target of targets) {
    try {
      console.log(`Fetching target: ${target}...`);
      await page.goto(`https://kick.com/api/v1/channels/${encodeURIComponent(target)}`, { waitUntil: 'networkidle2', timeout: 15000 });
      
      const content = await page.evaluate(() => document.body.innerText);
      const data = JSON.parse(content);

      if (data && data.id) {
        await processChannelPayload(data);
        try {
          const chatResult = await collectAndLogChat(data, target);
          if (chatResult.savedMessages || chatResult.discoveredUsers) {
            console.log(`[CHAT] Saved ${chatResult.savedMessages} messages and discovered ${chatResult.discoveredUsers} users for @${data.slug}`);
          }
        } catch (chatError) {
          console.warn(`[CHAT] Could not save live chat for @${data.slug}:`, chatError.message);
        }
      } else {
        console.log(`[-] Could not resolve payload for target: ${target}`);
      }
    } catch (err) {
      console.error(`[X] Error scraping target ${target}:`, err.message);
    }
  }

  await browser.close();
  db.close();
  console.log("Tracking iteration finished successfully.");
}

function getStreamerTags() {
  const streamersFile = fs.existsSync('./streamers.txt') ? fs.readFileSync('./streamers.txt', 'utf8') : '';
  const seen = new Set();
  return streamersFile.split('\n')
    .map(target => target.toLowerCase().replace(/^@/, '').trim())
    .filter(target => target && !target.startsWith('#') && !seen.has(target) && seen.add(target));
}

// Kick API request throttle: keep a minimum gap between requests and back off on 429s.
const KICK_MIN_REQUEST_GAP_MS = 300;
let kickLastRequestAt = 0;

async function throttleKickRequest() {
  const now = Date.now();
  const waitMs = kickLastRequestAt + KICK_MIN_REQUEST_GAP_MS - now;
  if (waitMs > 0) await new Promise(resolve => setTimeout(resolve, waitMs));
  kickLastRequestAt = Date.now();
}

async function fetchChannelByTag(tag) {
  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    await throttleKickRequest();
    const response = await fetch(`https://kick.com/api/v1/channels/${encodeURIComponent(tag)}`, {
      headers: { 'User-Agent': 'KickIntel Tracker/1.0', Accept: 'application/json' }
    });
    if (response.ok) {
      const payload = await response.json();
      if (!payload?.id) throw new Error('Kick returned an invalid channel payload');
      return payload;
    }
    // Rate limited: wait and retry before giving up on this tag.
    if (response.status === 429 && attempt < maxAttempts) {
      const backoffMs = attempt * 5000;
      console.warn(`[RATE] @${tag}: 429 from Kick API, retrying in ${backoffMs / 1000}s...`);
      await new Promise(resolve => setTimeout(resolve, backoffMs));
      continue;
    }
    throw new Error(`Kick API returned ${response.status}`);
  }
}

async function monitorTargets() {
  const intervalMs = Math.max(15000, Number(process.env.MONITOR_INTERVAL_MS || 30000));
  let stopping = false;

  const stop = () => {
    stopping = true;
    console.log('[MONITOR] Stop requested. Finishing the current check...');
  };
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);

  console.log(`[MONITOR] Watching streamers.txt every ${Math.round(intervalMs / 1000)} seconds.`);
  console.log('[MONITOR] Live chat is collected only while Kick reports a streamer as live.');

  while (!stopping) {
    await checkStreamerTagsOnce(() => stopping);

    if (!stopping) {
      await new Promise(resolve => setTimeout(resolve, intervalMs));
    }
  }

  db.close();
  console.log('[MONITOR] Stopped. Historical data was preserved.');
}

async function checkStreamerTagsOnce(shouldStop = () => false) {
  const tags = getStreamerTags();
  console.log(`[MONITOR] Checking ${tags.length} unique streamer tags...`);
  const runStartIso = new Date().toISOString();
  const runStats = { chatters: 0, messages: 0 };

  for (const tag of tags) {
    if (shouldStop()) break;
    try {
      const payload = await fetchChannelByTag(tag);
      await processChannelPayload(payload);
      const chatResult = await collectAndLogChat(payload, tag);
      runStats.chatters += chatResult.discoveredUsers || 0;
      runStats.messages += chatResult.savedMessages || 0;
    } catch (error) {
      console.warn(`[MONITOR] @${tag}: ${error.message}`);
    }
    // Small pause to stay friendly to the Kick API.
    await new Promise(resolve => setTimeout(resolve, 400));
  }

  recordRunSummary(runStats, runStartIso);

  // Runs after every poll cycle: cheap (indexed prune, no VACUUM unless rows
  // were actually freed) and keeps skirting GitHub's 100 MiB per-file push limit.
  try {
    await runChatStorageMaintenance();
  } catch (error) {
    console.warn('[DB] chat storage maintenance failed:', error.message);
  }
}

main();