const http = require("node:http");
const { URL } = require("node:url");
const {
  TikTokLiveConnection,
  WebcastEvent,
  ControlEvent,
} = require("tiktok-live-connector");
const { LiveChat } = require("youtube-chat");

const PORT = Number(process.env.PORT || process.env.CHAT_BRIDGE_PORT || 17427);
const HOST = process.env.CHAT_BRIDGE_HOST || "0.0.0.0";
const BRIDGE_TOKEN = String(process.env.CHAT_BRIDGE_TOKEN || "").trim();
const MAX_BUFFER = 500;
const MAX_SEEN_MESSAGE_IDS = 5000;
const RETRY_MIN_MS = 2500;
const RETRY_MAX_MS = 120000;
const OFFLINE_RETRY_MS = 60000;
const TIKTOK_CONNECT_TIMEOUT_MS = 30000;
const TIKTOK_STALE_AFTER_MS = 150000;
const STREAM_IDLE_TTL_MS = 15 * 60 * 1000;
const streams = new Map();

process.on("unhandledRejection", (error) => {
  console.error("Unhandled bridge rejection:", error);
});

process.on("uncaughtException", (error) => {
  console.error("Unhandled bridge exception:", error);
});

function streamKey(platform, channel) {
  const normalizedChannel =
    platform === "youtube" ? String(channel) : String(channel).toLowerCase();
  return `${platform}:${normalizedChannel}`;
}

function getStream(platform, channel) {
  const key = streamKey(platform, channel);
  let stream = streams.get(key);

  if (!stream) {
    stream = {
      platform,
      channel,
      connected: false,
      connecting: false,
      seq: 0,
      messages: [],
      error: "",
      connection: null,
      retryAfter: 0,
      retryTimer: null,
      connectTimeoutTimer: null,
      watchdogTimer: null,
      reconnectAttempt: 0,
      generation: 0,
      lastActivityAt: 0,
      lastPollAt: Date.now(),
      seenMessageIds: new Set(),
    };
    streams.set(key, stream);
  }

  stream.lastPollAt = Date.now();

  if (
    !stream.connected &&
    !stream.connecting &&
    !stream.retryTimer &&
    Date.now() >= stream.retryAfter
  ) {
    if (platform === "tiktok") connectTikTok(stream);
    if (platform === "youtube") connectYouTube(stream);
  }

  return stream;
}

function pushMessage(stream, user, text, id, type = "chat", kind = "") {
  const cleanText = String(text || "").trim();
  if (!cleanText) {
    return;
  }

  const messageId = id ? String(id) : `${stream.channel}-${stream.seq + 1}`;
  // TikTok may replay recent events after a reconnect. Do not show the same
  // chat or activity event twice, while keeping connection status visible.
  if (type !== "status" && stream.seenMessageIds.has(messageId)) {
    return;
  }
  if (type !== "status") {
    stream.seenMessageIds.add(messageId);
    if (stream.seenMessageIds.size > MAX_SEEN_MESSAGE_IDS) {
      stream.seenMessageIds.delete(stream.seenMessageIds.values().next().value);
    }
  }

  stream.seq += 1;
  stream.messages.push({
    seq: stream.seq,
    id: messageId,
    type,
    user: String(user || stream.platform),
    text: cleanText,
    kind: kind || type,
    ts: Date.now(),
  });

  if (stream.messages.length > MAX_BUFFER) {
    stream.messages.splice(0, stream.messages.length - MAX_BUFFER);
  }
}

function errorText(error) {
  return error && error.message ? error.message : String(error || "Unknown error");
}

function isCurrent(stream, connection, generation) {
  return stream.connection === connection && stream.generation === generation;
}

function stopTimer(stream, name) {
  if (stream[name]) {
    clearTimeout(stream[name]);
    stream[name] = null;
  }
}

function closeConnection(connection) {
  try {
    const closeResult = connection?.disconnect?.();
    if (closeResult?.catch) closeResult.catch(() => {});
  } catch {
    // It is already closed or the connector rejected a duplicate disconnect.
  }
}

function retryDelay(stream, offline = false) {
  if (offline) return OFFLINE_RETRY_MS;
  const exponential = Math.min(
    RETRY_MAX_MS,
    RETRY_MIN_MS * 2 ** Math.min(stream.reconnectAttempt, 6),
  );
  return Math.round(exponential * (0.8 + Math.random() * 0.4));
}

function scheduleReconnect(stream, reason, { offline = false } = {}) {
  if (stream.platform !== "tiktok") return;
  // An error often triggers a following disconnected event. The first retry
  // wins so those two notifications don't multiply the backoff delay.
  if (stream.retryTimer) return;
  stopTimer(stream, "connectTimeoutTimer");
  stopTimer(stream, "watchdogTimer");
  // Retire the previous connection before retrying. A few network failures
  // leave connect() pending forever; its late events must not revive it.
  const previousConnection = stream.connection;
  stream.generation += 1;
  stream.connection = null;
  closeConnection(previousConnection);
  stream.connected = false;
  stream.connecting = false;
  stream.error = reason;
  stream.reconnectAttempt += 1;
  const delay = retryDelay(stream, offline);
  stream.retryAfter = Date.now() + delay;
  stream.retryTimer = setTimeout(() => {
    stream.retryTimer = null;
    if (!stream.connected && !stream.connecting) connectTikTok(stream);
  }, delay);
}

function armTikTokWatchdog(stream, connection, generation) {
  stopTimer(stream, "watchdogTimer");
  const check = () => {
    if (!isCurrent(stream, connection, generation) || !stream.connected) return;
    if (Date.now() - stream.lastActivityAt >= TIKTOK_STALE_AFTER_MS) {
      closeConnection(connection);
      scheduleReconnect(stream, "TikTok connection became stale; reconnecting.");
      return;
    }
    stream.watchdogTimer = setTimeout(check, TIKTOK_STALE_AFTER_MS);
  };
  stream.watchdogTimer = setTimeout(check, TIKTOK_STALE_AFTER_MS);
}

function disposeStream(stream) {
  stopTimer(stream, "retryTimer");
  stopTimer(stream, "connectTimeoutTimer");
  stopTimer(stream, "watchdogTimer");
  stream.generation += 1;
  stream.connected = false;
  stream.connecting = false;
  closeConnection(stream.connection);
  try {
    const stopResult = stream.connection?.stop?.();
    if (stopResult?.catch) stopResult.catch(() => {});
  } catch {
    // The source is already stopped.
  }
  stream.connection = null;
}

function pickUser(data) {
  return (
    data?.nickname ||
    data?.uniqueId ||
    data?.user?.nickname ||
    data?.user?.uniqueId ||
    data?.common?.displayText?.displayText ||
    "TikTok"
  );
}

function pickText(data) {
  return (
    data?.comment ||
    data?.text ||
    data?.content ||
    data?.chatMessage?.content ||
    data?.chatMessage?.comment ||
    data?.displayText ||
    data?.common?.displayText?.displayText ||
    ""
  );
}

async function connectTikTok(stream) {
  stopTimer(stream, "retryTimer");
  stopTimer(stream, "connectTimeoutTimer");
  stopTimer(stream, "watchdogTimer");
  stream.connecting = true;
  stream.error = "";
  const generation = ++stream.generation;
  const connection = new TikTokLiveConnection(stream.channel, {
    processInitialData: false,
    enableExtendedGiftInfo: false,
  });
  stream.connection = connection;

  // Some networks or TikTok edge servers keep the initial request open
  // without emitting an error. Turn that into a visible retry instead of
  // leaving the channel in "connecting" indefinitely.
  stream.connectTimeoutTimer = setTimeout(() => {
    if (!isCurrent(stream, connection, generation) || !stream.connecting) return;
    scheduleReconnect(
      stream,
      `TikTok connection timed out after ${Math.round(TIKTOK_CONNECT_TIMEOUT_MS / 1000)} seconds; retrying.`,
    );
  }, TIKTOK_CONNECT_TIMEOUT_MS);

  connection.on(WebcastEvent.CHAT, (data) => {
    if (!isCurrent(stream, connection, generation)) return;
    stream.lastActivityAt = Date.now();
    pushMessage(stream, pickUser(data), pickText(data), data?.msgId || data?.id, "chat");
  });

  connection.on(WebcastEvent.MEMBER, (data) => {
    if (!isCurrent(stream, connection, generation)) return;
    stream.lastActivityAt = Date.now();
    pushMessage(stream, "TikTok", `${pickUser(data)} se unio`, data?.msgId || data?.id, "event", "member");
  });

  connection.on(WebcastEvent.SOCIAL, (data) => {
    if (!isCurrent(stream, connection, generation)) return;
    stream.lastActivityAt = Date.now();
    const label = data?.label || data?.socialTypeName || pickText(data);
    if (label) {
      pushMessage(stream, "TikTok", `${pickUser(data)} ${label}`, data?.msgId || data?.id, "event", "social");
    }
  });

  connection.on(WebcastEvent.GIFT, (data) => {
    if (!isCurrent(stream, connection, generation)) return;
    stream.lastActivityAt = Date.now();
    const giftName = data?.giftName || data?.gift?.name || "regalo";
    pushMessage(stream, "TikTok", `${pickUser(data)} envio ${giftName}`, data?.msgId || data?.id, "event", "gift");
  });

  connection.on(WebcastEvent.LIKE, (data) => {
    if (!isCurrent(stream, connection, generation)) return;
    stream.lastActivityAt = Date.now();
    const count = data?.likeCount || data?.count;
    if (count) {
      pushMessage(stream, "TikTok", `${pickUser(data)} dio ${count} likes`, data?.msgId || data?.id, "event", "like");
    }
  });

  connection.on(ControlEvent.CONNECTED, () => {
    if (!isCurrent(stream, connection, generation)) return;
    stream.lastActivityAt = Date.now();
  });

  connection.on(ControlEvent.WEBSOCKET_DATA, () => {
    if (!isCurrent(stream, connection, generation)) return;
    stream.lastActivityAt = Date.now();
  });

  connection.on(ControlEvent.ERROR, (error) => {
    if (!isCurrent(stream, connection, generation)) return;
    closeConnection(connection);
    scheduleReconnect(stream, `TikTok error: ${errorText(error?.exception || error)}`);
  });

  connection.on(ControlEvent.DISCONNECTED, ({ code, reason } = {}) => {
    if (!isCurrent(stream, connection, generation)) return;
    const detail = reason ? `: ${reason}` : code ? ` (code ${code})` : "";
    scheduleReconnect(stream, `TikTok disconnected${detail}; reconnecting.`);
  });

  connection.on(WebcastEvent.STREAM_END, () => {
    if (!isCurrent(stream, connection, generation)) return;
    closeConnection(connection);
    scheduleReconnect(stream, "TikTok live ended or is offline. Retrying shortly.", {
      offline: true,
    });
  });

  try {
    await connection.connect();
    if (!isCurrent(stream, connection, generation)) {
      closeConnection(connection);
      return;
    }
    stopTimer(stream, "connectTimeoutTimer");
    stream.connected = true;
    stream.connecting = false;
    stream.reconnectAttempt = 0;
    stream.retryAfter = 0;
    stream.lastActivityAt = Date.now();
    stream.error = "";
    armTikTokWatchdog(stream, connection, generation);
    pushMessage(
      stream,
      "TikTok",
      `Connected to @${stream.channel}.`,
      `connected-${generation}`,
      "status",
    );
  } catch (error) {
    if (isCurrent(stream, connection, generation)) {
      closeConnection(connection);
      scheduleReconnect(stream, `TikTok connection failed: ${errorText(error)}`);
    }
  } finally {
    if (isCurrent(stream, connection, generation)) {
      stopTimer(stream, "connectTimeoutTimer");
      stream.connecting = false;
    }
  }
}

function parseYouTubeTarget(channel) {
  const raw = String(channel || "").trim();
  try {
    const url = new URL(raw);
    if (url.hostname.includes("youtu.be")) {
      const id = url.pathname.split("/").filter(Boolean)[0];
      return id ? { liveId: id } : null;
    }

    const videoId = url.searchParams.get("v");
    if (videoId) {
      return { liveId: videoId };
    }

    const parts = url.pathname.split("/").filter(Boolean);
    if (parts[0]?.startsWith("@")) {
      return { handle: parts[0] };
    }

    const liveIndex = parts.indexOf("live");
    if (liveIndex >= 0 && parts[liveIndex + 1]) {
      return { liveId: parts[liveIndex + 1] };
    }

    const channelIndex = parts.indexOf("channel");
    if (channelIndex >= 0 && parts[channelIndex + 1]) {
      return { channelId: parts[channelIndex + 1] };
    }
  } catch {
  }

  if (/^UC[a-zA-Z0-9_-]{20,}$/.test(raw)) {
    return { channelId: raw };
  }

  if (raw.startsWith("@")) {
    return { handle: raw };
  }

  return { liveId: raw };
}

function formatYouTubeMessage(messageItems) {
  if (!Array.isArray(messageItems)) {
    return "";
  }

  return messageItems
    .map((item) => item.text || item.emojiText || item.alt || "")
    .join("")
    .trim();
}

async function connectYouTube(stream) {
  stream.connecting = true;
  stream.error = "";
  stream.timedOut = false;

  const target = parseYouTubeTarget(stream.channel);
  if (!target) {
    stream.connecting = false;
    stream.error = "Invalid YouTube live URL, video ID, or channel ID.";
    return;
  }

  const liveChat = new LiveChat(target);
  stream.connection = liveChat;

  const timeout = setTimeout(() => {
    if (!stream.connected && stream.connecting) {
      stream.timedOut = true;
      stream.connecting = false;
      stream.retryAfter = Date.now() + 30000;
      stream.error = "YouTube did not answer. Check that the URL is live and has chat enabled.";
      pushMessage(stream, "YouTube", stream.error, "timeout", "status");
    }
  }, 12000);

  liveChat.on("start", (liveId) => {
    clearTimeout(timeout);
    stream.timedOut = false;
    stream.connected = true;
    stream.connecting = false;
    stream.retryAfter = 0;
    stream.error = "";
    pushMessage(stream, "YouTube", `Connected to ${liveId}.`, "connected", "status");
  });

  liveChat.on("chat", (chatItem) => {
    const text = formatYouTubeMessage(chatItem?.message);
    const user = chatItem?.author?.name || "YouTube";
    const id = chatItem?.id || chatItem?.timestamp?.getTime?.();
    pushMessage(stream, user, text, id, "chat");
  });

  liveChat.on("end", (reason) => {
    clearTimeout(timeout);
    stream.connected = false;
    stream.connecting = false;
    stream.retryAfter = Date.now() + 30000;
    stream.error = reason ? String(reason) : "YouTube live chat ended.";
    pushMessage(stream, "YouTube", stream.error, "ended", "status");
  });

  liveChat.on("error", (error) => {
    clearTimeout(timeout);
    stream.error = error && error.message ? error.message : String(error);
    if (!stream.connected) {
      stream.connecting = false;
      stream.retryAfter = Date.now() + 30000;
      pushMessage(stream, "YouTube", stream.error, "error", "status");
    }
  });

  try {
    const ok = await liveChat.start();
    clearTimeout(timeout);
    if (!ok) {
      stream.connected = false;
      stream.retryAfter = Date.now() + 30000;
      if (!stream.error) {
        stream.error = "Failed to start YouTube live chat.";
        pushMessage(stream, "YouTube", stream.error, "start-failed", "status");
      }
    }
  } catch (error) {
    clearTimeout(timeout);
    stream.connected = false;
    stream.retryAfter = Date.now() + 30000;
    stream.error = error && error.message ? error.message : String(error);
    pushMessage(stream, "YouTube", stream.error, "exception", "status");
  } finally {
    if (!stream.timedOut) {
      stream.connecting = false;
    }
  }
}

function json(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "access-control-allow-origin": "*",
  });
  res.end(body);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "127.0.0.1"}`);

  // /health stays public so clients can tell "asleep/unreachable" apart
  // from "needs a token"; it exposes no chat data.
  if (url.pathname === "/health") {
    json(res, 200, { ok: true, tokenRequired: Boolean(BRIDGE_TOKEN) });
    return;
  }

  if (BRIDGE_TOKEN && req.headers.authorization !== `Bearer ${BRIDGE_TOKEN}`) {
    json(res, 401, { ok: false, error: "Unauthorized" });
    return;
  }

  if (url.pathname === "/inject") {
    const platform = String(url.searchParams.get("platform") || "tiktok").toLowerCase();
    const channel = String(url.searchParams.get("channel") || "").trim().replace(/^@/, "");
    const user = String(url.searchParams.get("user") || "TikTokTester");
    const text = String(url.searchParams.get("text") || "Probando emblema de TikTok");
    if (!channel) {
      json(res, 400, { ok: false, error: "Missing channel" });
      return;
    }
    const stream = getStream(platform, channel);
    pushMessage(stream, user, text, `inject-${Date.now()}`, "chat");
    json(res, 200, { ok: true, channel, user, text, seq: stream.seq });
    return;
  }

  if (url.pathname !== "/poll") {
    json(res, 404, { ok: false, error: "Not found" });
    return;
  }

  const platform = String(url.searchParams.get("platform") || "").toLowerCase();
  let channel = String(url.searchParams.get("channel") || "").trim();
  const since = Number(url.searchParams.get("since") || 0);

  if (!channel) {
    json(res, 400, { ok: false, error: "Missing channel" });
    return;
  }

  if (platform !== "tiktok" && platform !== "youtube") {
    json(res, 400, { ok: false, error: `Unsupported platform: ${platform}` });
    return;
  }

  if (platform === "tiktok") {
    channel = channel.replace(/^@/, "");
  }

  const stream = getStream(platform, channel);
  json(res, 200, {
    ok: true,
    platform,
    channel,
    connected: stream.connected,
    connecting: stream.connecting,
    error: stream.error,
    reconnectAttempt: stream.reconnectAttempt,
    retryInMs: Math.max(0, (stream.retryAfter || 0) - Date.now()),
    lastActivityAt: stream.lastActivityAt || null,
    latestSeq: stream.seq,
    messages: stream.messages.filter((message) => message.seq > since),
  });
});

const cleanupTimer = setInterval(() => {
  const cutoff = Date.now() - STREAM_IDLE_TTL_MS;
  for (const [key, stream] of streams) {
    if (stream.lastPollAt < cutoff) {
      disposeStream(stream);
      streams.delete(key);
    }
  }
}, 60000);

function shutdown() {
  clearInterval(cleanupTimer);
  for (const stream of streams.values()) disposeStream(stream);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);

server.listen(PORT, HOST, () => {
  console.log(`Chat bridge listening on http://${HOST}:${PORT}`);
});
