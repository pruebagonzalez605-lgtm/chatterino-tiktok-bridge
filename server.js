const http = require("node:http");
const { URL } = require("node:url");
const { TikTokLiveConnection, WebcastEvent } = require("tiktok-live-connector");
const { LiveChat } = require("youtube-chat");

const PORT = Number(process.env.CHAT_BRIDGE_PORT || 17427);
const HOST = process.env.CHAT_BRIDGE_HOST || "0.0.0.0";
const BRIDGE_TOKEN = String(process.env.CHAT_BRIDGE_TOKEN || "").trim();
const MAX_BUFFER = 500;
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
    };
    streams.set(key, stream);
  }

  if (
    platform === "tiktok" &&
    !stream.connected &&
    !stream.connecting &&
    Date.now() >= (stream.retryAfter || 0)
  ) {
    connectTikTok(stream);
  }
  if (
    platform === "youtube" &&
    !stream.connected &&
    !stream.connecting &&
    Date.now() >= (stream.retryAfter || 0)
  ) {
    connectYouTube(stream);
  }

  return stream;
}

function pushMessage(stream, user, text, id, type = "chat") {
  const cleanText = String(text || "").trim();
  if (!cleanText) {
    return;
  }

  stream.seq += 1;
  stream.messages.push({
    seq: stream.seq,
    id: id ? String(id) : `${stream.channel}-${stream.seq}`,
    type,
    user: String(user || stream.platform),
    text: cleanText,
    ts: Date.now(),
  });

  if (stream.messages.length > MAX_BUFFER) {
    stream.messages.splice(0, stream.messages.length - MAX_BUFFER);
  }
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
  stream.connecting = true;
  stream.error = "";
  const connection = new TikTokLiveConnection(stream.channel, {
    processInitialData: false,
    enableExtendedGiftInfo: false,
  });
  stream.connection = connection;

  connection.on(WebcastEvent.CHAT, (data) => {
    pushMessage(stream, pickUser(data), pickText(data), data?.msgId || data?.id, "chat");
  });

  connection.on(WebcastEvent.MEMBER, (data) => {
    pushMessage(stream, "TikTok", `${pickUser(data)} se unio`, data?.msgId || data?.id, "event");
  });

  connection.on(WebcastEvent.SOCIAL, (data) => {
    const label = data?.label || data?.socialTypeName || pickText(data);
    if (label) {
      pushMessage(stream, "TikTok", `${pickUser(data)} ${label}`, data?.msgId || data?.id, "event");
    }
  });

  connection.on(WebcastEvent.GIFT, (data) => {
    const giftName = data?.giftName || data?.gift?.name || "regalo";
    pushMessage(stream, "TikTok", `${pickUser(data)} envio ${giftName}`, data?.msgId || data?.id, "event");
  });

  connection.on(WebcastEvent.LIKE, (data) => {
    const count = data?.likeCount || data?.count;
    if (count) {
      pushMessage(stream, "TikTok", `${pickUser(data)} dio ${count} likes`, data?.msgId || data?.id, "event");
    }
  });

  connection.on(WebcastEvent.DISCONNECTED || "disconnected", () => {
    stream.connected = false;
    stream.connecting = false;
    stream.error = "TikTok disconnected; reconnecting on next poll.";
  });

  connection.on(WebcastEvent.STREAM_END, () => {
    stream.connected = false;
    stream.connecting = false;
    stream.error = "TikTok live ended or is offline.";
  });

  try {
    await connection.connect();
    stream.connected = true;
    stream.error = "";
    pushMessage(stream, "TikTok", `Connected to @${stream.channel}.`, "connected", "status");
  } catch (error) {
    stream.connected = false;
    stream.error = error && error.message ? error.message : String(error);
    stream.retryAfter = Date.now() + 15000;
  } finally {
    stream.connecting = false;
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

  if (BRIDGE_TOKEN && req.headers.authorization !== `Bearer ${BRIDGE_TOKEN}`) {
    json(res, 401, { ok: false, error: "Unauthorized" });
    return;
  }

  if (url.pathname === "/health") {
    json(res, 200, { ok: true });
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
    latestSeq: stream.seq,
    messages: stream.messages.filter((message) => message.seq > since),
  });
});

server.listen(PORT, HOST, () => {
  console.log(`Chat bridge listening on http://${HOST}:${PORT}`);
});
