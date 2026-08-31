import makeWASocket, { Browsers, DisconnectReason, downloadContentFromMessage, makeCacheableSignalKeyStore, toBuffer, useMultiFileAuthState } from "@whiskeysockets/baileys";
import { createHmac, timingSafeEqual } from "node:crypto";
import { chmod, mkdir, readFile, readdir, rm, unlink } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import http from "node:http";
import { join } from "node:path";
import P from "pino";
import QRCode from "qrcode";
import { pipeline } from "node:stream/promises";

let account;
let qrCode;
let reconnectTimer;
let socket;
let status = "starting";
let connectionGeneration = 0;
const runtime = async () => JSON.parse(await readFile("/codex-home/whatsapp/runtime.json", "utf8"));
const attachmentDir = process.env.CODEX_WHATSAPP_ATTACHMENT_DIR ?? "/codex-home/whatsapp/attachments";
const MAX_ATTACHMENT_BYTES = 50 * 1024 * 1024;
const safeAttachmentExtension = (fileName) => {
  const extension = fileName?.match(/\.[A-Za-z0-9]{1,16}$/u)?.[0];
  return extension ? extension.toLowerCase() : "";
};
const attachmentPath = (message, fileName) => {
  const id = message.key.id.replace(/[^A-Za-z0-9_-]/gu, "_");
  return join(attachmentDir, `${id}${safeAttachmentExtension(fileName)}`);
};
const downloadToFile = async (message, content, mediaType, fileName) => {
  const path = attachmentPath(message, fileName);
  await mkdir(attachmentDir, { recursive: true });
  let bytes = 0;
  const source = await downloadContentFromMessage(content, mediaType);
  const limitedSource = async function* () {
    for await (const chunk of source) {
      bytes += chunk.length;
      if (bytes > MAX_ATTACHMENT_BYTES) throw new Error("attachment exceeds the 50 MiB experiment limit");
      yield chunk;
    }
  };
  try {
    await pipeline(limitedSource(), createWriteStream(path, { flags: "wx", mode: 0o644 }));
    await chmod(path, 0o644);
    return path;
  } catch (error) {
    await unlink(path).catch(() => {});
    throw error;
  }
};
const resetAuth = async () => {
  for (const entry of await readdir("/data/auth")) await rm(`/data/auth/${entry}`, { force: true, recursive: true });
  await mkdir("/data/auth", { recursive: true });
};
const reply = (response, statusCode, body) => { response.writeHead(statusCode, { "content-type": "application/json" }); response.end(JSON.stringify(body)); };
const isAuthorised = (request, config) => {
  const token = request.headers.authorization?.replace(/^Bearer /u, "");
  if (!token) return false;
  const actual = Buffer.from(token);
  const expected = Buffer.from(config.transportApiToken);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
};
const unwrapMessageContent = (content) => {
  let current = content ?? {};
  for (const wrapper of ["ephemeralMessage", "viewOnceMessage", "viewOnceMessageV2", "viewOnceMessageV2Extension", "documentWithCaptionMessage"]) {
    if (current[wrapper]?.message) current = current[wrapper].message;
  }
  return current;
};
const deliver = async (message, client) => {
  const config = await runtime();
  const content = unwrapMessageContent(message.message);
  const image = content.imageMessage;
  const audio = content.audioMessage;
  const document = content.documentMessage;
  const video = content.videoMessage;
  const body = content.conversation ?? content.extendedTextMessage?.text ?? image?.caption ?? document?.caption ?? video?.caption ?? "";
  let attachment;
  if (image) {
    const media = await toBuffer(await downloadContentFromMessage(image, "image"));
    attachment = { type: "image", mimeType: image.mimetype ?? "image/jpeg", dataBase64: media.toString("base64") };
  } else if (audio) {
    attachment = { type: "unsupported", kind: "audio attachment" };
  } else if (video) {
    attachment = { type: "unsupported", kind: "video attachment" };
  } else if (document) {
    const path = await downloadToFile(message, document, "document", document.fileName);
    attachment = { type: "document", fileName: document.fileName ?? null, mimeType: document.mimetype ?? null, path };
  } else if (content.stickerMessage) {
    attachment = { type: "unsupported", kind: "sticker attachment" };
  }
  const chatId = message.key.remoteJid;
  const fromMe = message.key.fromMe === true;
  const isGroup = chatId?.endsWith("@g.us") === true;
  const ownJids = [client.user?.id, client.user?.lid].filter(Boolean);
  const jidAddress = (jid) => jid?.split("@")[0]?.split(":")[0];
  const isSelfChat = fromMe && !isGroup && ownJids.some((jid) => jid === chatId || jidAddress(jid) === jidAddress(chatId));
  console.log(JSON.stringify({ event: "message.received", id: message.key.id, chatId, fromMe, isGroup, isSelfChat, hasText: body.length > 0 }));
  const payload = JSON.stringify({ event: "message", idempotencyKey: message.key.id, data: { body, chatId, fromMe, id: message.key.id, isGroup, isSelfChat, attachment: attachment ?? null } });
  const signature = createHmac("sha256", config.webhookSigningSecret).update(payload).digest("hex");
  const response = await fetch(config.webhookUrl, { body: payload, headers: { "content-type": "application/json", "x-codex-transport-signature": `sha256=${signature}` }, method: "POST", signal: AbortSignal.timeout(10000) });
  console.log(JSON.stringify({ event: "message.delivered", id: message.key.id, status: response.status, accepted: response.ok }));
};
const scheduleReconnect = (delay) => {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = undefined;
    void connect().catch((error) => {
      console.log(JSON.stringify({ event: "session.connect_failed", error: String(error) }));
      status = "disconnected";
      scheduleReconnect(5000);
    });
  }, delay);
};
const connect = async () => {
  const generation = ++connectionGeneration;
  const { saveCreds, state } = await useMultiFileAuthState("/data/auth");
  const nextSocket = makeWASocket({ auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, P({ level: "silent" })) }, browser: Browsers.ubuntu("WhatsApp Codex"), logger: P({ level: "silent" }), markOnlineOnConnect: false, printQRInTerminal: false, syncFullHistory: false });
  socket = nextSocket;
  nextSocket.ev.on("creds.update", saveCreds);
  nextSocket.ev.on("connection.update", (update) => {
    void (async () => {
      if (generation !== connectionGeneration) return;
      if (update.qr) { qrCode = await QRCode.toDataURL(update.qr); status = "pairing"; }
      if (update.connection === "open") { account = nextSocket.user?.id?.split("@")[0]?.split(":")[0]; qrCode = undefined; status = "ready"; }
      if (update.connection === "close") {
        const code = update.lastDisconnect?.error?.output?.statusCode;
        if (code === DisconnectReason.loggedOut) {
          console.log(JSON.stringify({ event: "session.logged_out", action: "reset_auth_and_repair" }));
          status = "pairing";
          qrCode = undefined;
          nextSocket.ev.removeAllListeners("creds.update");
          try {
            await resetAuth();
          } catch (error) {
            console.log(JSON.stringify({ event: "session.auth_reset_failed", error: String(error) }));
            scheduleReconnect(5000);
            return;
          }
          scheduleReconnect(500);
        } else {
          status = "disconnected";
          scheduleReconnect(2000);
        }
      }
    })().catch((error) => {
      console.log(JSON.stringify({ event: "session.update_failed", error: String(error) }));
      status = "disconnected";
      scheduleReconnect(5000);
    });
  });
  nextSocket.ev.on("messages.upsert", async ({ messages, type }) => {
    if (generation !== connectionGeneration) return;
    console.log(JSON.stringify({ event: "messages.upsert", type, count: messages.length }));
    if (type === "notify") {
      for (const message of messages) {
        if (!message.key.id || !message.key.remoteJid || !message.message) continue;
        try {
          await deliver(message, nextSocket);
        } catch (error) {
          console.log(JSON.stringify({ event: "message.delivery_failed", id: message.key.id, error: String(error) }));
        }
      }
    }
  });
};
const server = http.createServer((request, response) => {
  void (async () => {
    try {
      const config = await runtime();
      if (!isAuthorised(request, config)) return reply(response, 401, {});
      if (request.method === "GET" && request.url === "/v1/status") return reply(response, 200, { account, status });
      if (request.method === "GET" && request.url === "/v1/pairing") return reply(response, 200, { qrCode });
      let raw = "";
      for await (const chunk of request) {
        raw += chunk;
        if (raw.length > 64 * 1024) return reply(response, 413, {});
      }
      const body = JSON.parse(raw);
      if (request.method === "POST" && request.url === "/v1/messages" && status === "ready") { const sent = await socket.sendMessage(body.chatId, { text: body.text }); return reply(response, 201, { id: sent.key.id }); }
      if (request.method === "POST" && request.url === "/v1/messages/edit" && status === "ready") { await socket.sendMessage(body.chatId, { text: body.text }, { edit: { fromMe: true, id: body.messageId, remoteJid: body.chatId } }); return reply(response, 204, {}); }
      return reply(response, 409, {});
    } catch (error) {
      console.log(JSON.stringify({ event: "http.request_failed", path: request.url, error: String(error) }));
      if (!response.headersSent) return reply(response, 500, {});
      response.end();
    }
  })();
});
const shutdown = () => {
  connectionGeneration += 1;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = undefined;
  try { socket?.end(new Error("gateway shutting down")); } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
};
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
server.listen(3000, "0.0.0.0");
void connect().catch((error) => {
  console.log(JSON.stringify({ event: "session.connect_failed", error: String(error) }));
  status = "disconnected";
  scheduleReconnect(5000);
});
