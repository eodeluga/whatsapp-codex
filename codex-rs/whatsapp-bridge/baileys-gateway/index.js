import makeWASocket, { Browsers, DisconnectReason, makeCacheableSignalKeyStore, useMultiFileAuthState } from "@whiskeysockets/baileys";
import { createHmac, timingSafeEqual } from "node:crypto";
import { readFile } from "node:fs/promises";
import http from "node:http";
import P from "pino";
import QRCode from "qrcode";

let account;
let qrCode;
let socket;
let status = "starting";
const runtime = async () => JSON.parse(await readFile("/codex-home/whatsapp/runtime.json", "utf8"));
const reply = (response, statusCode, body) => { response.writeHead(statusCode, { "content-type": "application/json" }); response.end(JSON.stringify(body)); };
const isAuthorised = (request, config) => {
  const token = request.headers.authorization?.replace(/^Bearer /u, "");
  if (!token) return false;
  const actual = Buffer.from(token);
  const expected = Buffer.from(config.transportApiToken);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
};
const deliver = async (message) => {
  const config = await runtime();
  const payload = JSON.stringify({ event: "message", idempotencyKey: message.key.id, data: { body: message.message?.conversation ?? message.message?.extendedTextMessage?.text ?? "", chatId: message.key.remoteJid, fromMe: message.key.fromMe === true, id: message.key.id, isGroup: message.key.remoteJid?.endsWith("@g.us") === true } });
  const signature = createHmac("sha256", config.webhookSigningSecret).update(payload).digest("hex");
  await fetch(config.webhookUrl, { body: payload, headers: { "content-type": "application/json", "x-codex-transport-signature": `sha256=${signature}` }, method: "POST" });
};
const connect = async () => {
  const { saveCreds, state } = await useMultiFileAuthState("/data/auth");
  socket = makeWASocket({ auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, P({ level: "silent" })) }, browser: Browsers.ubuntu("WhatsApp Codex"), logger: P({ level: "silent" }), markOnlineOnConnect: false, printQRInTerminal: false, syncFullHistory: false });
  socket.ev.on("creds.update", saveCreds);
  socket.ev.on("connection.update", async (update) => {
    if (update.qr) { qrCode = await QRCode.toDataURL(update.qr); status = "pairing"; }
    if (update.connection === "open") { account = socket.user?.id?.split("@")[0]?.split(":")[0]; qrCode = undefined; status = "ready"; }
    if (update.connection === "close") { const code = update.lastDisconnect?.error?.output?.statusCode; status = code === DisconnectReason.loggedOut ? "logged_out" : "disconnected"; if (code !== DisconnectReason.loggedOut) setTimeout(() => void connect(), 2000); }
  });
  socket.ev.on("messages.upsert", async ({ messages, type }) => { if (type === "notify") for (const message of messages) if (message.key.id && message.key.remoteJid && message.message) await deliver(message); });
};
http.createServer(async (request, response) => {
  const config = await runtime();
  if (!isAuthorised(request, config)) return reply(response, 401, {});
  if (request.method === "GET" && request.url === "/v1/status") return reply(response, 200, { account, status });
  if (request.method === "GET" && request.url === "/v1/pairing") return reply(response, 200, { qrCode });
  let raw = ""; for await (const chunk of request) raw += chunk; const body = JSON.parse(raw);
  if (request.method === "POST" && request.url === "/v1/messages" && status === "ready") { const sent = await socket.sendMessage(body.chatId, { text: body.text }); return reply(response, 201, { id: sent.key.id }); }
  if (request.method === "POST" && request.url === "/v1/messages/edit" && status === "ready") { await socket.sendMessage(body.chatId, { edit: { fromMe: true, id: body.messageId, remoteJid: body.chatId }, text: body.text }); return reply(response, 204, {}); }
  return reply(response, 409, {});
}).listen(3000, "0.0.0.0");
void connect();
