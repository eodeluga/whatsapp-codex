import { createHash, randomUUID } from "node:crypto";
import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

const DELIVERY_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
const ACKNOWLEDGED_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const MAX_UNACKNOWLEDGED_DELIVERIES = 10_000;

export class DeliveryRequestError extends Error {
  constructor(statusCode, message) {
    super(message);
    this.name = "DeliveryRequestError";
    this.statusCode = statusCode;
  }
}

export class DeliveryStore {
  constructor(path) {
    this.path = path;
    this.document = { operations: {} };
    this.mutex = Promise.resolve();
  }

  async load() {
    let document;
    try {
      document = JSON.parse(await readFile(this.path, "utf8"));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      document = { operations: {} };
    }
    if (!document || typeof document.operations !== "object" || Array.isArray(document.operations)) {
      throw new Error("delivery idempotency store has an invalid format");
    }
    this.document = { operations: document.operations };
    await this.withLock(() => this.compactUnlocked(Date.now()));
  }

  async withLock(operation) {
    const run = this.mutex.then(operation, operation);
    this.mutex = run.catch(() => {});
    return run;
  }

  get(deliveryId) {
    const record = this.document.operations[deliveryId];
    return record ? { ...record } : undefined;
  }

  async prepare(deliveryId, chatIdHash, textHash, providerMessageId) {
    return this.withLock(() => this.prepareUnlocked(deliveryId, chatIdHash, textHash, providerMessageId));
  }

  async markSent(deliveryId) {
    return this.withLock(() => this.markSentUnlocked(deliveryId));
  }

  async acknowledge(deliveryId, providerMessageId) {
    return this.withLock(() => this.acknowledgeUnlocked(deliveryId, providerMessageId));
  }

  async compact(now = Date.now()) {
    return this.withLock(() => this.compactUnlocked(now));
  }

  counts() {
    const counts = { prepared: 0, sent: 0, acknowledged: 0 };
    for (const record of Object.values(this.document.operations)) {
      if (record.state in counts) counts[record.state] += 1;
    }
    return counts;
  }

  diagnostics() {
    const counts = this.counts();
    return {
      ...counts,
      degraded: counts.prepared + counts.sent > MAX_UNACKNOWLEDGED_DELIVERIES,
    };
  }

  async prepareUnlocked(deliveryId, chatIdHash, textHash, providerMessageId) {
    const existing = this.document.operations[deliveryId];
    if (existing) {
      if (existing.chatIdHash !== chatIdHash || existing.textHash !== textHash) {
        throw new DeliveryRequestError(409, "delivery id conflicts with existing content");
      }
      return { ...existing };
    }
    const now = Date.now();
    const record = {
      deliveryId,
      chatIdHash,
      textHash,
      providerMessageId,
      state: "prepared",
      createdAt: now,
      updatedAt: now,
    };
    this.document.operations[deliveryId] = record;
    await this.saveUnlocked();
    return { ...record };
  }

  async markSentUnlocked(deliveryId) {
    const record = this.requireRecord(deliveryId);
    if (record.state === "acknowledged") return { ...record };
    record.state = "sent";
    record.updatedAt = Date.now();
    await this.saveUnlocked();
    return { ...record };
  }

  async acknowledgeUnlocked(deliveryId, providerMessageId) {
    const record = this.requireRecord(deliveryId);
    if (record.providerMessageId !== providerMessageId) {
      throw new DeliveryRequestError(409, "delivery acknowledgement does not match the provider message");
    }
    if (record.state !== "acknowledged") {
      record.state = "acknowledged";
      record.updatedAt = Date.now();
      await this.saveUnlocked();
    }
    await this.compactUnlocked(Date.now());
    return { ...record };
  }

  async compactUnlocked(now) {
    let changed = false;
    for (const [deliveryId, record] of Object.entries(this.document.operations)) {
      if (record.state === "acknowledged" && now - record.updatedAt >= ACKNOWLEDGED_RETENTION_MS) {
        delete this.document.operations[deliveryId];
        changed = true;
      }
    }
    if (changed) await this.saveUnlocked();
  }

  requireRecord(deliveryId) {
    const record = this.document.operations[deliveryId];
    if (!record) throw new DeliveryRequestError(409, "delivery id is not known");
    return record;
  }

  async saveUnlocked() {
    const parent = dirname(this.path);
    await mkdir(parent, { recursive: true, mode: 0o700 });
    const temporary = join(parent, `.${basename(this.path)}.${process.pid}.${randomUUID()}.tmp`);
    let file;
    try {
      file = await open(temporary, "wx", 0o600);
      await file.writeFile(`${JSON.stringify(this.document)}\n`, "utf8");
      await file.sync();
      await file.close();
      file = undefined;
      await rename(temporary, this.path);
      const directory = await open(parent, "r");
      try {
        await directory.sync();
      } finally {
        await directory.close();
      }
    } finally {
      if (file) await file.close().catch(() => {});
      await unlink(temporary).catch(() => {});
    }
  }
}

export const hashDeliveryValue = (value) => createHash("sha256").update(value).digest("hex");

const requireDeliveryId = (body) => {
  if (typeof body?.deliveryId !== "string" || !DELIVERY_ID_PATTERN.test(body.deliveryId)) {
    throw new DeliveryRequestError(400, "deliveryId must be a UUID");
  }
  return body.deliveryId;
};

const requireText = (body) => {
  if (typeof body?.text !== "string" || body.text.length === 0) {
    throw new DeliveryRequestError(400, "text must be a non-empty string");
  }
  return body.text;
};

const requireChatId = (body) => {
  if (typeof body?.chatId !== "string" || body.chatId.length === 0) {
    throw new DeliveryRequestError(400, "chatId must be a non-empty string");
  }
  return body.chatId;
};

export const createDeliveryHandler = ({ store, sendMessage, generateMessageId, rememberOutboundMessage, forgetOutboundMessage }) => ({
  send: (body) => store.withLock(async () => {
    const deliveryId = requireDeliveryId(body);
    const chatId = requireChatId(body);
    const text = requireText(body);
    const chatIdHash = hashDeliveryValue(chatId);
    const textHash = hashDeliveryValue(text);
    const existing = store.get(deliveryId);
    if (existing && (existing.chatIdHash !== chatIdHash || existing.textHash !== textHash)) {
      throw new DeliveryRequestError(409, "delivery id conflicts with existing content");
    }
    if (existing?.state === "sent" || existing?.state === "acknowledged") {
      rememberOutboundMessage(existing.providerMessageId);
      return { statusCode: 200, body: { id: existing.providerMessageId, replayed: true } };
    }

    const prepared = existing ?? await store.prepareUnlocked(deliveryId, chatIdHash, textHash, generateMessageId());
    rememberOutboundMessage(prepared.providerMessageId);
    try {
      await sendMessage({ chatId, messageId: prepared.providerMessageId, text });
      await store.markSentUnlocked(deliveryId);
      return { statusCode: 201, body: { id: prepared.providerMessageId } };
    } catch (error) {
      forgetOutboundMessage(prepared.providerMessageId);
      throw error;
    }
  }),

  acknowledge: (body) => store.withLock(async () => {
    const deliveryId = requireDeliveryId(body);
    if (typeof body?.messageId !== "string" || body.messageId.length === 0) {
      throw new DeliveryRequestError(400, "messageId must be a non-empty string");
    }
    await store.acknowledgeUnlocked(deliveryId, body.messageId);
    return { statusCode: 204 };
  }),
});
