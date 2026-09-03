import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { DeliveryStore, createDeliveryHandler } from "./delivery-store.js";

const deliveryId = "11111111-1111-4111-8111-111111111111";
const otherDeliveryId = "22222222-2222-4222-8222-222222222222";

const createHarness = async ({ sendMessage, generatedMessageId = "baileys-message-1" } = {}) => {
  const directory = await mkdtemp(join(tmpdir(), "codex-delivery-store-"));
  const store = new DeliveryStore(join(directory, "idempotency.json"));
  await store.load();
  const sent = [];
  const handler = createDeliveryHandler({
    forgetOutboundMessage: () => {},
    generateMessageId: () => generatedMessageId,
    rememberOutboundMessage: () => {},
    sendMessage: sendMessage ?? (async (message) => { sent.push(message); }),
    store,
  });
  return { directory, handler, sent, store };
};

const cleanup = async ({ directory }) => rm(directory, { recursive: true, force: true });

test("prepares before sending, persists sent, and replays after restart", async () => {
  const harness = await createHarness();
  try {
    let preparedAtSend;
    harness.handler = createDeliveryHandler({
      forgetOutboundMessage: () => {},
      generateMessageId: () => "baileys-message-1",
      rememberOutboundMessage: () => {},
      sendMessage: async (message) => {
        preparedAtSend = harness.store.get(deliveryId);
        harness.sent.push(message);
      },
      store: harness.store,
    });

    const request = { chatId: "self@c.us", deliveryId, text: "hello" };
    assert.deepEqual(await harness.handler.send(request), {
      body: { id: "baileys-message-1" },
      statusCode: 201,
    });
    assert.equal(preparedAtSend.state, "prepared");
    assert.equal(harness.store.get(deliveryId).state, "sent");

    const restartedStore = new DeliveryStore(join(harness.directory, "idempotency.json"));
    await restartedStore.load();
    let replaySendCount = 0;
    const restartedHandler = createDeliveryHandler({
      forgetOutboundMessage: () => {},
      generateMessageId: () => "should-not-be-used",
      rememberOutboundMessage: () => {},
      sendMessage: async () => { replaySendCount += 1; },
      store: restartedStore,
    });
    assert.deepEqual(await restartedHandler.send(request), {
      body: { id: "baileys-message-1", replayed: true },
      statusCode: 200,
    });
    assert.equal(replaySendCount, 0);
  } finally {
    await cleanup(harness);
  }
});

test("serializes concurrent first requests and rejects content conflicts", async () => {
  const harness = await createHarness();
  try {
    const request = { chatId: "self@c.us", deliveryId, text: "hello" };
    const results = await Promise.all([
      harness.handler.send(request),
      harness.handler.send(request),
    ]);
    assert.equal(results.filter((result) => result.statusCode === 201).length, 1);
    assert.equal(results.filter((result) => result.statusCode === 200).length, 1);
    assert.equal(harness.sent.length, 1);

    await assert.rejects(
      harness.handler.send({ ...request, text: "different" }),
      (error) => error.statusCode === 409,
    );
    await assert.rejects(
      harness.handler.send({ ...request, chatId: "another@c.us" }),
      (error) => error.statusCode === 409,
    );
  } finally {
    await cleanup(harness);
  }
});

test("retries a prepared operation with the same provider message id", async () => {
  const harness = await createHarness();
  try {
    let attempts = 0;
    const messages = [];
    harness.handler = createDeliveryHandler({
      forgetOutboundMessage: () => {},
      generateMessageId: () => "baileys-message-2",
      rememberOutboundMessage: () => {},
      sendMessage: async (message) => {
        messages.push(message);
        attempts += 1;
        if (attempts === 1) throw new Error("simulated provider failure");
      },
      store: harness.store,
    });
    const request = { chatId: "self@c.us", deliveryId, text: "hello" };

    await assert.rejects(harness.handler.send(request));
    assert.equal(harness.store.get(deliveryId).state, "prepared");
    const restartedStore = new DeliveryStore(join(harness.directory, "idempotency.json"));
    await restartedStore.load();
    const restartedHandler = createDeliveryHandler({
      forgetOutboundMessage: () => {},
      generateMessageId: () => "should-not-be-used",
      rememberOutboundMessage: () => {},
      sendMessage: async (message) => { messages.push(message); },
      store: restartedStore,
    });
    assert.deepEqual(await restartedHandler.send(request), {
      body: { id: "baileys-message-2" },
      statusCode: 201,
    });
    assert.deepEqual(messages.map((message) => message.messageId), [
      "baileys-message-2",
      "baileys-message-2",
    ]);
  } finally {
    await cleanup(harness);
  }
});

test("keeps prepared state if the provider accepted before sent persistence", async () => {
  const harness = await createHarness();
  try {
    const accepted = [];
    let failPersistence = true;
    const originalMarkSent = harness.store.markSentUnlocked.bind(harness.store);
    harness.store.markSentUnlocked = async (id) => {
      if (failPersistence) {
        failPersistence = false;
        throw new Error("simulated persistence failure");
      }
      return originalMarkSent(id);
    };
    harness.handler = createDeliveryHandler({
      forgetOutboundMessage: () => {},
      generateMessageId: () => "baileys-message-3",
      rememberOutboundMessage: () => {},
      sendMessage: async (message) => { accepted.push(message); },
      store: harness.store,
    });
    const request = { chatId: "self@c.us", deliveryId, text: "hello" };

    await assert.rejects(harness.handler.send(request));
    assert.equal(harness.store.get(deliveryId).state, "prepared");
    await harness.handler.send(request);
    assert.equal(harness.store.get(deliveryId).state, "sent");
    assert.deepEqual(accepted.map((message) => message.messageId), [
      "baileys-message-3",
      "baileys-message-3",
    ]);
  } finally {
    await cleanup(harness);
  }
});

test("acknowledges durably and retains only recent acknowledged records", async () => {
  const harness = await createHarness({ generatedMessageId: "baileys-message-4" });
  try {
    const request = { chatId: "self@c.us", deliveryId, text: "hello" };
    await harness.handler.send(request);
    assert.deepEqual(await harness.handler.acknowledge({ deliveryId, messageId: "baileys-message-4" }), {
      statusCode: 204,
    });
    assert.equal(harness.store.get(deliveryId).state, "acknowledged");
    await harness.store.prepare(otherDeliveryId, "chat-hash", "text-hash", "other-message");
    await harness.store.prepare("33333333-3333-4333-8333-333333333333", "chat-hash", "text-hash", "sent-message");
    await harness.store.markSent("33333333-3333-4333-8333-333333333333");
    await harness.store.compact(Date.now() + 8 * 24 * 60 * 60 * 1000);
    assert.equal(harness.store.get(deliveryId), undefined);
    assert.equal(harness.store.get(otherDeliveryId).state, "prepared");
    assert.equal(harness.store.get("33333333-3333-4333-8333-333333333333").state, "sent");

    await assert.rejects(
      harness.handler.acknowledge({ deliveryId: otherDeliveryId, messageId: "missing" }),
      (error) => error.statusCode === 409,
    );
  } finally {
    await cleanup(harness);
  }
});

test("requires UUID delivery ids", async () => {
  const harness = await createHarness();
  try {
    await assert.rejects(
      harness.handler.send({ chatId: "self@c.us", deliveryId: "not-a-uuid", text: "hello" }),
      (error) => error.statusCode === 400,
    );
  } finally {
    await cleanup(harness);
  }
});
