"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.ts
var index_exports = {};
__export(index_exports, {
  connectMessagingService: () => connectMessagingService,
  messagingService: () => messagingService,
  publish: () => publish,
  request: () => request,
  subscribeWithMsgHandler: () => subscribeWithMsgHandler,
  unsubscribe: () => unsubscribe
});
module.exports = __toCommonJS(index_exports);
var import_nats_core = require("@nats-io/nats-core");
var MessagingService = class {
  conn = null;
  defaultTimeout = 1e4;
  subscriptions = {};
  async connect(config) {
    const {
      servers,
      user,
      pass,
      authenticator,
      timeoutMs = 1e4,
      maxReconnectAttempts = -1,
      reconnectTimeWaitMs = 2e3,
      pingIntervalMs = 2e3
    } = config;
    this.defaultTimeout = timeoutMs;
    const options = {
      servers,
      user,
      pass,
      authenticator,
      maxReconnectAttempts,
      reconnectTimeWait: reconnectTimeWaitMs,
      pingInterval: pingIntervalMs
    };
    this.conn = await (0, import_nats_core.wsconnect)(options);
    this.monitorStatus(this.conn);
    return this.conn;
  }
  async request(channel, msg, opt) {
    const conn = this.getConn();
    const timeout = opt?.timeout ?? this.defaultTimeout;
    const res = await conn.request(channel, JSON.stringify(msg), { timeout });
    const decoded = res.json();
    if (!decoded?.success) {
      throw decoded;
    }
    return decoded;
  }
  publish(channel, msg) {
    const conn = this.getConn();
    conn.publish(channel, JSON.stringify(msg));
  }
  subscribe(channel, msgHandler) {
    if (this.subscriptions[channel]) return this.subscriptions[channel];
    const conn = this.getConn();
    const sub = conn.subscribe(channel, {
      callback: (err, msg) => {
        if (err) {
          console.error(`[NATS] Subscription error on channel ${channel}:`, err);
          return;
        }
        try {
          const data = msg.json();
          msgHandler(channel, data);
        } catch (e) {
          console.error(`[NATS] Failed to decode/handle message on ${channel}:`, e);
        }
      }
    });
    this.subscriptions[channel] = sub;
    return sub;
  }
  unsubscribe(channel) {
    const sub = this.subscriptions[channel];
    if (sub) {
      sub.drain().then(() => {
        sub.unsubscribe();
        delete this.subscriptions[channel];
      });
    }
  }
  async disconnect() {
    if (this.conn) {
      await this.conn.drain();
      this.conn = null;
    }
  }
  getConn() {
    if (!this.conn) {
      throw new Error("NATS MessagingService is not connected. Call connect() first.");
    }
    return this.conn;
  }
  async monitorStatus(conn) {
    try {
      for await (const s of conn.status()) {
        if (s.type !== "ping" && s.type !== "update") {
          console.log(`[NATS] Event:`, s);
        }
      }
    } catch (err) {
      console.error("[NATS] Status monitor error:", err);
    }
  }
};
var messagingService = new MessagingService();
var connectMessagingService = (config) => {
  switch (config.authType) {
    case "userpass":
      return messagingService.connect({
        servers: config.servers,
        user: config.creds.username,
        pass: config.creds.password
      });
    case "jwt":
      return messagingService.connect({
        servers: config.servers,
        authenticator: (0, import_nats_core.jwtAuthenticator)(config.creds.jwt)
      });
    default: {
      throw new Error(`Auth type not supported: ${JSON.stringify(config)}`);
    }
  }
};
var request = (channel, msg, opt) => messagingService.request(channel, msg, opt);
var publish = (channel, msg) => messagingService.publish(channel, msg);
var subscribeWithMsgHandler = (channel, msgHandler) => messagingService.subscribe(channel, msgHandler);
var unsubscribe = (channel) => messagingService.unsubscribe(channel);
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  connectMessagingService,
  messagingService,
  publish,
  request,
  subscribeWithMsgHandler,
  unsubscribe
});
