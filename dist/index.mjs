// src/index.ts
import {
  wsconnect,
  jwtAuthenticator
} from "@nats-io/nats-core";
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
    this.conn = await wsconnect(options);
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
        authenticator: jwtAuthenticator(config.creds.jwt)
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
export {
  connectMessagingService,
  messagingService,
  publish,
  request,
  subscribeWithMsgHandler,
  unsubscribe
};
