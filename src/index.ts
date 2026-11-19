import {
  wsconnect,
  NatsConnection,
  ConnectionOptions,
  Subscription,
  Msg, Authenticator, jwtAuthenticator, DisconnectStatus, ReconnectStatus, ReconnectingStatus, ServerErrorStatus,
  ClientPingStatus, StaleConnectionStatus, SlowConsumerStatus, CloseStatus, MsgCallback
} from '@nats-io/nats-core';

export interface MessagingConfig {
  servers: string | string[];
  user?: string;
  pass?: string;
  authenticator?: Authenticator;
  timeoutMs?: number;
  maxReconnectAttempts?: number;
  reconnectTimeWaitMs?: number;
  pingIntervalMs?: number;
}

export interface BaseConnectionConfig {
  authType: string;
  servers: string[];
  credentials: unknown
}

export interface UserPassConnectionConfig extends BaseConnectionConfig {
  authType: 'userpass';
  credentials: { username: string; password: string }
}

export interface JwtConnectionConfig extends BaseConnectionConfig {
  authType: 'jwt';
  credentials: { jwt: string }
}

export type ConnectionConfig = UserPassConnectionConfig | JwtConnectionConfig;

export interface Options extends ConnectionOptions {
  onDisconnect?: (e: DisconnectStatus) => void;
  onReconnect?: (e: ReconnectStatus) => void;
  onReconnecting?: (e: ReconnectingStatus) => void;
  onServerError?: (e: ServerErrorStatus) => void;
  onPing?: (e: ClientPingStatus) => void;
  onStaleConnection?: (e: StaleConnectionStatus) => void;
  onSlowConsumer?: (e: SlowConsumerStatus) => void;
  onClose?: (e: CloseStatus) => void;
}

export interface RequestOptions {
  timeout?: number;
}

export interface StandardResponse<T = unknown> {
  success: boolean;
  data?: T;
  error?: string;
  [key: string]: unknown;
}

export interface Message<T = unknown> {
  action: string;
  data: T
}

export type ActionHandler<T = unknown> = (channel: string, data: T) => void;

class MessagingService {
  private connectId = 0
  private conn: NatsConnection | null = null;
  private connecting: Promise<NatsConnection | null> | null = null;
  private ready!: Promise<NatsConnection>;
  private resolveReady!: (conn: NatsConnection) => void;
  private readyResolved = false

  private connectionOptions: Options = {} as Options
  private config: MessagingConfig = {} as MessagingConfig
  private defaultTimeout = 10000;

  private subscriptions: Map<string, Promise<Subscription>> = new Map();
  private handlers = new Map<string, MsgCallback<Msg>>()

  private cancelled = new WeakSet<Promise<Subscription>>()

  constructor() {
    this.reset()
  }

  private reset() {
    const isPending = this.ready !== undefined && !this.readyResolved

    if (isPending) {
      // Nobody has received a connection from this promise yet,
      // and callers may be waiting on it. Replacing it would
      // strand them, so keep it.
      return
    }

    // Either this is the first call (from the constructor), or the old
    // promise already handed out a connection that's now gone.
    // Create a new pending promise.
    this.readyResolved = false
    this.ready = new Promise(resolve => {
      // Wrap `resolve` so we record that it happened.
      this.resolveReady = (conn) => {
        this.readyResolved = true
        resolve(conn)
      }
    })
  }

  async connect(config: MessagingConfig, connectionOptions?: Options): Promise<NatsConnection | null> {
    if (this.conn) return Promise.resolve(this.conn)
    if (this.connecting) return this.connecting

    const {
      servers,
      user,
      pass,
      authenticator,
      timeoutMs = this.defaultTimeout,
      maxReconnectAttempts = -1,
      reconnectTimeWaitMs = 2000,
      pingIntervalMs = 2000,
    } = config;

    this.config = config
    this.connectionOptions = connectionOptions ?? {} as Options
    this.defaultTimeout = timeoutMs;

    const options: ConnectionOptions = {
      servers,
      user,
      pass,
      authenticator,
      maxReconnectAttempts,
      reconnectTimeWait: reconnectTimeWaitMs,
      pingInterval: pingIntervalMs,
      ...connectionOptions
    };

    const id = ++this.connectId
    console.log(new Date().toISOString(), '[NATS] connect options', options)

    this.connecting = wsconnect(options)
      .then(conn => {
        // disconnect() was called while connecting
        if (id !== this.connectId) {
          conn.close()
          return null
        }
        this.conn = conn
        this.monitorStatus(conn, this.connectionOptions)
        conn.closed().then(() => this.onClosed(conn))
        this.resolveReady(conn)
        return conn
      })
      .finally(() => { if (id === this.connectId) this.connecting = null })

    return this.connecting
  }

  private async getConn(timeout: number): Promise<NatsConnection> {
    let t
    const timer = new Promise<never>((_, reject) => {
      t = setTimeout(() => reject(new Error('[NATS] not connected')), timeout)
    })
    try { return await Promise.race([this.ready, timer]) }
    finally { clearTimeout(t) }
  }

  async request<TResponse extends StandardResponse = StandardResponse>(
    channel: string,
    msg: Message,
    opt?: RequestOptions
  ): Promise<TResponse> {
    const timeout = opt?.timeout ?? this.defaultTimeout;
    const conn = await this.getConn(timeout)
    const res = await conn.request(channel, JSON.stringify(msg), { timeout });
    return res.json<TResponse>();
  }

  async publish(channel: string, msg: Message): Promise<void> {
    const conn = await this.getConn(this.defaultTimeout)
    conn.publish(channel, JSON.stringify(msg));
  }

  subscribe(
    channel: string,
    msgHandler: (channel: string, data: Message) => void
  ): Promise<Subscription> {
    return this.subscribeInner(channel, (err: Error | null, msg: Msg) => {
      if (err) {
        console.error(`[NATS] Subscription error on channel ${channel}:`, err);
        return;
      }
      try {
        const data = msg.json<Message>();
        msgHandler(channel, data);
      } catch (e) {
        console.error(`[NATS] Failed to decode/handle message on ${channel}, msg: ${msg}`, e);
      }
    })
  }

  private subscribeInner(channel: string, callback: MsgCallback<Msg>): Promise<Subscription> {
    const existingSub = this.subscriptions.get(channel)
    if (existingSub) return Promise.resolve(existingSub)

    this.handlers.set(channel, callback)
    const subPromise: Promise<Subscription> = this.ready.then(conn => {
      const current = this.subscriptions.get(channel)
      if (current !== subPromise) {
        // replaced by a newer subscription to this channel: the channel
        // is still subscribed, so resolve with that one
        const wasCancelled = this.cancelled.delete(subPromise)
        if (current && !wasCancelled) return current
        // removed by disconnect()/unsubscribe(): really not subscribed
        throw new Error(`[NATS] subscription to ${channel} was cancelled`)
      }
      return conn.subscribe(channel, { callback })
    })

    this.subscriptions.set(channel, subPromise)

    subPromise.catch(() => {
      if (this.subscriptions.get(channel) === subPromise) {
        this.subscriptions.delete(channel)
        this.handlers.delete(channel)
      }
    })

    return subPromise
  }

  async unsubscribe(channel: string) {
    const p = this.subscriptions.get(channel)
    if (p) this.cancelled.add(p)
    this.subscriptions.delete(channel)
    this.handlers.delete(channel)

    if (p) {
      try {
        const sub = await p
        await sub.drain()
      } catch (err) {
        console.error(`[NATS] Error draining ${channel}:`, err);
      }
    }
  }

  private onClosed(conn: NatsConnection) {
    if (this.conn !== conn) return // intentional disconnect() or stale connection
    this.conn = null
    this.reset()
    // subscriptions died with the connection; re-register them so they
    // come back automatically on the next successful connect()
    this.subscriptions.clear()
    for (const [channel, cb] of this.handlers) this.subscribeInner(channel, cb)
  }

  async disconnect(): Promise<void> {
    this.connectId++
    this.connecting = null
    const conn = this.conn
    this.conn = null

    this.reset()
    this.subscriptions.clear()
    this.handlers.clear()

    if (conn && !(conn.isDraining() || conn.isClosed())) {
      // case of disconnect while NATS trying to reconnect
      try {
        await Promise.race([
          conn.drain(),
          new Promise(resolve => setTimeout(resolve, 3000)),
        ])
      } catch (e) {
        console.error('[NATS] drain error', e)
      }
      if (!conn.isClosed()) await conn.close().catch(() => {})
    }
  }

  async reconnect(): Promise<void> {
    if (this.conn && this.conn.isDraining()) return;
    const handlers = [...this.handlers]
    await this.disconnect()
    for (const [channel, cb] of handlers) this.subscribeInner(channel, cb);
    await this.connect(this.config, this.connectionOptions)
  }

  async reconnectIfNeeded(): Promise<void> {
    console.log('reconnectIfNeeded', this.conn || this.connecting || !this.config.servers)
    if (this.conn || this.connecting || !this.config.servers) return;
    await this.connect(this.config, this.connectionOptions)
  }

  private async monitorStatus(conn: NatsConnection, eventHandlers: Options): Promise<void> {
    if (conn) {
      try {
        for await (const s of conn.status()) {
          if (s.type !== 'close' && conn !== this.conn) continue;
          switch (s.type) {
            case "close":
              eventHandlers.onClose?.(s)
              break
            case "disconnect":
              this.reset()
              eventHandlers.onDisconnect?.(s)
              break
            case "slowConsumer":
              eventHandlers.onSlowConsumer?.(s)
              break
            case "error":
              eventHandlers.onServerError?.(s)
              break
            case "staleConnection":
              eventHandlers.onStaleConnection?.(s)
              break
            case "reconnect":
              this.resolveReady(conn)
              eventHandlers.onReconnect?.(s)
              break
            case "reconnecting":
              eventHandlers.onReconnecting?.(s)
              break
            case "ping":
              eventHandlers.onPing?.(s)
              break
          }
          if (s.type !== 'ping' && s.type !== 'update') {
             console.log(new Date().toISOString(), `[NATS] Event:`, s);
          }
        }
      } catch (err) {
        console.error('[NATS] Status monitor error:', err);
      }
    }
  }
}

export const messagingService = new MessagingService();

// Standalone function exports
export const connectMessagingService = (config: ConnectionConfig, options?: Options) => {
  switch (config.authType) {
    case 'userpass':
      return messagingService.connect({
        servers: config.servers,
        user: config.credentials.username,
        pass: config.credentials.password,
      }, options);

    case 'jwt':
      return messagingService.connect({
        servers: config.servers,
        authenticator: jwtAuthenticator(config.credentials.jwt),
      }, options);

    default: {
      throw new Error(`Auth type not supported: ${JSON.stringify(config)}`);
    }
  }
};

export const request = <R extends StandardResponse = StandardResponse>(
  channel: string,
  msg: Message,
  opt?: RequestOptions
) => messagingService.request<R>(channel, msg, opt);

export const publish = (channel: string, msg: Message) =>
  messagingService.publish(channel, msg);

export const subscribeWithMsgHandler = (
  channel: string,
  msgHandler: (channel: string, data: Message) => void
) => messagingService.subscribe(channel, msgHandler);

export const unsubscribe = (
  channel: string,
) => messagingService.unsubscribe(channel);