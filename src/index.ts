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
  private conn: NatsConnection | null = null;
  private connecting: Promise<NatsConnection> | null = null;
  private ready!: Promise<NatsConnection>;
  private resolveReady!: (conn: NatsConnection) => void;

  private connectionOptions: Options = {} as Options
  private config: MessagingConfig = {} as MessagingConfig
  private defaultTimeout = 10000;

  private subscriptions: Map<string, Promise<Subscription>> = new Map();
  private handlers = new Map<string, MsgCallback<Msg>>()

  constructor() {
    this.reset()
  }

  private reset() {
    this.ready = new Promise(resolve => (this.resolveReady = resolve))
  }

  async connect(config: MessagingConfig, connectionOptions?: Options): Promise<NatsConnection | null> {
    console.log(new Date().toISOString(), '[NATS] connect', this.conn)
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

    console.log(new Date().toISOString(), '[NATS] connect options', options)

    this.connecting = wsconnect(options)
      .then(conn => {
        console.log(new Date().toISOString(), '[NATS] connected')
        this.conn = conn
        this.monitorStatus(this.connectionOptions)
        this.resolveReady(conn)
        return conn
      })
      .finally(() => {
        this.connecting = null
      })

    return this.connecting
  }

  async request<TResponse extends StandardResponse = StandardResponse>(
    channel: string,
    msg: Message,
    opt?: RequestOptions
  ): Promise<TResponse> {
    const conn = await this.ready
    const timeout = opt?.timeout ?? this.defaultTimeout;
    const res = await conn.request(channel, JSON.stringify(msg), { timeout });
    return res.json<TResponse>();
  }

  async publish(channel: string, msg: Message): Promise<void> {
    const conn = await this.ready
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
    console.log(new Date().toISOString(), '[NATS] subscribeInner', channel, existingSub)
    if (existingSub) return Promise.resolve(existingSub)

    this.handlers.set(channel, callback)
    const sub = this.ready.then(conn => conn.subscribe(channel, { callback }))
    console.log(new Date().toISOString(), '[NATS] subed', channel)
    this.subscriptions.set(channel, sub)

    sub.catch(() => {
      console.log(new Date().toISOString(), '[NATS] sub catch', channel)
      this.subscriptions.delete(channel)
      this.handlers.delete(channel)
    })
    return sub
  }

  async unsubscribe(channel: string) {
    const p = this.subscriptions.get(channel);
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

  async disconnect(): Promise<void> {
    if (this.conn && !(this.conn.isDraining() || this.conn.isClosed())) {
      await this.conn.drain();
      this.conn = null;
      this.reset()
      this.subscriptions.clear()
    }
  }

  async reconnect(): Promise<void> {
    if (this.conn && this.conn.isDraining()) return;
    const handlers = [...this.handlers]
    await this.disconnect()
    for (const [channel, cb] of handlers) this.subscribeInner(channel, cb);
    await this.connect(this.config, this.connectionOptions)
  }

  private async monitorStatus(eventHandlers: Options): Promise<void> {
    if (this.conn) {
      try {
        for await (const s of this.conn.status()) {
          switch (s.type) {
            case "close":
              eventHandlers.onClose?.(s)
              break
            case "disconnect":
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