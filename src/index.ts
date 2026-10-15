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
  onDisconnect: (e: DisconnectStatus) => void;
  onReconnect: (e: ReconnectStatus) => void;
  onReconnecting: (e: ReconnectingStatus) => void;
  onServerError: (e: ServerErrorStatus) => void;
  onPing: (e: ClientPingStatus) => void;
  onStaleConnection: (e: StaleConnectionStatus) => void;
  onSlowConsumer: (e: SlowConsumerStatus) => void;
  onClose: (e: CloseStatus) => void;
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

interface Task {
  channel: string;
  msgHandler?: (channel: string, data: Message) => void;
  callback?: MsgCallback<Msg>;
}

export type ActionHandler<T = unknown> = (channel: string, data: T) => void;

class MessagingService {
  private conn: NatsConnection | null = null;
  private connectionOptions: Options = {} as Options
  private config: MessagingConfig = {} as MessagingConfig
  private queue: Task[] = [];
  private connecting = false;
  private defaultTimeout = 10000;
  private subscriptions: Map<string, Subscription> = new Map();

  async connect(config: MessagingConfig, connectionOptions?: Options): Promise<NatsConnection | null> {
    if (this.conn) {
      return this.conn;
    }

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

    if (!this.conn && !this.connecting) {
      this.connecting = true
      this.conn = await wsconnect(options)
      this.monitorStatus(this.conn)
      this.connecting = false

      while (this.queue.length) {
        const task = this.queue.shift()
        if (task) {
          if (task.msgHandler) this.subscribe(task.channel, task.msgHandler)
          else if (task.callback) this.subscribeHandler(task.channel, task.callback)
        }
      }
      return this.conn;
    }

    return null;
  }

  async request<TResponse extends StandardResponse = StandardResponse>(
    channel: string,
    msg: Message,
    opt?: RequestOptions
  ): Promise<TResponse> {
    const conn = this.getConn();
    const timeout = opt?.timeout ?? this.defaultTimeout;
    const res = await conn.request(channel, JSON.stringify(msg), { timeout });
    return res.json<TResponse>();;
  }

  publish(channel: string, msg: Message): void {
    const conn = this.getConn();
    conn.publish(channel, JSON.stringify(msg));
  }

  subscribe(
    channel: string,
    msgHandler: (channel: string, data: Message) => void
  ): Subscription | undefined {
    if (!this.conn) {
      this.queue.push({ channel, msgHandler })
      try {
        this.connect(this.config as MessagingConfig, this.connectionOptions)
      } catch (e) {
        console.warn(`[NATS] failed to connect on subscribe [${channel}], config [${this.config}]`, e)
      }
      return
    } else if (this.subscriptions.has(channel)) {
      return this.subscriptions.get(channel)
    } else {
      const conn = this.getConn();
      const sub = conn.subscribe(channel, {
        callback: (err: Error | null, msg: Msg) => {
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
        },
      });
      this.subscriptions.set(channel, sub)
      return sub
    }
  }

  unsubscribe(channel: string) {
    this.queue = this.queue.filter(el => el.channel !== channel)
    const sub = this.subscriptions.get(channel)
    if (sub) {
      sub.drain()
        .then(() => {
          sub.unsubscribe()
          this.subscriptions.delete(channel)
        })
        .catch((err) => console.error(`[NATS] Error draining ${channel}:`, err));
    }
  }

  async disconnect(): Promise<void> {
    if (this.conn && !(this.conn.isDraining() || this.conn.isClosed())) {
      await this.conn.drain();
      this.conn = null;
      this.subscriptions.clear()
    }
  }

  async reconnect(): Promise<void> {
    if (this.conn && this.conn.isDraining()) return
    this.subscriptions.forEach(sub => {
      this.queue.push({
        channel: sub.getSubject(),
        callback: sub.callback,
      })
    })
    this.disconnect()
      .then(() => this.connect(this.config, this.connectionOptions))
  }

  private subscribeHandler(
    channel: string,
    callback: MsgCallback<Msg>
  ) {
    if (this.conn && !this.subscriptions.has(channel)) {
      const sub = this.conn.subscribe(channel, { callback })
      this.subscriptions.set(channel, sub)
    }
  }

  private getConn(): NatsConnection {
    if (!this.conn) {
      throw new Error('NATS MessagingService is not connected. Call connect() first.');
    }
    return this.conn;
  }

  private async monitorStatus(conn: NatsConnection): Promise<void> {
    try {
      for await (const s of conn.status()) {
        if (s.type !== 'ping' && s.type !== 'update') {
          console.log(`[NATS] Event:`, s);
        }
      }
    } catch (err) {
      console.error('[NATS] Status monitor error:', err);
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