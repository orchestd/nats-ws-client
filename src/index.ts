import {
  wsconnect,
  NatsConnection,
  ConnectionOptions,
  Subscription,
  Msg, Authenticator, jwtAuthenticator
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
  msgHandler: (channel: string, data: Message) => void;
}

export type ActionHandler<T = unknown> = (channel: string, data: T) => void;

class MessagingService {
  private conn: NatsConnection | null = null;
  private optionalOptions: ConnectionOptions = {} as ConnectionOptions
  private config: MessagingConfig = {} as MessagingConfig
  private queue: Task[] = [];
  private connecting = false;
  private defaultTimeout = 10000;
  private subscriptions: Map<string, Subscription> = new Map();

  async connect(config: MessagingConfig, optionalOptions?: ConnectionOptions): Promise<NatsConnection> {
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
    this.optionalOptions = optionalOptions ?? {}
    this.defaultTimeout = timeoutMs;

    const options: ConnectionOptions = {
      servers,
      user,
      pass,
      authenticator,
      maxReconnectAttempts,
      reconnectTimeWait: reconnectTimeWaitMs,
      pingInterval: pingIntervalMs,
      ...optionalOptions
    };

    if (!this.conn && !this.connecting) {
      this.connecting = true
      this.conn = await wsconnect(options)
      this.monitorStatus(this.conn)
      this.connecting = false

      while (this.queue.length) {
        const task = this.queue.shift()
        if (task) {
          this.subscribe(task?.channel, task.msgHandler)
        }
      }
      return this.conn;
    }

    return this.conn as NatsConnection;
  }

  async request<TResponse extends StandardResponse = StandardResponse>(
    channel: string,
    msg: Message,
    opt?: RequestOptions
  ): Promise<TResponse> {
    const conn = this.getConn();
    const timeout = opt?.timeout ?? this.defaultTimeout;

    const res = await conn.request(channel, JSON.stringify(msg), { timeout });
    const decoded = res.json<TResponse>();

    if (!decoded?.success) {
      throw decoded;
    }

    return decoded;
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
      console.log('test')
      this.queue.push({ channel, msgHandler })
      this.connect(this.config as MessagingConfig, this.optionalOptions)
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
    const index = this.queue.findIndex(el => el.channel === channel)
    if (index !== -1) {
      this.queue.splice(index, 1)
    }

    const sub = this.subscriptions.get(channel)
    if (sub) {
      sub.drain()
        .then(() => {
          sub.unsubscribe()
          this.subscriptions.delete(channel)
        })
    }
  }

  async disconnect(): Promise<void> {
    if (this.conn && !(this.conn.isDraining() || this.conn.isClosed())) {
      await this.conn.drain();
      this.conn = null;
      this.subscriptions.clear()
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
export const connectMessagingService = (config: ConnectionConfig, options?: ConnectionOptions) => {
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