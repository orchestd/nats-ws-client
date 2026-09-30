import {
  connect,
  DebugEvents,
  Events,
  JSONCodec,
  NatsConnection,
  ConnectionOptions,
  Subscription,
  Msg, Authenticator, jwtAuthenticator,
} from 'nats.ws';

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
  creds: unknown
}

export interface UserPassConnectionConfig extends BaseConnectionConfig {
  authType: 'userpass';
  creds: { username: string; password: string }
}

export interface JwtConnectionConfig extends BaseConnectionConfig {
  authType: 'jwt';
  creds: { jwt: string }
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

export type ActionHandler<T = unknown> = (channel: string, data: T) => void;

class MessagingService {
  private conn: NatsConnection | null = null;
  private readonly codec = JSONCodec();
  private defaultTimeout = 10000;
  private subscriptions: Record<string, Subscription> = {}

  async connect(config: MessagingConfig): Promise<NatsConnection> {
    const {
      servers,
      user,
      pass,
      authenticator,
      timeoutMs = 10000,
      maxReconnectAttempts = -1,
      reconnectTimeWaitMs = 2000,
      pingIntervalMs = 2000,
    } = config;

    this.defaultTimeout = timeoutMs;

    const options: ConnectionOptions = {
      servers,
      user,
      pass,
      authenticator,
      maxReconnectAttempts,
      reconnectTimeWait: reconnectTimeWaitMs,
      pingInterval: pingIntervalMs,
    };

    this.conn = await connect(options);
    this.monitorStatus(this.conn);
    return this.conn;
  }

  async request<TResponse extends StandardResponse = StandardResponse>(
    channel: string,
    msg: Message,
    opt?: RequestOptions
  ): Promise<TResponse> {
    const conn = this.getConn();
    const timeout = opt?.timeout ?? this.defaultTimeout;

    const res = await conn.request(channel, this.codec.encode(msg), { timeout });
    const decoded = this.codec.decode(res.data) as TResponse;

    if (!decoded?.success) {
      throw decoded;
    }

    return decoded;
  }

  publish(channel: string, msg: Message): void {
    const conn = this.getConn();
    conn.publish(channel, this.codec.encode(msg));
  }

  subscribe(
    channel: string,
    msgHandler: (channel: string, data: Message) => void
  ): Subscription {
    if (this.subscriptions[channel]) return this.subscriptions[channel]
    const conn = this.getConn();
    const sub = conn.subscribe(channel, {
      callback: (err: Error | null, msg: Msg) => {
        if (err) {
          console.error(`[NATS] Subscription error on channel ${channel}:`, err);
          return;
        }
        try {
          const data = this.codec.decode(msg.data) as Message;
          msgHandler(channel, data);
        } catch (e) {
          console.error(`[NATS] Failed to decode/handle message on ${channel}:`, e);
        }
      },
    });
    this.subscriptions[channel] = sub
    return sub
  }

  unsubscribe(channel: string) {
    const sub = this.subscriptions[channel]
    if (sub) {
      sub.drain()
        .then(() => {
          sub.unsubscribe()
          delete this.subscriptions[channel]
        })
    }
  }

  async disconnect(): Promise<void> {
    if (this.conn) {
      await this.conn.drain();
      this.conn = null;
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
        switch (s.type) {
          case Events.Disconnect:
            console.log(`[NATS] Disconnected: ${s.data}`);
            break;
          case Events.Reconnect:
            console.log(`[NATS] Reconnected: ${s.data}`);
            break;
          case Events.LDM:
            console.log('[NATS] Requested to reconnect (LDM)');
            break;
          case Events.Update:
            console.log(`[NATS] Cluster update received: ${s.data}`);
            break;
          case DebugEvents.Reconnecting:
            console.log('[NATS] Attempting reconnect...');
            break;
          case DebugEvents.StaleConnection:
            console.log('[NATS] Connection is stale');
            break;
        }
      }
    } catch (err) {
      console.error('[NATS] Status monitor error:', err);
    }
  }
}

export const messagingService = new MessagingService();

// Standalone function exports
export const connectMessagingService = (config: ConnectionConfig) => {
  switch (config.authType) {
    case 'userpass':
      return messagingService.connect({
        servers: config.servers,
        user: config.creds.username,
        pass: config.creds.password,
      });

    case 'jwt':
      return messagingService.connect({
        servers: config.servers,
        authenticator: jwtAuthenticator(config.creds.jwt),
      });

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