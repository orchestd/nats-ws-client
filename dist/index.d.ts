import { Authenticator, NatsConnection, Subscription } from '@nats-io/nats-core';

interface MessagingConfig {
    servers: string | string[];
    user?: string;
    pass?: string;
    authenticator?: Authenticator;
    timeoutMs?: number;
    maxReconnectAttempts?: number;
    reconnectTimeWaitMs?: number;
    pingIntervalMs?: number;
}
interface BaseConnectionConfig {
    authType: string;
    servers: string[];
    creds: unknown;
}
interface UserPassConnectionConfig extends BaseConnectionConfig {
    authType: 'userpass';
    creds: {
        username: string;
        password: string;
    };
}
interface JwtConnectionConfig extends BaseConnectionConfig {
    authType: 'jwt';
    creds: {
        jwt: string;
    };
}
type ConnectionConfig = UserPassConnectionConfig | JwtConnectionConfig;
interface RequestOptions {
    timeout?: number;
}
interface StandardResponse<T = unknown> {
    success: boolean;
    data?: T;
    error?: string;
    [key: string]: unknown;
}
interface Message<T = unknown> {
    action: string;
    data: T;
}
type ActionHandler<T = unknown> = (channel: string, data: T) => void;
declare class MessagingService {
    private conn;
    private defaultTimeout;
    private subscriptions;
    connect(config: MessagingConfig): Promise<NatsConnection>;
    request<TResponse extends StandardResponse = StandardResponse>(channel: string, msg: Message, opt?: RequestOptions): Promise<TResponse>;
    publish(channel: string, msg: Message): void;
    subscribe(channel: string, msgHandler: (channel: string, data: Message) => void): Subscription;
    unsubscribe(channel: string): void;
    disconnect(): Promise<void>;
    private getConn;
    private monitorStatus;
}
declare const messagingService: MessagingService;
declare const connectMessagingService: (config: ConnectionConfig) => Promise<NatsConnection>;
declare const request: <R extends StandardResponse = StandardResponse>(channel: string, msg: Message, opt?: RequestOptions) => Promise<R>;
declare const publish: (channel: string, msg: Message) => void;
declare const subscribeWithMsgHandler: (channel: string, msgHandler: (channel: string, data: Message) => void) => Subscription;
declare const unsubscribe: (channel: string) => void;

export { type ActionHandler, type BaseConnectionConfig, type ConnectionConfig, type JwtConnectionConfig, type Message, type MessagingConfig, type RequestOptions, type StandardResponse, type UserPassConnectionConfig, connectMessagingService, messagingService, publish, request, subscribeWithMsgHandler, unsubscribe };
