import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// ---------------------------------------------------------------------------
// Fake NATS connection
// Simulates just enough of NatsConnection for the service: status events,
// closed(), drain/close, flush (ping), subscribe/request/publish.
// ---------------------------------------------------------------------------
const h = vi.hoisted(() => {
  type Callback = (err: Error | null, msg: unknown) => void

  class FakeSub {
    active = true
    constructor(public subject: string, public callback: Callback) {}
    drain = vi.fn(async () => { this.active = false })
    unsubscribe = vi.fn(() => { this.active = false })
  }

  class FakeConn {
    closedFlag = false
    drainingFlag = false
    /** drain() never finishes (e.g. it can't flush while disconnected) */
    hangDrain = false
    /** flush() never gets a PONG back (silently dead socket) */
    hangFlush = false
    subs: FakeSub[] = []

    private events: { type: string }[] = []
    private waiter: ((r: IteratorResult<{ type: string }>) => void) | null = null
    private resolveClosed!: () => void
    private closedPromise = new Promise<void>(r => (this.resolveClosed = r))

    request = vi.fn(async (subject: string, data: string) => {
      if (this.closedFlag) throw new Error('connection closed')
      return { json: () => ({ success: true, subject, echo: JSON.parse(data) }) }
    })

    publish = vi.fn((_subject: string, _data: string) => {
      if (this.closedFlag) throw new Error('connection closed')
    })

    drain = vi.fn(async () => {
      this.drainingFlag = true
      if (this.hangDrain) return new Promise<void>(() => {})
      await this.close()
    })

    flush = vi.fn(async () => {
      if (this.hangFlush || this.closedFlag) return new Promise<void>(() => {})
    })

    subscribe(subject: string, opts: { callback: Callback }) {
      if (this.closedFlag) throw new Error('connection closed')
      const sub = new FakeSub(subject, opts.callback)
      this.subs.push(sub)
      return sub
    }

    activeSubs(subject: string) {
      return this.subs.filter(s => s.active && s.subject === subject)
    }

    /** simulate the server delivering a message */
    deliver(subject: string, data: unknown) {
      for (const s of this.activeSubs(subject)) {
        s.callback(null, { json: () => data, string: () => JSON.stringify(data) })
      }
    }

    /** simulate a status event from nats.js */
    emit(type: string) {
      if (this.closedFlag && type !== 'close') return
      const ev = { type }
      if (this.waiter) {
        const w = this.waiter
        this.waiter = null
        w({ value: ev, done: false })
      } else {
        this.events.push(ev)
      }
    }

    status() {
      return {
        [Symbol.asyncIterator]: () => ({
          next: () =>
            new Promise<IteratorResult<{ type: string }>>(resolve => {
              if (this.events.length) resolve({ value: this.events.shift()!, done: false })
              else if (this.closedFlag) resolve({ value: undefined, done: true })
              else this.waiter = resolve
            }),
        }),
      }
    }

    closed() { return this.closedPromise }
    isClosed() { return this.closedFlag }
    isDraining() { return this.drainingFlag && !this.closedFlag }

    async close() {
      if (this.closedFlag) return
      this.closedFlag = true
      this.subs.forEach(s => (s.active = false))
      this.emit('close')
      if (this.waiter) {
        const w = this.waiter
        this.waiter = null
        w({ value: undefined, done: true })
      }
      this.resolveClosed()
    }
  }

  const state = {
    conns: [] as FakeConn[],
    /** when set, wsconnect() waits for it: lets a test hold a connect "in flight" */
    gate: null as Promise<void> | null,
  }

  const wsconnect = vi.fn(async (_opts: unknown) => {
    const conn = new FakeConn()
    state.conns.push(conn)
    if (state.gate) await state.gate
    return conn
  })

  const jwtAuthenticator = vi.fn((jwt: string) => ({ jwt }))

  return { FakeConn, state, wsconnect, jwtAuthenticator }
})

vi.mock('@nats-io/nats-core', () => ({
  wsconnect: h.wsconnect,
  jwtAuthenticator: h.jwtAuthenticator,
}))

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
type Mod = typeof import('./index.js')
type FakeConn = InstanceType<typeof h.FakeConn>

let m: Mod
let errorSpy: ReturnType<typeof vi.spyOn>

const CONFIG = { servers: ['ws://test:4222'] }
const MSG = { action: 'ping', data: { n: 1 } }

/** let pending promise chains run (no timers) */
async function settle() {
  for (let i = 0; i < 50; i++) await Promise.resolve()
}

/** connect and return the fake connection that was created */
async function connected(): Promise<FakeConn> {
  await m.messagingService.connect(CONFIG)
  await settle()
  return h.state.conns[h.state.conns.length - 1]
}

beforeEach(async () => {
  vi.useFakeTimers()
  h.state.conns.length = 0
  h.state.gate = null
  h.wsconnect.mockClear()
  h.jwtAuthenticator.mockClear()
  errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
  // fresh singleton for every test
  vi.resetModules()
  m = await import('./index.js')
})

afterEach(() => {
  errorSpy.mockRestore()
  vi.useRealTimers()
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
describe('connectMessagingService', () => {
  it('passes user/pass for userpass auth', async () => {
    await m.connectMessagingService({
      authType: 'userpass',
      servers: CONFIG.servers,
      credentials: { username: 'u', password: 'p' },
    })
    expect(h.wsconnect).toHaveBeenCalledWith(expect.objectContaining({ user: 'u', pass: 'p' }))
  })

  it('uses a jwt authenticator for jwt auth', async () => {
    await m.connectMessagingService({
      authType: 'jwt',
      servers: CONFIG.servers,
      credentials: { jwt: 'token' },
    })
    expect(h.jwtAuthenticator).toHaveBeenCalledWith('token')
    expect(h.wsconnect).toHaveBeenCalledWith(
      expect.objectContaining({ authenticator: { jwt: 'token' } }),
    )
  })

  it('throws on an unsupported auth type', () => {
    expect(() =>
      m.connectMessagingService({ authType: 'nope', servers: [], credentials: {} } as never),
    ).toThrow('Auth type not supported')
  })

  it('concurrent connect() calls share one connection', async () => {
    const [a, b] = await Promise.all([
      m.messagingService.connect(CONFIG),
      m.messagingService.connect(CONFIG),
    ])
    expect(a).toBe(b)
    expect(h.wsconnect).toHaveBeenCalledTimes(1)
  })
})

describe('request / publish', () => {
  it('a request made before connect waits, then succeeds', async () => {
    const p = m.request('svc.echo', MSG)
    const conn = await connected()
    await expect(p).resolves.toMatchObject({ success: true, subject: 'svc.echo', echo: MSG })
    expect(conn.request).toHaveBeenCalledTimes(1)
  })

  it('a request rejects with "not connected" if no connection arrives in time', async () => {
    const p = m.request('svc.echo', MSG, { timeout: 1000 })
    const assertion = expect(p).rejects.toThrow('not connected')
    await vi.advanceTimersByTimeAsync(1000)
    await assertion
  })

  it('publish waits for the connection and sends JSON', async () => {
    const p = m.publish('events.x', MSG)
    const conn = await connected()
    await p
    expect(conn.publish).toHaveBeenCalledWith('events.x', JSON.stringify(MSG))
  })

  it('publish rejects with "not connected" if no connection arrives in time', async () => {
    const p = m.publish('events.x', MSG)
    const assertion = expect(p).rejects.toThrow('not connected')
    await vi.advanceTimersByTimeAsync(10_000)
    await assertion
  })

  it('requests wait during a disconnect and continue after nats.js reconnects', async () => {
    const conn = await connected()
    conn.emit('disconnect')
    await settle()

    const p = m.request('svc.echo', MSG)
    await settle()
    expect(conn.request).not.toHaveBeenCalled()

    conn.emit('reconnect')
    await expect(p).resolves.toMatchObject({ success: true })
    expect(conn.request).toHaveBeenCalledTimes(1)
  })

  it('a request made while disconnected is not stranded by a second disconnect event', async () => {
    const conn = await connected()
    conn.emit('disconnect')
    await settle()
    const p = m.request('svc.echo', MSG)
    conn.emit('disconnect') // second reset() while pending must keep the same promise
    await settle()
    conn.emit('reconnect')
    await expect(p).resolves.toMatchObject({ success: true })
  })
})

describe('subscriptions', () => {
  it('a subscription made before connect becomes active once connected', async () => {
    const handler = vi.fn()
    const subP = m.subscribeWithMsgHandler('x', handler)
    const conn = await connected()
    await subP

    conn.deliver('x', MSG)
    expect(handler).toHaveBeenCalledTimes(1)
    expect(handler).toHaveBeenCalledWith('x', MSG)
  })

  it('subscribing twice to a channel creates only one NATS subscription', async () => {
    const h1 = vi.fn()
    const h2 = vi.fn()
    const a = m.subscribeWithMsgHandler('x', h1)
    const b = m.subscribeWithMsgHandler('x', h2)
    const conn = await connected()
    expect(await a).toBe(await b)
    expect(conn.activeSubs('x')).toHaveLength(1)
  })

  it('unsubscribe drains the subscription', async () => {
    const handler = vi.fn()
    const conn = await connected()
    await m.subscribeWithMsgHandler('x', handler)

    await m.unsubscribe('x')
    expect(conn.subs[0].drain).toHaveBeenCalledTimes(1)

    conn.deliver('x', MSG)
    expect(handler).not.toHaveBeenCalled()
  })

  it('subscribe → unsubscribe → subscribe before connecting leaves exactly one live subscription', async () => {
    const h1 = vi.fn()
    const h2 = vi.fn()
    m.subscribeWithMsgHandler('x', h1).catch(() => {}) // will be cancelled
    const unsub = m.unsubscribe('x')
    const second = m.subscribeWithMsgHandler('x', h2)

    const conn = await connected()
    await unsub
    await second

    expect(conn.activeSubs('x')).toHaveLength(1)
    conn.deliver('x', MSG)
    expect(h1).not.toHaveBeenCalled()
    expect(h2).toHaveBeenCalledTimes(1)
  })

  it('a subscription cancelled by unsubscribe before connecting rejects with "cancelled"', async () => {
    const p = m.subscribeWithMsgHandler('x', vi.fn())
    const assertion = expect(p).rejects.toThrow('was cancelled')
    const unsub = m.unsubscribe('x') // resolves only once a connection exists
    await connected()
    await assertion
    await unsub
  })
})

describe('connect / disconnect lifecycle', () => {
  it('disconnect() during an in-flight connect cancels it, and the next connect works', async () => {
    let release!: () => void
    h.state.gate = new Promise<void>(r => (release = r))

    const first = m.messagingService.connect(CONFIG)
    await m.messagingService.disconnect()

    h.state.gate = null
    const second = m.messagingService.connect(CONFIG)
    release()

    await expect(first).resolves.toBeNull()
    expect(await second).toBe(h.state.conns[1])
    await settle()
    expect(h.state.conns[0].isClosed()).toBe(true)

    await expect(m.request('svc.echo', MSG)).resolves.toMatchObject({ success: true })
    expect(h.state.conns[1].request).toHaveBeenCalledTimes(1)
    expect(h.state.conns[0].request).not.toHaveBeenCalled()
  })

  it('disconnect() forgets subscriptions: they are not restored on the next connect', async () => {
    await connected()
    await m.subscribeWithMsgHandler('x', vi.fn())

    await m.messagingService.disconnect()
    const conn2 = await connected()

    expect(conn2.subs).toHaveLength(0)
  })

  it('reconnect() moves subscriptions to the new connection, exactly once', async () => {
    const handler = vi.fn()
    const conn1 = await connected()
    await m.subscribeWithMsgHandler('x', handler)

    await m.messagingService.reconnect()
    await settle()
    const conn2 = h.state.conns[1]

    expect(conn1.isClosed()).toBe(true)
    expect(conn2.activeSubs('x')).toHaveLength(1)
    conn2.deliver('x', MSG)
    expect(handler).toHaveBeenCalledTimes(1)
  })

  it('reconnect() during an outage does not duplicate subscriptions made during the outage', async () => {
    const handler = vi.fn()
    const conn1 = await connected()
    conn1.emit('disconnect')
    await settle()

    m.subscribeWithMsgHandler('y', handler) // pending: no connection right now
    await m.messagingService.reconnect()
    await settle()
    const conn2 = h.state.conns[1]

    expect(conn2.activeSubs('y')).toHaveLength(1)
    conn2.deliver('y', MSG)
    expect(handler).toHaveBeenCalledTimes(1)
  })

  it('after the server closes the connection, reconnectIfNeeded() restores it and its subscriptions', async () => {
    const handler = vi.fn()
    const conn1 = await connected()
    await m.subscribeWithMsgHandler('x', handler)

    await conn1.close() // nats.js gave up (auth error, max reconnects...)
    await settle()

    await m.messagingService.reconnectIfNeeded()
    await settle()
    const conn2 = h.state.conns[1]

    expect(conn2).toBeDefined()
    expect(conn2.activeSubs('x')).toHaveLength(1)
    conn2.deliver('x', MSG)
    expect(handler).toHaveBeenCalledTimes(1)
    await expect(m.request('svc.echo', MSG)).resolves.toMatchObject({ success: true })
  })

  it('a hanging drain does not block reconnect(), and late events from the old connection are ignored', async () => {
    const conn1 = await connected()
    conn1.emit('disconnect')
    await settle()
    conn1.hangDrain = true // can't flush while disconnected

    const reconnecting = m.messagingService.reconnect()
    await settle()

    // the old connection "reconnects" late: must NOT be handed out to callers
    conn1.emit('reconnect')
    await settle()
    const req = m.request('svc.echo', MSG)
    await settle()
    expect(conn1.request).not.toHaveBeenCalled()

    await vi.advanceTimersByTimeAsync(3000) // drain timeout → force close
    await reconnecting

    await expect(req).resolves.toMatchObject({ success: true })
    expect(conn1.isClosed()).toBe(true)
    expect(h.state.conns[1].request).toHaveBeenCalledTimes(1)
  })
})

describe('reconnectIfNeeded', () => {
  it('does nothing before connect() was ever called', async () => {
    await m.messagingService.reconnectIfNeeded()
    expect(h.wsconnect).not.toHaveBeenCalled()
  })

  it('does nothing when the connection is healthy', async () => {
    const conn = await connected()
    await m.messagingService.reconnectIfNeeded()
    expect(conn.flush).toHaveBeenCalledTimes(1)
    expect(h.wsconnect).toHaveBeenCalledTimes(1)
  })

  it('forces a reconnect when nats.js is stuck reconnecting (app was minimized)', async () => {
    const handler = vi.fn()
    const conn1 = await connected()
    await m.subscribeWithMsgHandler('x', handler)

    conn1.emit('disconnect')
    conn1.emit('reconnecting')
    await settle()
    conn1.hangDrain = true

    const p = m.messagingService.reconnectIfNeeded()
    await vi.advanceTimersByTimeAsync(3000)
    await p
    await settle()

    expect(h.wsconnect).toHaveBeenCalledTimes(2)
    const conn2 = h.state.conns[1]
    conn2.deliver('x', MSG)
    expect(handler).toHaveBeenCalledTimes(1)
  })

  it('forces a reconnect when the socket is silently dead (ping gets no reply)', async () => {
    const conn1 = await connected()
    conn1.hangFlush = true

    const p = m.messagingService.reconnectIfNeeded()
    await vi.advanceTimersByTimeAsync(2000) // ping timeout
    await p

    expect(h.wsconnect).toHaveBeenCalledTimes(2)
    expect(conn1.isClosed()).toBe(true)
  })

  it('concurrent calls (visibilitychange + focus) reconnect only once', async () => {
    const conn1 = await connected()
    conn1.emit('disconnect')
    await settle()

    const a = m.messagingService.reconnectIfNeeded()
    const b = m.messagingService.reconnectIfNeeded()
    expect(a).toBe(b)
    await a

    expect(h.wsconnect).toHaveBeenCalledTimes(2)
  })
})