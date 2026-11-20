import WebSocket from 'ws';
import { applyResponseId, extractRequestId } from './errors.js';
import { BoundedRateQueue, TokenBucket } from './rateLimiter.js';

function createProxyError(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function isClioCurrentRejection(msg) {
  return Boolean(
    msg &&
      msg.status === 'error' &&
      typeof msg.error_message === 'string' &&
      /clio/i.test(msg.error_message) &&
      /validated/i.test(msg.error_message),
  );
}

function isLedgerNotFoundError(msg) {
  return Boolean(
    msg &&
      msg.status === 'error' &&
      (msg.error === 'lgrNotFound' || msg.error === 'ledgerNotFound'),
  );
}

class UpstreamPoolConnection {
  constructor(node, index, manager) {
    this.node = node;
    this.index = index;
    this.manager = manager;
    this.ws = null;
    this.outboundQueue = [];
    this.pingTimer = null;
    this.pongTimer = null;
    this.missedPongs = 0;
    this.reconnectTimer = null;
    this.reconnectAttempts = 0;
    this.destroyed = false;
    this.hasConnectedOnce = false;
    this.ledgerSubscribed = false;
  }

  get isOpen() {
    return Boolean(this.ws && this.ws.readyState === WebSocket.OPEN);
  }

  get isConnecting() {
    return Boolean(this.ws && this.ws.readyState === WebSocket.CONNECTING);
  }

  connect() {
    if (this.destroyed) return;
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    let ws;
    try {
      ws = new WebSocket(this.node.wsUrl, {
        handshakeTimeout: Math.min(this.manager.limits.requestTimeoutMs, 10000),
      });
    } catch (err) {
      this.handleFailure(err);
      this.scheduleReconnect();
      return;
    }

    this.ws = ws;

    const onOpen = () => {
      if (this.destroyed || this.ws !== ws) return;
      this.reconnectAttempts = 0;
      this.missedPongs = 0;
      this.hasConnectedOnce = true;
      this.ledgerSubscribed = false;
      this.node.onPoolConnected();
      this.startPing();
      this.flushOutboundQueue();
      if (this.index === 0 && this.manager.isLedgerStreamEnabled()) {
        this.manager.subscribePoolLedger(this.node, this);
      }
    };

    const onPong = () => {
      if (this.destroyed || this.ws !== ws) return;
      this.missedPongs = 0;
      this.stopPongTimer();
    };

    const onMessage = (rawData) => {
      if (this.destroyed || this.ws !== ws) return;
      this.missedPongs = 0;
      this.stopPongTimer();
      this.manager.handlePoolMessage(this.node, this, rawData);
    };

    const onError = () => {
      // Close event handles cleanup & failure recording
    };

    const onClose = () => {
      this.stopPing();
      this.missedPongs = 0;
      this.ledgerSubscribed = false;
      ws.removeAllListeners();
      if (this.ws === ws) {
        this.ws = null;
      }

      const pendingError = createProxyError(
        'upstreamUnavailable',
        `Upstream node "${this.node.id}" connection closed`,
      );
      this.rejectPendingForConn(pendingError);
      this.rejectOutboundQueue(pendingError);

      if (!this.destroyed) {
        this.node.recordFailure('poolDisconnect');
        this.scheduleReconnect();
      }
    };

    ws.on('open', onOpen);
    ws.on('pong', onPong);
    ws.on('message', onMessage);
    ws.on('error', onError);
    ws.on('close', onClose);
  }

  stopPongTimer() {
    if (this.pongTimer) {
      clearTimeout(this.pongTimer);
      this.pongTimer = null;
    }
  }

  startPing() {
    this.stopPing();
    const intervalMs = this.manager.limits.upstreamPingIntervalMs;
    if (!intervalMs || intervalMs <= 0) return;

    const rawPongTimeout = this.manager.limits.upstreamPongTimeoutMs ?? 10000;
    const pongTimeoutMs = Math.max(
      10,
      Math.min(rawPongTimeout, Math.max(10, intervalMs - 10)),
    );
    const maxMissed = Math.max(1, this.manager.limits.upstreamMaxMissedPongs ?? 2);

    this.pingTimer = setInterval(() => {
      const socket = this.ws;
      if (!socket || socket.readyState !== WebSocket.OPEN) return;

      this.stopPongTimer();
      try {
        socket.ping();
      } catch {
        return;
      }

      this.pongTimer = setTimeout(() => {
        this.pongTimer = null;
        if (this.destroyed || this.ws !== socket || socket.readyState !== WebSocket.OPEN) {
          return;
        }
        this.missedPongs += 1;
        if (this.missedPongs >= maxMissed) {
          console.warn(
            `[proxy] Upstream node "${this.node.id}" missed ${this.missedPongs} pongs, terminating dead connection`,
          );
          this.stopPing();
          try {
            socket.terminate();
          } catch {
            // ignore
          }
        }
      }, pongTimeoutMs);

      if (typeof this.pongTimer.unref === 'function') {
        this.pongTimer.unref();
      }
    }, intervalMs);

    if (typeof this.pingTimer.unref === 'function') {
      this.pingTimer.unref();
    }
  }

  stopPing() {
    this.stopPongTimer();
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  scheduleReconnect() {
    if (this.destroyed || this.reconnectTimer) return;
    const base = this.manager.limits.reconnectBaseDelayMs;
    const max = this.manager.limits.reconnectMaxDelayMs;
    const exp = Math.min(max, base * Math.pow(2, this.reconnectAttempts));
    const jitter = 0.5 + Math.random() * 0.5;
    const delay = Math.max(50, Math.round(exp * jitter));
    this.reconnectAttempts += 1;

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);

    if (typeof this.reconnectTimer.unref === 'function') {
      this.reconnectTimer.unref();
    }
  }

  sendRaw(serialized) {
    if (this.destroyed) {
      return false;
    }
    if (this.isOpen) {
      try {
        this.ws.send(serialized);
        return true;
      } catch {
        return false;
      }
    }
    if (this.isConnecting) {
      this.outboundQueue.push(serialized);
      return true;
    }
    return false;
  }

  flushOutboundQueue() {
    while (this.outboundQueue.length > 0 && this.isOpen) {
      const msg = this.outboundQueue.shift();
      try {
        this.ws.send(msg);
      } catch {
        break;
      }
    }
  }

  rejectOutboundQueue(err) {
    while (this.outboundQueue.length > 0) {
      const msg = this.outboundQueue.shift();
      try {
        const parsed = JSON.parse(msg);
        if (parsed && parsed.id !== undefined) {
          this.manager.rejectPendingById(parsed.id, err);
        }
      } catch {
        // ignore
      }
    }
  }

  rejectPendingForConn(err) {
    this.manager.rejectPendingByConn(this, err);
  }

  handleFailure() {
    this.node.recordFailure('poolConnectError');
  }

  destroy() {
    this.destroyed = true;
    this.stopPing();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    const err = createProxyError('upstreamUnavailable', 'Proxy is shutting down');
    this.rejectPendingForConn(err);
    this.rejectOutboundQueue(err);
    if (this.ws) {
      const socket = this.ws;
      this.ws = null;
      socket.removeAllListeners();
      try {
        if (
          socket.readyState === WebSocket.OPEN ||
          socket.readyState === WebSocket.CONNECTING
        ) {
          socket.terminate();
        }
      } catch {
        // ignore
      }
    }
  }
}

export class NodeRuntime {
  constructor(nodeConfig, limits, manager) {
    this.id = nodeConfig.id;
    this.wsUrl = nodeConfig.wsUrl;
    this.roles = [...nodeConfig.roles];
    this.poolSize = nodeConfig.poolSize ?? 2;
    this.maxPathConns = nodeConfig.maxPathConns ?? 0;
    this.maxSubscribeConns = nodeConfig.maxSubscribeConns ?? 0;
    this.limits = limits;
    this.manager = manager;

    this.bucket = new TokenBucket({
      ratePerSec: nodeConfig.rateLimit?.ratePerSec ?? 15,
      burst: nodeConfig.rateLimit?.burst ?? 30,
      slowDownRecoveryMs: limits.slowDownRecoveryMs,
    });

    this.pool = [];
    this.rrIndex = 0;

    // Circuit breaker state
    this.circuitState = 'CLOSED'; // CLOSED | OPEN | HALF_OPEN
    this.consecutiveFailures = 0;
    this.cooldownUntil = null;
    this.cooldownTimer = null;
    this.halfOpenInFlight = false;

    // Stateful slots
    this.activePathConns = 0;
    this.activeSubscribeConns = 0;

    // Telemetry & ledger capability learning
    this.totalRequests = 0;
    this.totalErrors = 0;
    this.requestTimestamps = [];
    this.lastLoadWarningAt = null;
    this.lastErrorMessage = null;
    this.lastValidatedSeq = null;
    this.lastValidatedAt = null;
    this.clioVersion = null;
    this.currentRejectedUntil = 0;
    this.ledgerStreamSupported = true;
  }

  onPoolConnected() {
    this.currentRejectedUntil = 0;
    this.clioVersion = null;
    this.ledgerStreamSupported = true;
  }

  isCurrentRejected(now = Date.now()) {
    return this.currentRejectedUntil > now;
  }

  markCurrentRejected(cooldownMs = this.limits.clioCooldownMs ?? 120000) {
    this.currentRejectedUntil = Date.now() + cooldownMs;
  }

  extractLedgerSnapshot(obj) {
    if (!obj || typeof obj !== 'object') return null;
    const rawSeq = obj.ledger_index;
    if (typeof rawSeq !== 'number' || !Number.isFinite(rawSeq) || rawSeq <= 0) {
      return null;
    }
    const seq = Math.trunc(rawSeq);
    const snapshot = { ledger_index: seq };
    if (typeof obj.ledger_hash === 'string') snapshot.ledger_hash = obj.ledger_hash;
    if (typeof obj.ledger_time === 'number') snapshot.ledger_time = obj.ledger_time;
    if (typeof obj.fee_base === 'number') snapshot.fee_base = obj.fee_base;
    if (typeof obj.fee_ref === 'number') snapshot.fee_ref = obj.fee_ref;
    if (typeof obj.reserve_base === 'number') snapshot.reserve_base = obj.reserve_base;
    if (typeof obj.reserve_inc === 'number') snapshot.reserve_inc = obj.reserve_inc;
    if (typeof obj.txn_count === 'number') snapshot.txn_count = obj.txn_count;
    if (typeof obj.validated_ledgers === 'string') {
      snapshot.validated_ledgers = obj.validated_ledgers;
    }
    return snapshot;
  }

  updateLedgerTelemetryFromEvent(msg) {
    const snapshot = this.extractLedgerSnapshot(msg);
    if (!snapshot) return null;
    this.lastValidatedSeq =
      this.lastValidatedSeq === null
        ? snapshot.ledger_index
        : Math.max(this.lastValidatedSeq, snapshot.ledger_index);
    this.lastValidatedAt = Date.now();
    return snapshot;
  }

  updateLedgerTelemetry(msg) {
    if (!msg || typeof msg !== 'object' || msg.status === 'error') return;
    const result = msg.result;
    if (!result || typeof result !== 'object') return;

    if (typeof result.info?.clio_version === 'string') {
      this.clioVersion = result.info.clio_version;
    }

    let seq = null;
    if (
      typeof result.info?.validated_ledger?.seq === 'number' &&
      Number.isFinite(result.info.validated_ledger.seq)
    ) {
      seq = Math.trunc(result.info.validated_ledger.seq);
    } else if (
      typeof result.ledger_index === 'number' &&
      Number.isFinite(result.ledger_index) &&
      (result.validated === true ||
        typeof result.validated_ledgers === 'string' ||
        (typeof result.ledger_hash === 'string' &&
          result.ledger_current_index === undefined))
    ) {
      seq = Math.trunc(result.ledger_index);
    }

    if (seq !== null && seq > 0) {
      this.lastValidatedSeq =
        this.lastValidatedSeq === null ? seq : Math.max(this.lastValidatedSeq, seq);
      this.lastValidatedAt = Date.now();
    }
  }

  startPool() {
    if (this.pool.length > 0) return;
    for (let i = 0; i < this.poolSize; i += 1) {
      const conn = new UpstreamPoolConnection(this, i, this.manager);
      this.pool.push(conn);
      conn.connect();
    }
  }

  hasRole(role) {
    return this.roles.includes(role);
  }

  getActivePoolCount() {
    let count = 0;
    for (const conn of this.pool) {
      if (conn.isOpen) count += 1;
    }
    return count;
  }

  hasAvailablePoolConnection() {
    for (const conn of this.pool) {
      if (conn.isOpen || (!conn.hasConnectedOnce && conn.isConnecting)) return true;
    }
    return false;
  }

  hasOpenPoolConnection() {
    for (const conn of this.pool) {
      if (conn.isOpen) return true;
    }
    return false;
  }

  pickPoolConnection() {
    if (this.pool.length === 0) return null;
    const len = this.pool.length;
    for (let offset = 0; offset < len; offset += 1) {
      const idx = (this.rrIndex + offset) % len;
      const conn = this.pool[idx];
      if (conn.isOpen) {
        this.rrIndex = (idx + 1) % len;
        return conn;
      }
    }
    for (let offset = 0; offset < len; offset += 1) {
      const idx = (this.rrIndex + offset) % len;
      const conn = this.pool[idx];
      if (!conn.hasConnectedOnce && conn.isConnecting) {
        this.rrIndex = (idx + 1) % len;
        return conn;
      }
    }
    return null;
  }

  isHealthyForTraffic() {
    if (this.circuitState === 'OPEN') {
      if (this.cooldownUntil && Date.now() >= this.cooldownUntil) {
        this.transitionToHalfOpen();
      } else {
        return false;
      }
    }
    if (this.circuitState === 'HALF_OPEN' && this.halfOpenInFlight) {
      return false;
    }
    return true;
  }

  recordRequestStart() {
    const now = Date.now();
    this.totalRequests += 1;
    this.requestTimestamps.push(now);
    if (this.requestTimestamps.length > 2000) {
      this.pruneTimestamps(now);
    }
  }

  pruneTimestamps(now = Date.now()) {
    const cutoff = now - 5000;
    while (this.requestTimestamps.length > 0 && this.requestTimestamps[0] < cutoff) {
      this.requestTimestamps.shift();
    }
  }

  getQps(now = Date.now()) {
    this.pruneTimestamps(now);
    return Number((this.requestTimestamps.length / 5).toFixed(2));
  }

  triggerSlowDown() {
    this.lastLoadWarningAt = Date.now();
    this.bucket.triggerSlowDown(this.limits.slowDownRecoveryMs);
  }

  recordSuccess() {
    this.consecutiveFailures = 0;
    this.lastErrorMessage = null;
    if (this.circuitState === 'HALF_OPEN') {
      this.setCircuitState('CLOSED');
    }
  }

  recordFailure(reason = 'error') {
    this.totalErrors += 1;
    this.lastErrorMessage = reason;

    if (this.circuitState === 'HALF_OPEN') {
      this.halfOpenInFlight = false;
      this.tripCircuit();
      return;
    }

    this.consecutiveFailures += 1;
    if (
      this.circuitState === 'CLOSED' &&
      this.consecutiveFailures >= this.limits.circuitBreakerThreshold
    ) {
      this.tripCircuit();
    }
  }

  tripCircuit() {
    if (this.cooldownTimer) {
      clearTimeout(this.cooldownTimer);
      this.cooldownTimer = null;
    }
    const prev = this.circuitState;
    this.circuitState = 'OPEN';
    this.halfOpenInFlight = false;
    this.cooldownUntil = Date.now() + this.limits.nodeCooldownMs;

    if (prev !== 'OPEN') {
      console.warn(
        `[proxy] Circuit OPEN for node "${this.id}" (cooldown ${this.limits.nodeCooldownMs}ms, failures=${this.consecutiveFailures})`,
      );
    }

    this.cooldownTimer = setTimeout(() => {
      this.cooldownTimer = null;
      if (this.circuitState === 'OPEN') {
        this.transitionToHalfOpen();
      }
    }, this.limits.nodeCooldownMs);

    if (typeof this.cooldownTimer.unref === 'function') {
      this.cooldownTimer.unref();
    }
  }

  transitionToHalfOpen() {
    if (this.cooldownTimer) {
      clearTimeout(this.cooldownTimer);
      this.cooldownTimer = null;
    }
    this.circuitState = 'HALF_OPEN';
    this.cooldownUntil = null;
    this.halfOpenInFlight = false;
  }

  setCircuitState(state) {
    if (this.cooldownTimer) {
      clearTimeout(this.cooldownTimer);
      this.cooldownTimer = null;
    }
    if (state === 'OPEN') {
      this.tripCircuit();
      return;
    }
    this.circuitState = 'CLOSED';
    this.consecutiveFailures = 0;
    this.cooldownUntil = null;
    this.halfOpenInFlight = false;
  }

  forceReconnectPool() {
    for (const conn of this.pool) {
      if (!conn.isOpen && !conn.isConnecting && !conn.destroyed) {
        if (conn.reconnectTimer) {
          clearTimeout(conn.reconnectTimer);
          conn.reconnectTimer = null;
        }
        conn.reconnectAttempts = 0;
        conn.connect();
      }
    }
  }

  getStats(now = Date.now()) {
    const activePoolConns = this.getActivePoolCount();
    const cooldownRemainingSec =
      this.circuitState === 'OPEN' && this.cooldownUntil
        ? Math.max(0, Math.ceil((this.cooldownUntil - now) / 1000))
        : 0;

    return {
      id: this.id,
      wsUrl: this.wsUrl,
      roles: [...this.roles],
      circuitState: this.circuitState,
      isOnline: activePoolConns > 0,
      cooldownRemainingSec,
      consecutiveFailures: this.consecutiveFailures,
      poolSize: this.poolSize,
      activePoolConns,
      activePathConns: this.activePathConns,
      maxPathConns: this.maxPathConns,
      activeSubscribeConns: this.activeSubscribeConns,
      maxSubscribeConns: this.maxSubscribeConns,
      tokens: Number(this.bucket.getAvailableTokens(now).toFixed(2)),
      burst: this.bucket.burst,
      baseRatePerSec: this.bucket.baseRatePerSec,
      effectiveRatePerSec: Number(this.bucket.effectiveRate.toFixed(2)),
      rateMultiplier: this.bucket.rateMultiplier,
      isSlowedDown: this.bucket.rateMultiplier < 1,
      totalRequests: this.totalRequests,
      totalErrors: this.totalErrors,
      qps: this.getQps(now),
      lastLoadWarningAt: this.lastLoadWarningAt,
      lastValidatedSeq: this.lastValidatedSeq,
      lastValidatedAt: this.lastValidatedAt,
      clioVersion: this.clioVersion,
      currentRejectedCooldownSec:
        this.currentRejectedUntil > now
          ? Math.ceil((this.currentRejectedUntil - now) / 1000)
          : 0,
    };
  }

  destroy() {
    if (this.cooldownTimer) {
      clearTimeout(this.cooldownTimer);
      this.cooldownTimer = null;
    }
    this.bucket.destroy();
    for (const conn of this.pool) {
      conn.destroy();
    }
    this.pool = [];
  }
}

export class NodeManager {
  constructor(config) {
    this.config = config;
    this.limits = config.limits;
    this.nodes = config.nodes.map((nodeCfg) => new NodeRuntime(nodeCfg, this.limits, this));
    this.nodeMap = new Map(this.nodes.map((n) => [n.id, n]));
    this.nextProxyId = 1;
    this.pendingRequests = new Map();
    this.rateQueue = new BoundedRateQueue({
      maxLength: this.limits.queueMaxLength,
      maxWaitMs: this.limits.queueMaxWaitMs,
    });
    this.started = false;
    this.ledgerSnapshots = new Map();
    this.highestSnapshotSeq = 0;
    this.lastBroadcastSeq = 0;
    this.onSafeLedgerAdvanced = null;
    this.pendingLedgerSubWaiters = [];
    this.streamSettleDeadline = 0;
    this.streamSettleTimer = null;
  }

  isLedgerStreamEnabled() {
    return Boolean(this.config.ledgerStream?.enabled);
  }

  hasPendingInitialLedgerNodes(now = Date.now()) {
    if (!this.isLedgerStreamEnabled() || now >= this.streamSettleDeadline) {
      return false;
    }
    return this.nodes.some(
      (n) =>
        n.ledgerStreamSupported &&
        n.circuitState !== 'OPEN' &&
        n.lastValidatedSeq === null &&
        n.pool[0] &&
        !n.pool[0].destroyed,
    );
  }

  start() {
    if (this.started) return;
    this.started = true;
    if (this.isLedgerStreamEnabled()) {
      const settleMs = this.config.ledgerStream?.startupSettleMs ?? 400;
      this.streamSettleDeadline = Date.now() + settleMs;
      if (this.streamSettleTimer) {
        clearTimeout(this.streamSettleTimer);
      }
      this.streamSettleTimer = setTimeout(() => {
        this.streamSettleTimer = null;
        this.flushPendingLedgerWaiters();
        this.evaluateSafeBroadcast();
      }, settleMs + 10);
      if (typeof this.streamSettleTimer.unref === 'function') {
        this.streamSettleTimer.unref();
      }
    }
    for (const node of this.nodes) {
      node.startPool();
    }
  }

  getNode(id) {
    return this.nodeMap.get(id) || null;
  }

  getEligibleStatelessNodes(role, excludedNodeIds = null) {
    const roleNodes = this.nodes.filter((n) => n.hasRole(role));
    const healthyConnected = roleNodes.filter(
      (n) =>
        n.isHealthyForTraffic() &&
        n.hasAvailablePoolConnection() &&
        (!excludedNodeIds || !excludedNodeIds.has(n.id)),
    );
    return { roleNodes, healthyConnected };
  }

  tryAcquireStatelessCandidate(role, weight, options = {}) {
    const {
      excludedNodeIds = null,
      requestedLedgerIndex = null,
      requiresCurrent = false,
      preferSafeLag = false,
    } = options;

    const { roleNodes, healthyConnected } = this.getEligibleStatelessNodes(
      role,
      excludedNodeIds,
    );
    if (roleNodes.length === 0 || healthyConnected.length === 0) {
      throw createProxyError(
        'noNodesAvailable',
        `No healthy upstream nodes available for role "${role}"`,
      );
    }

    const now = Date.now();
    let candidates = healthyConnected;

    if (requiresCurrent) {
      const nonClio = candidates.filter((n) => !n.isCurrentRejected(now));
      if (nonClio.length > 0) {
        candidates = nonClio;
      }
    }

    if (requestedLedgerIndex !== null && requestedLedgerIndex !== undefined) {
      const knownSynced = candidates.filter(
        (n) => n.lastValidatedSeq !== null && n.lastValidatedSeq >= requestedLedgerIndex,
      );
      if (knownSynced.length > 0) {
        candidates = knownSynced;
      } else {
        const unsampledOrSynced = candidates.filter(
          (n) => n.lastValidatedSeq === null || n.lastValidatedSeq >= requestedLedgerIndex,
        );
        if (unsampledOrSynced.length > 0) {
          candidates = unsampledOrSynced;
        }
      }
    }

    let maxSeq = null;
    if (preferSafeLag) {
      for (const n of candidates) {
        if (n.lastValidatedSeq !== null) {
          maxSeq = maxSeq === null ? n.lastValidatedSeq : Math.max(maxSeq, n.lastValidatedSeq);
        }
      }
    }
    const maxSafeLag = this.limits.maxSafeLedgerLag ?? 15;

    const sorted = [...candidates].sort((a, b) => {
      const aOpen = a.hasOpenPoolConnection() ? 1 : 0;
      const bOpen = b.hasOpenPoolConnection() ? 1 : 0;
      if (aOpen !== bOpen) return bOpen - aOpen;

      if (preferSafeLag && maxSeq !== null) {
        const aUnknown = a.lastValidatedSeq === null;
        const bUnknown = b.lastValidatedSeq === null;
        if (aUnknown !== bUnknown) return aUnknown ? -1 : 1;

        const aSafe =
          a.lastValidatedSeq !== null && maxSeq - a.lastValidatedSeq <= maxSafeLag;
        const bSafe =
          b.lastValidatedSeq !== null && maxSeq - b.lastValidatedSeq <= maxSafeLag;
        if (aSafe !== bSafe) return aSafe ? -1 : 1;
        if (aSafe && bSafe && a.lastValidatedSeq !== b.lastValidatedSeq) {
          return a.lastValidatedSeq - b.lastValidatedSeq;
        }
      }

      return b.bucket.getAvailableTokens(now) - a.bucket.getAvailableTokens(now);
    });

    for (const node of sorted) {
      if (node.bucket.tryConsume(weight, now)) {
        const conn = node.pickPoolConnection();
        if (conn) {
          if (node.circuitState === 'HALF_OPEN') {
            node.halfOpenInFlight = true;
          }
          return { node, conn };
        }
      }
    }

    return null;
  }

  async acquireStateless(role, weight, options = {}) {
    const immediate = this.tryAcquireStatelessCandidate(role, weight, options);
    if (immediate) {
      return immediate;
    }
    return this.rateQueue.enqueue(() =>
      this.tryAcquireStatelessCandidate(role, weight, options),
    );
  }

  async sendSingleStatelessAttempt(payload, routeInfo, excludedNodeIds) {
    const { hasOriginalId, originalId } = extractRequestId(payload);
    const { node, conn } = await this.acquireStateless(routeInfo.role, routeInfo.weight, {
      excludedNodeIds,
      requestedLedgerIndex: routeInfo.requestedLedgerIndex,
      requiresCurrent: routeInfo.requiresCurrent,
      preferSafeLag: routeInfo.preferSafeLag,
    });

    node.recordRequestStart();
    const proxyId = this.nextProxyId++;
    const upstreamPayload = { ...payload, id: proxyId };

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const entry = this.pendingRequests.get(proxyId);
        if (!entry) return;
        this.pendingRequests.delete(proxyId);
        if (node.circuitState === 'HALF_OPEN') {
          node.halfOpenInFlight = false;
        }
        node.recordFailure('upstreamTimeout');
        reject(
          createProxyError(
            'upstreamTimeout',
            `Upstream node "${node.id}" timed out after ${this.limits.requestTimeoutMs}ms`,
          ),
        );
      }, this.limits.requestTimeoutMs);

      if (typeof timer.unref === 'function') {
        timer.unref();
      }

      this.pendingRequests.set(proxyId, {
        originalId,
        hasOriginalId,
        timer,
        nodeId: node.id,
        node,
        conn,
        resolve: (restored) => resolve({ node, response: restored }),
        reject,
      });

      const sent = conn.sendRaw(JSON.stringify(upstreamPayload));
      if (!sent) {
        clearTimeout(timer);
        this.pendingRequests.delete(proxyId);
        if (node.circuitState === 'HALF_OPEN') {
          node.halfOpenInFlight = false;
        }
        node.recordFailure('sendFailed');
        reject(
          createProxyError(
            'upstreamUnavailable',
            `Upstream node "${node.id}" connection unavailable`,
          ),
        );
      }
    });
  }

  async sendStatelessRequest(payload, roleOrRouteInfo, weightArg) {
    const routeInfo =
      typeof roleOrRouteInfo === 'string'
        ? {
            role: roleOrRouteInfo,
            weight: weightArg ?? 2,
            requestedLedgerIndex: null,
            requiresCurrent: false,
            preferSafeLag: false,
          }
        : roleOrRouteInfo;

    const maxAttempts = 1 + (this.limits.ledgerRetryMaxAttempts ?? 2);
    const excludedNodeIds = new Set();
    let lastResponse = null;

    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      let attemptResult;
      try {
        attemptResult = await this.sendSingleStatelessAttempt(
          payload,
          routeInfo,
          excludedNodeIds,
        );
      } catch (err) {
        if (lastResponse && err.code === 'noNodesAvailable') {
          return lastResponse;
        }
        throw err;
      }

      const { node, response } = attemptResult;
      lastResponse = response;

      if (isClioCurrentRejection(response)) {
        node.markCurrentRejected(this.limits.clioCooldownMs);
        excludedNodeIds.add(node.id);
        const { healthyConnected } = this.getEligibleStatelessNodes(
          routeInfo.role,
          excludedNodeIds,
        );
        if (healthyConnected.length > 0 && attempt + 1 < maxAttempts) {
          continue;
        }
        return response;
      }

      if (isLedgerNotFoundError(response)) {
        excludedNodeIds.add(node.id);
        const { healthyConnected } = this.getEligibleStatelessNodes(
          routeInfo.role,
          excludedNodeIds,
        );
        if (healthyConnected.length > 0 && attempt + 1 < maxAttempts) {
          continue;
        }
        return response;
      }

      return response;
    }

    return lastResponse;
  }

  subscribePoolLedger(node, conn) {
    if (!this.isLedgerStreamEnabled() || !conn || !conn.isOpen || conn.ledgerSubscribed) {
      return;
    }
    const proxyId = this.nextProxyId++;
    const timeoutMs = Math.min(this.limits.requestTimeoutMs ?? 5000, 5000);
    const timer = setTimeout(() => {
      const entry = this.pendingRequests.get(proxyId);
      if (!entry) return;
      this.pendingRequests.delete(proxyId);
      // Do not trip circuit breaker on internal ledger subscribe timeout
      node.ledgerStreamSupported = false;
    }, timeoutMs);

    if (typeof timer.unref === 'function') {
      timer.unref();
    }

    this.pendingRequests.set(proxyId, {
      originalId: proxyId,
      hasOriginalId: true,
      isInternalLedgerSub: true,
      timer,
      nodeId: node.id,
      node,
      conn,
      resolve: () => {},
      reject: () => {},
    });

    const sent = conn.sendRaw(
      JSON.stringify({
        id: proxyId,
        command: 'subscribe',
        streams: ['ledger'],
      }),
    );
    if (!sent) {
      clearTimeout(timer);
      this.pendingRequests.delete(proxyId);
    }
  }

  recordLedgerSnapshot(snapshot) {
    if (!snapshot || typeof snapshot.ledger_index !== 'number' || snapshot.ledger_index <= 0) {
      return;
    }
    const seq = snapshot.ledger_index;
    const prev = this.ledgerSnapshots.get(seq);
    const latestSnap =
      this.highestSnapshotSeq > 0 ? this.ledgerSnapshots.get(this.highestSnapshotSeq) : null;
    const merged = {
      ...(latestSnap
        ? {
            fee_base: latestSnap.fee_base,
            fee_ref: latestSnap.fee_ref,
            reserve_base: latestSnap.reserve_base,
            reserve_inc: latestSnap.reserve_inc,
            validated_ledgers: latestSnap.validated_ledgers,
          }
        : {}),
      ...(prev || {}),
      ...snapshot,
      ledger_index: seq,
    };
    this.ledgerSnapshots.set(seq, merged);
    if (seq > this.highestSnapshotSeq) {
      this.highestSnapshotSeq = seq;
    }

    const maxSnapshots = Math.max(5, this.config.ledgerStream?.maxSnapshots ?? 30);
    if (this.ledgerSnapshots.size > maxSnapshots) {
      const cutoff = this.highestSnapshotSeq - maxSnapshots;
      for (const key of this.ledgerSnapshots.keys()) {
        if (key <= cutoff || this.ledgerSnapshots.size > maxSnapshots) {
          this.ledgerSnapshots.delete(key);
          if (this.ledgerSnapshots.size <= maxSnapshots && key > cutoff) {
            break;
          }
        }
      }
    }

    if (this.pendingLedgerSubWaiters.length > 0 && !this.hasPendingInitialLedgerNodes()) {
      this.flushPendingLedgerWaiters();
    }
  }

  flushPendingLedgerWaiters() {
    if (this.pendingLedgerSubWaiters.length === 0) return;
    const snap = this.getBestSafeSnapshot();
    if (!snap && this.hasPendingInitialLedgerNodes()) return;
    const waiters = this.pendingLedgerSubWaiters.splice(0, this.pendingLedgerSubWaiters.length);
    for (const w of waiters) {
      clearTimeout(w.timer);
      w.resolve(snap);
    }
  }

  computeLedgerWatermarks(now = Date.now()) {
    const staleMs = this.config.ledgerStream?.staleMs ?? 20000;
    const maxSafeLag = this.limits.maxSafeLedgerLag ?? 15;

    const sampledAll = this.nodes.filter((n) => typeof n.lastValidatedSeq === 'number');
    const sampledHealthyOnline = sampledAll.filter(
      (n) => n.isOnline && n.circuitState === 'CLOSED',
    );
    const freshHealthy = sampledHealthyOnline.filter(
      (n) => typeof n.lastValidatedAt === 'number' && now - n.lastValidatedAt <= staleMs,
    );

    const activePool =
      freshHealthy.length > 0
        ? freshHealthy
        : sampledHealthyOnline.length > 0
          ? sampledHealthyOnline
          : sampledAll;

    let maxValidatedSeq = null;
    let minValidatedSeq = null;
    let safeValidatedSeq = null;

    if (activePool.length > 0) {
      for (const n of activePool) {
        const seq = n.lastValidatedSeq;
        if (maxValidatedSeq === null || seq > maxValidatedSeq) {
          maxValidatedSeq = seq;
        }
        if (minValidatedSeq === null || seq < minValidatedSeq) {
          minValidatedSeq = seq;
        }
      }
      for (const n of activePool) {
        const seq = n.lastValidatedSeq;
        if (maxValidatedSeq - seq <= maxSafeLag) {
          if (safeValidatedSeq === null || seq < safeValidatedSeq) {
            safeValidatedSeq = seq;
          }
        }
      }
    }

    return {
      maxValidatedSeq,
      minValidatedSeq,
      safeValidatedSeq,
      sampledLedgerNodes: sampledAll.length,
    };
  }

  getBestSafeSnapshot() {
    const { safeValidatedSeq } = this.computeLedgerWatermarks();
    const targetSeq = safeValidatedSeq ?? this.highestSnapshotSeq;
    if (targetSeq && this.ledgerSnapshots.has(targetSeq)) {
      return this.ledgerSnapshots.get(targetSeq);
    }
    let best = null;
    for (const [seq, snap] of this.ledgerSnapshots.entries()) {
      if (targetSeq && seq > targetSeq) continue;
      if (!best || seq > best.ledger_index) {
        best = snap;
      }
    }
    if (!best && this.highestSnapshotSeq > 0) {
      best = this.ledgerSnapshots.get(this.highestSnapshotSeq) || null;
    }
    return best;
  }

  async getInitialLedgerSnapshot(timeoutMs = 1500) {
    if (!this.hasPendingInitialLedgerNodes()) {
      const existing = this.getBestSafeSnapshot();
      if (existing) return existing;
    }
    if (!this.isLedgerStreamEnabled()) return null;

    return new Promise((resolve) => {
      const waiter = {
        timer: null,
        resolve,
      };
      waiter.timer = setTimeout(() => {
        const idx = this.pendingLedgerSubWaiters.indexOf(waiter);
        if (idx !== -1) {
          this.pendingLedgerSubWaiters.splice(idx, 1);
        }
        resolve(this.getBestSafeSnapshot());
      }, timeoutMs);
      if (typeof waiter.timer.unref === 'function') {
        waiter.timer.unref();
      }
      this.pendingLedgerSubWaiters.push(waiter);
    });
  }

  evaluateSafeBroadcast() {
    if (this.hasPendingInitialLedgerNodes()) {
      return;
    }
    const { safeValidatedSeq } = this.computeLedgerWatermarks();
    if (safeValidatedSeq === null || safeValidatedSeq <= this.lastBroadcastSeq) {
      return;
    }

    let snap = this.ledgerSnapshots.get(safeValidatedSeq) || null;
    if (!snap) {
      for (const [seq, candidate] of this.ledgerSnapshots.entries()) {
        if (seq <= safeValidatedSeq && seq > this.lastBroadcastSeq) {
          if (!snap || seq > snap.ledger_index) {
            snap = candidate;
          }
        }
      }
    }

    if (snap && snap.ledger_index > this.lastBroadcastSeq) {
      this.lastBroadcastSeq = snap.ledger_index;
      if (typeof this.onSafeLedgerAdvanced === 'function') {
        this.onSafeLedgerAdvanced(snap);
      }
    }
  }

  handlePoolMessage(node, conn, rawData) {
    let msg;
    try {
      const text = typeof rawData === 'string' ? rawData : rawData.toString('utf8');
      msg = JSON.parse(text);
    } catch {
      return;
    }

    if (!msg || typeof msg !== 'object') return;

    if (msg.type === 'ledgerClosed') {
      const snapshot = node.updateLedgerTelemetryFromEvent(msg);
      if (snapshot) {
        this.recordLedgerSnapshot(snapshot);
        this.evaluateSafeBroadcast();
      }
      return;
    }

    const proxyId = msg.id;
    if (proxyId === undefined || proxyId === null) return;

    const entry = this.pendingRequests.get(proxyId);
    if (!entry) return;

    clearTimeout(entry.timer);
    this.pendingRequests.delete(proxyId);

    if (entry.isInternalLedgerSub) {
      if (msg.status === 'error') {
        // Internal background subscribe unsupported or busy: never trip circuit breaker
        node.ledgerStreamSupported = false;
        if (!this.hasPendingInitialLedgerNodes()) {
          this.flushPendingLedgerWaiters();
          this.evaluateSafeBroadcast();
        }
        return;
      }
      if (conn) {
        conn.ledgerSubscribed = true;
      }
      node.ledgerStreamSupported = true;
      node.updateLedgerTelemetry(msg);
      const snapshot = node.extractLedgerSnapshot(msg.result);
      if (snapshot) {
        this.recordLedgerSnapshot(snapshot);
        this.evaluateSafeBroadcast();
      }
      node.recordSuccess();
      return;
    }

    if (node.circuitState === 'HALF_OPEN') {
      node.halfOpenInFlight = false;
    }

    node.updateLedgerTelemetry(msg);

    // Load signal sniffing
    const isLoadWarning = msg.warning === 'load';
    const isLoadError =
      msg.status === 'error' && ['tooBusy', 'slowDown', 'highLoad'].includes(msg.error);

    if (isLoadWarning || isLoadError) {
      node.triggerSlowDown();
    }

    if (isLoadError) {
      node.recordFailure(`upstream:${msg.error}`);
    } else {
      node.recordSuccess();
    }

    const restored = applyResponseId(msg, entry.originalId, entry.hasOriginalId);
    entry.resolve(restored);
  }

  rejectPendingById(proxyId, err) {
    const entry = this.pendingRequests.get(proxyId);
    if (!entry) return;
    clearTimeout(entry.timer);
    this.pendingRequests.delete(proxyId);
    if (entry.node.circuitState === 'HALF_OPEN') {
      entry.node.halfOpenInFlight = false;
    }
    entry.reject(err);
  }

  rejectPendingByConn(conn, err) {
    for (const [proxyId, entry] of this.pendingRequests.entries()) {
      if (entry.conn === conn) {
        clearTimeout(entry.timer);
        this.pendingRequests.delete(proxyId);
        if (entry.node.circuitState === 'HALF_OPEN') {
          entry.node.halfOpenInFlight = false;
        }
        entry.reject(err);
      }
    }
  }

  getStatefulCandidates(kind) {
    const requiredRole = kind === 'path' ? 'path' : 'normal';
    const maxKey = kind === 'path' ? 'maxPathConns' : 'maxSubscribeConns';
    const activeKey = kind === 'path' ? 'activePathConns' : 'activeSubscribeConns';

    const quotaNodes = this.nodes.filter(
      (n) => n.hasRole(requiredRole) && n[maxKey] > 0,
    );
    const healthyNodes = quotaNodes.filter((n) => n.isHealthyForTraffic());
    const availableNodes = healthyNodes.filter((n) => n[activeKey] < n[maxKey]);

    return { quotaNodes, healthyNodes, availableNodes, activeKey };
  }

  tryReserveStatefulNode(kind, weight) {
    const { quotaNodes, healthyNodes, availableNodes, activeKey } =
      this.getStatefulCandidates(kind);

    if (quotaNodes.length === 0 || healthyNodes.length === 0) {
      throw createProxyError(
        'noNodesAvailable',
        `No healthy upstream nodes available for "${kind}" channel`,
      );
    }

    if (availableNodes.length === 0) {
      throw createProxyError(
        'tooBusy',
        `All upstream "${kind}" channel slots are at capacity`,
      );
    }

    const now = Date.now();
    const sorted = [...availableNodes].sort((a, b) => {
      if (a[activeKey] !== b[activeKey]) {
        return a[activeKey] - b[activeKey];
      }
      return b.bucket.getAvailableTokens(now) - a.bucket.getAvailableTokens(now);
    });

    for (const node of sorted) {
      if (node.bucket.tryConsume(weight, now)) {
        node[activeKey] += 1;
        let released = false;
        const releaseSlot = () => {
          if (released) return;
          released = true;
          node[activeKey] = Math.max(0, node[activeKey] - 1);
        };
        return { node, releaseSlot };
      }
    }

    return null;
  }

  async acquireStatefulNode(kind, weight) {
    const immediate = this.tryReserveStatefulNode(kind, weight);
    if (immediate) {
      return immediate;
    }
    return this.rateQueue.enqueue(() => this.tryReserveStatefulNode(kind, weight));
  }

  consumeTokensOnNode(node, weight) {
    if (node.bucket.tryConsume(weight)) {
      return Promise.resolve(true);
    }
    return this.rateQueue.enqueue(() => {
      if (node.circuitState === 'OPEN') {
        throw createProxyError(
          'upstreamUnavailable',
          `Upstream node "${node.id}" circuit opened`,
        );
      }
      return node.bucket.tryConsume(weight) ? true : null;
    });
  }

  async probeNodeLedger(node) {
    if (!node || !node.isHealthyForTraffic() || !node.hasAvailablePoolConnection()) {
      return null;
    }
    const conn = node.pickPoolConnection();
    if (!conn) return null;

    const proxyId = this.nextProxyId++;
    const upstreamPayload = { id: proxyId, command: 'server_info' };

    return new Promise((resolve) => {
      const timeoutMs = Math.min(this.limits.requestTimeoutMs ?? 5000, 5000);
      const timer = setTimeout(() => {
        const entry = this.pendingRequests.get(proxyId);
        if (!entry) return;
        this.pendingRequests.delete(proxyId);
        resolve(null);
      }, timeoutMs);

      if (typeof timer.unref === 'function') {
        timer.unref();
      }

      this.pendingRequests.set(proxyId, {
        originalId: proxyId,
        hasOriginalId: true,
        timer,
        nodeId: node.id,
        node,
        conn,
        resolve: () => resolve(node.lastValidatedSeq),
        reject: () => resolve(null),
      });

      const sent = conn.sendRaw(JSON.stringify(upstreamPayload));
      if (!sent) {
        clearTimeout(timer);
        this.pendingRequests.delete(proxyId);
        resolve(null);
      }
    });
  }

  async probeAllLedgers() {
    const results = await Promise.all(
      this.nodes.map(async (node) => ({
        id: node.id,
        lastValidatedSeq: await this.probeNodeLedger(node),
      })),
    );
    return results;
  }

  getStats() {
    const now = Date.now();
    const nodes = this.nodes.map((n) => n.getStats(now));
    const healthyNodes = nodes.filter(
      (n) => n.circuitState === 'CLOSED' && n.isOnline,
    ).length;
    const totalQps = Number(
      nodes.reduce((sum, n) => sum + n.qps, 0).toFixed(2),
    );
    const {
      maxValidatedSeq,
      minValidatedSeq,
      safeValidatedSeq,
      sampledLedgerNodes,
    } = this.computeLedgerWatermarks(now);

    return {
      totalNodes: nodes.length,
      healthyNodes,
      totalQps,
      pendingRequests: this.pendingRequests.size,
      nextProxyId: this.nextProxyId,
      rateQueueLength: this.rateQueue.length,
      maxValidatedSeq,
      minValidatedSeq,
      safeValidatedSeq,
      safeBroadcastSeq: this.lastBroadcastSeq > 0 ? this.lastBroadcastSeq : safeValidatedSeq,
      sampledLedgerNodes,
      snapshotCacheSize: this.ledgerSnapshots.size,
      nodes,
    };
  }

  destroy() {
    this.rateQueue.clear();
    this.onSafeLedgerAdvanced = null;
    if (this.streamSettleTimer) {
      clearTimeout(this.streamSettleTimer);
      this.streamSettleTimer = null;
    }
    if (this.pendingLedgerSubWaiters.length > 0) {
      const waiters = this.pendingLedgerSubWaiters.splice(0, this.pendingLedgerSubWaiters.length);
      for (const w of waiters) {
        clearTimeout(w.timer);
        w.resolve(null);
      }
    }
    this.ledgerSnapshots.clear();
    for (const [proxyId, entry] of this.pendingRequests.entries()) {
      clearTimeout(entry.timer);
      this.pendingRequests.delete(proxyId);
      entry.reject(createProxyError('upstreamUnavailable', 'Proxy is shutting down'));
    }
    for (const node of this.nodes) {
      node.destroy();
    }
  }
}
