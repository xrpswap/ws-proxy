import http from 'node:http';
import { WebSocketServer } from 'ws';
import { ReadCache } from './cache.js';
import { createConfig } from './config.js';
import { buildErrorResponse, extractRequestId, sendWsJson } from './errors.js';
import { NodeManager } from './nodeManager.js';
import { classifyRequest } from './router.js';
import { ClientStatefulManager } from './stateful.js';

function isPureLedgerCommand(payload, expectedCommand) {
  if (!payload || typeof payload !== 'object' || payload.command !== expectedCommand) {
    return false;
  }
  if (
    !Array.isArray(payload.streams) ||
    payload.streams.length !== 1 ||
    payload.streams[0] !== 'ledger'
  ) {
    return false;
  }
  const extraKeys = [
    'accounts',
    'accounts_proposed',
    'books',
    'url',
    'url_username',
    'url_password',
  ];
  for (const key of extraKeys) {
    if (payload[key] !== undefined && payload[key] !== null) {
      if (!Array.isArray(payload[key]) || payload[key].length > 0) {
        return false;
      }
    }
  }
  return true;
}

export class ProxyServer {
  constructor(customConfig = {}) {
    this.config = customConfig.nodes && customConfig.server && customConfig.limits
      ? customConfig
      : createConfig(customConfig);

    this.nodeManager = new NodeManager(this.config);
    this.nodeManager.onSafeLedgerAdvanced = (snapshot) => {
      this.broadcastSharedLedgerClosed(snapshot);
    };
    this.cache = new ReadCache(this.config.cache);

    this.wss = new WebSocketServer({
      noServer: true,
      maxPayload: this.config.server.maxPayloadBytes,
    });

    this.httpServer = null;
    this.ownsHttpServer = false;
    this.upgradeHandler = null;

    this.sessions = new Map(); // clientWs -> session
    this.ipConnectionCounts = new Map();

    this.heartbeatTimer = null;
    this.statsLogTimer = null;

    this.started = false;
    this.isShuttingDown = false;
  }

  getClientIp(req) {
    if (this.config.server.trustProxy) {
      const xff = req.headers['x-forwarded-for'];
      if (typeof xff === 'string' && xff.trim().length > 0) {
        return xff.split(',')[0].trim();
      }
    }
    return req.socket?.remoteAddress || 'unknown';
  }

  attachToServer(httpServer, { wsPath = null } = {}) {
    if (this.httpServer === httpServer && this.upgradeHandler) {
      return;
    }
    this.httpServer = httpServer;

    this.upgradeHandler = (req, socket, head) => {
      if (this.isShuttingDown) {
        socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
        socket.destroy();
        return;
      }

      if (wsPath) {
        const reqPath = (req.url || '/').split('?')[0];
        if (reqPath !== wsPath && reqPath !== `${wsPath}/`) {
          return;
        }
      }

      const clientIp = this.getClientIp(req);
      const currentCount = this.ipConnectionCounts.get(clientIp) || 0;
      if (currentCount >= this.config.server.maxConnectionsPerIp) {
        socket.write('HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\n');
        socket.destroy();
        return;
      }

      this.wss.handleUpgrade(req, socket, head, (ws) => {
        this.handleClientConnection(ws, clientIp);
      });
    };

    httpServer.on('upgrade', this.upgradeHandler);
  }

  /**
   * @param {{ httpServer?: http.Server | null; wsPath?: string | null }} [options]
   */
  async start(options = {}) {
    const { httpServer = null, wsPath = null } = options;
    if (this.started) {
      return this.getAddress();
    }
    this.started = true;
    this.isShuttingDown = false;

    this.nodeManager.start();
    this.startHeartbeat();
    this.startStatsLogger();

    if (httpServer) {
      this.ownsHttpServer = false;
      this.attachToServer(httpServer, { wsPath });
      return this.getAddress();
    }

    this.ownsHttpServer = true;
    const srv = http.createServer((_req, res) => {
      res.writeHead(426, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'WebSocket upgrade required' }));
    });
    this.attachToServer(srv, { wsPath });

    await new Promise((resolve, reject) => {
      const onError = (err) => {
        srv.off('listening', onListening);
        reject(err);
      };
      const onListening = () => {
        srv.off('error', onError);
        resolve();
      };
      srv.once('error', onError);
      srv.once('listening', onListening);
      srv.listen(this.config.server.port, this.config.server.host);
    });

    return this.getAddress();
  }

  getAddress() {
    if (!this.httpServer) return null;
    return this.httpServer.address();
  }

  handleClientConnection(ws, clientIp) {
    if (this.isShuttingDown) {
      ws.close(1001, 'Server shutting down');
      return;
    }

    const prevCount = this.ipConnectionCounts.get(clientIp) || 0;
    this.ipConnectionCounts.set(clientIp, prevCount + 1);

    const session = {
      ws,
      ip: clientIp,
      isAlive: true,
      sharedLedgerSubscribed: false,
      stateful: new ClientStatefulManager(ws, this.nodeManager, this.config.limits),
      cleaned: false,
      handlers: null,
    };

    const onPong = () => {
      session.isAlive = true;
    };

    const onMessage = (rawData) => {
      session.isAlive = true;
      this.handleClientMessage(session, rawData);
    };

    const onClose = () => {
      this.cleanupSession(session);
    };

    const onError = () => {
      this.cleanupSession(session);
    };

    session.handlers = { onPong, onMessage, onClose, onError };
    this.sessions.set(ws, session);

    ws.on('pong', onPong);
    ws.on('message', onMessage);
    ws.on('close', onClose);
    ws.on('error', onError);
  }

  cleanupSession(session) {
    if (!session || session.cleaned) return;
    session.cleaned = true;
    session.sharedLedgerSubscribed = false;

    const { ws, ip, handlers, stateful } = session;
    this.sessions.delete(ws);

    if (handlers) {
      ws.off('pong', handlers.onPong);
      ws.off('message', handlers.onMessage);
      ws.off('close', handlers.onClose);
      ws.off('error', handlers.onError);
    }

    const remaining = (this.ipConnectionCounts.get(ip) || 1) - 1;
    if (remaining <= 0) {
      this.ipConnectionCounts.delete(ip);
    } else {
      this.ipConnectionCounts.set(ip, remaining);
    }

    stateful.destroy();
  }

  async handleClientMessage(session, rawData) {
    if (this.isShuttingDown) return;

    let payload;
    try {
      const text = typeof rawData === 'string' ? rawData : rawData.toString('utf8');
      payload = JSON.parse(text);
    } catch {
      sendWsJson(
        session.ws,
        buildErrorResponse(undefined, false, 'jsonInvalid', 'Invalid JSON payload'),
      );
      return;
    }

    const { hasOriginalId, originalId } = extractRequestId(payload);

    if (
      !payload ||
      typeof payload !== 'object' ||
      Array.isArray(payload) ||
      typeof payload.command !== 'string' ||
      payload.command.trim().length === 0
    ) {
      sendWsJson(
        session.ws,
        buildErrorResponse(
          originalId,
          hasOriginalId,
          'jsonInvalid',
          'Missing or invalid "command" field',
        ),
      );
      return;
    }

    const routeInfo = classifyRequest(payload, this.config.routing, this.config.cache);

    if (this.config.ledgerStream?.shareWithClients) {
      if (isPureLedgerCommand(payload, 'subscribe')) {
        const snapshot = await this.nodeManager.getInitialLedgerSnapshot();
        if (session.cleaned || this.isShuttingDown) return;
        if (snapshot) {
          session.sharedLedgerSubscribed = true;
          const resp = {
            status: 'success',
            type: 'response',
            result: { ...snapshot },
          };
          if (hasOriginalId) resp.id = originalId;
          sendWsJson(session.ws, resp);
          return;
        }
      } else if (isPureLedgerCommand(payload, 'unsubscribe')) {
        const wasShared = session.sharedLedgerSubscribed;
        session.sharedLedgerSubscribed = false;
        if (wasShared || !session.stateful.subscribeChannel) {
          const resp = {
            status: 'success',
            type: 'response',
            result: {},
          };
          if (hasOriginalId) resp.id = originalId;
          sendWsJson(session.ws, resp);
          return;
        }
      }
    }

    if (routeInfo.isStateful) {
      await session.stateful.handleMessage(payload, routeInfo);
      return;
    }

    try {
      let response;
      if (routeInfo.isCacheable) {
        response = await this.cache.getOrFetch(payload, () =>
          this.nodeManager.sendStatelessRequest(payload, routeInfo),
        );
      } else {
        response = await this.nodeManager.sendStatelessRequest(payload, routeInfo);
      }
      sendWsJson(session.ws, response);
    } catch (err) {
      sendWsJson(
        session.ws,
        buildErrorResponse(
          originalId,
          hasOriginalId,
          err.code || 'upstreamUnavailable',
          err.message || 'Upstream request failed',
        ),
      );
    }
  }

  startHeartbeat() {
    if (this.heartbeatTimer) return;
    const intervalMs = this.config.server.heartbeatIntervalMs;
    if (!intervalMs || intervalMs <= 0) return;

    this.heartbeatTimer = setInterval(() => {
      for (const session of this.sessions.values()) {
        if (!session.isAlive) {
          try {
            session.ws.terminate();
          } catch {
            // ignore
          }
          this.cleanupSession(session);
          continue;
        }
        session.isAlive = false;
        try {
          session.ws.ping();
        } catch {
          // ignore
        }
      }
    }, intervalMs);

    if (typeof this.heartbeatTimer.unref === 'function') {
      this.heartbeatTimer.unref();
    }
  }

  startStatsLogger() {
    if (this.statsLogTimer) return;
    const intervalMs = this.config.server.statsLogIntervalMs;
    if (!intervalMs || intervalMs <= 0) return;

    this.statsLogTimer = setInterval(() => {
      const s = this.getStats();
      console.log(
        `[proxy] clients=${s.clientConnections} ips=${s.uniqueIps} qps=${s.totalQps} healthy=${s.healthyNodes}/${s.totalNodes} pending=${s.pendingRequests}`,
      );
    }, intervalMs);

    if (typeof this.statsLogTimer.unref === 'function') {
      this.statsLogTimer.unref();
    }
  }

  broadcastSharedLedgerClosed(snapshot) {
    if (this.isShuttingDown || !snapshot || !this.config.ledgerStream?.shareWithClients) {
      return;
    }
    const maxBuffer = this.config.ledgerStream?.maxClientBufferBytes ?? 256 * 1024;
    const eventPayload = {
      type: 'ledgerClosed',
      ledger_index: snapshot.ledger_index,
      ...(snapshot.ledger_hash ? { ledger_hash: snapshot.ledger_hash } : {}),
      ...(snapshot.ledger_time !== undefined ? { ledger_time: snapshot.ledger_time } : {}),
      ...(snapshot.fee_base !== undefined ? { fee_base: snapshot.fee_base } : {}),
      ...(snapshot.fee_ref !== undefined ? { fee_ref: snapshot.fee_ref } : {}),
      ...(snapshot.reserve_base !== undefined ? { reserve_base: snapshot.reserve_base } : {}),
      ...(snapshot.reserve_inc !== undefined ? { reserve_inc: snapshot.reserve_inc } : {}),
      ...(snapshot.txn_count !== undefined ? { txn_count: snapshot.txn_count } : {}),
      ...(snapshot.validated_ledgers ? { validated_ledgers: snapshot.validated_ledgers } : {}),
    };
    const serialized = JSON.stringify(eventPayload);

    for (const session of this.sessions.values()) {
      if (session.cleaned || !session.sharedLedgerSubscribed) continue;
      const ws = session.ws;
      if (ws.readyState !== ws.OPEN) continue;
      if (maxBuffer > 0 && ws.bufferedAmount >= maxBuffer) {
        continue;
      }
      try {
        ws.send(serialized);
      } catch {
        // ignore send errors on closing socket
      }
    }
  }

  getStats() {
    const nodeStats = this.nodeManager.getStats();
    const cacheStats = this.cache.getStats();
    let sharedLedgerSubscribers = 0;
    for (const session of this.sessions.values()) {
      if (!session.cleaned && session.sharedLedgerSubscribed) {
        sharedLedgerSubscribers += 1;
      }
    }

    return {
      clientConnections: this.sessions.size,
      uniqueIps: this.ipConnectionCounts.size,
      sharedLedgerSubscribers,
      totalNodes: nodeStats.totalNodes,
      healthyNodes: nodeStats.healthyNodes,
      totalQps: nodeStats.totalQps,
      pendingRequests: nodeStats.pendingRequests,
      nextProxyId: nodeStats.nextProxyId,
      rateQueueLength: nodeStats.rateQueueLength,
      maxValidatedSeq: nodeStats.maxValidatedSeq,
      minValidatedSeq: nodeStats.minValidatedSeq,
      safeValidatedSeq: nodeStats.safeValidatedSeq,
      safeBroadcastSeq: nodeStats.safeBroadcastSeq,
      sampledLedgerNodes: nodeStats.sampledLedgerNodes,
      snapshotCacheSize: nodeStats.snapshotCacheSize,
      cache: cacheStats,
      ledgerStream: this.config.ledgerStream,
      nodes: nodeStats.nodes,
      routing: {
        pathCommands: this.config.routing.pathCommands,
        historyCommands: this.config.routing.historyCommands,
        submitCommands: this.config.routing.submitCommands,
        statefulCommands: this.config.routing.statefulCommands,
        routeExplicitLedgerToHistory: this.config.routing.routeExplicitLedgerToHistory,
        preferSafeLagForLedgerClosed: this.config.routing.preferSafeLagForLedgerClosed,
        defaultWeight: this.config.routing.defaultWeight,
        commandWeights: this.config.routing.commandWeights,
      },
      limits: this.config.limits,
      server: {
        host: this.config.server.host,
        port: this.config.server.port,
        maxPayloadBytes: this.config.server.maxPayloadBytes,
        maxConnectionsPerIp: this.config.server.maxConnectionsPerIp,
        trustProxy: this.config.server.trustProxy,
      },
    };
  }

  async shutdown() {
    if (this.isShuttingDown) return;
    this.isShuttingDown = true;

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.statsLogTimer) {
      clearInterval(this.statsLogTimer);
      this.statsLogTimer = null;
    }

    if (this.httpServer && this.upgradeHandler) {
      this.httpServer.off('upgrade', this.upgradeHandler);
      this.upgradeHandler = null;
    }

    for (const session of Array.from(this.sessions.values())) {
      const ws = session.ws;
      this.cleanupSession(session);
      try {
        if (ws.readyState === ws.OPEN || ws.readyState === ws.CONNECTING) {
          ws.close(1001, 'Server shutting down');
        }
      } catch {
        // ignore
      }
    }

    this.cache.clear();
    this.nodeManager.destroy();
    this.wss.removeAllListeners();

    if (this.ownsHttpServer && this.httpServer) {
      const srv = this.httpServer;
      await new Promise((resolve) => setTimeout(resolve, 25));
      srv.closeAllConnections?.();
      await new Promise((resolve) => {
        srv.close(() => resolve());
      });
    }

    this.started = false;
  }
}
