import WebSocket from 'ws';
import {
  applyResponseId,
  buildErrorResponse,
  extractRequestId,
  sendWsJson,
} from './errors.js';

class StatefulChannel {
  constructor({ kind, node, releaseSlot, clientWs, limits, onClosed }) {
    this.kind = kind; // 'path' | 'subscribe'
    this.node = node;
    this.releaseSlot = releaseSlot;
    this.clientWs = clientWs;
    this.limits = limits;
    this.onClosed = onClosed;

    this.ws = null;
    this.hasOpened = false;
    this.intentionalClose = false;
    this.closed = false;
    this.outboundQueue = [];
    this.ttlTimer = null;
    this.closeResponseTimer = null;
    this.closingOnResponse = false;
    this.closeRequestInternalId = null;
    this.nextChannelId = 1;
    this.pendingIds = new Map();
  }

  connect() {
    let ws;
    try {
      ws = new WebSocket(this.node.wsUrl, {
        handshakeTimeout: Math.min(this.limits.requestTimeoutMs, 10000),
      });
    } catch {
      this.failBeforeOpen();
      return;
    }

    this.ws = ws;

    if (this.kind === 'path' && this.limits.maxPathTtlMs > 0) {
      this.ttlTimer = setTimeout(() => {
        this.ttlTimer = null;
        if (this.closed) return;
        this.closeGracefullyWithPathClose();
      }, this.limits.maxPathTtlMs);
      if (typeof this.ttlTimer.unref === 'function') {
        this.ttlTimer.unref();
      }
    }

    const onOpen = () => {
      if (this.closed || this.ws !== ws) return;
      this.hasOpened = true;
      this.node.recordSuccess();
      this.flushOutboundQueue();
    };

    const onMessage = (rawData) => {
      if (this.closed || this.ws !== ws) return;
      this.handleUpstreamMessage(rawData);
    };

    const onError = () => {
      // handled in onClose
    };

    const onClose = () => {
      ws.removeAllListeners();
      if (this.ws === ws) {
        this.ws = null;
      }

      const wasOpened = this.hasOpened;
      const wasIntentional = this.intentionalClose;

      this.cleanupTimers();

      if (!wasOpened && !wasIntentional) {
        this.failOutboundWithUnavailable();
        this.node.recordFailure('statefulHandshakeFailed');
      } else if (wasOpened && !wasIntentional) {
        this.node.recordFailure('statefulUnexpectedDisconnect');
        if (this.clientWs && this.clientWs.readyState === WebSocket.OPEN) {
          try {
            this.clientWs.close(1011, 'Upstream stateful connection closed unexpectedly');
          } catch {
            // ignore
          }
        }
      }

      this.finalize();
    };

    ws.on('open', onOpen);
    ws.on('message', onMessage);
    ws.on('error', onError);
    ws.on('close', onClose);
  }

  failBeforeOpen() {
    this.cleanupTimers();
    this.failOutboundWithUnavailable();
    this.node.recordFailure('statefulConnectError');
    this.finalize();
  }

  failOutboundWithUnavailable() {
    while (this.outboundQueue.length > 0) {
      const item = this.outboundQueue.shift();
      const errResp = buildErrorResponse(
        item.originalId,
        item.hasOriginalId,
        'upstreamUnavailable',
        `Upstream stateful channel "${this.node.id}" unavailable`,
      );
      sendWsJson(this.clientWs, errResp);
    }
  }

  sendPayload(payload, { isPathClose = false } = {}) {
    if (this.closed) return;

    const { hasOriginalId, originalId } = extractRequestId(payload);
    const internalId = this.nextChannelId++;
    this.pendingIds.set(internalId, { hasOriginalId, originalId, isPathClose });

    const upstreamMsg = { ...payload, id: internalId };
    if (isPathClose) {
      this.closingOnResponse = true;
      this.closeRequestInternalId = internalId;
      if (this.closeResponseTimer) {
        clearTimeout(this.closeResponseTimer);
      }
      this.closeResponseTimer = setTimeout(() => {
        this.closeResponseTimer = null;
        this.close();
      }, Math.min(this.limits.requestTimeoutMs, 3000));
      if (typeof this.closeResponseTimer.unref === 'function') {
        this.closeResponseTimer.unref();
      }
    }

    const serialized = JSON.stringify(upstreamMsg);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(serialized);
      } catch {
        // close handler will deal with disconnect
      }
    } else {
      this.outboundQueue.push({ serialized, originalId, hasOriginalId });
    }
  }

  flushOutboundQueue() {
    while (
      this.outboundQueue.length > 0 &&
      this.ws &&
      this.ws.readyState === WebSocket.OPEN
    ) {
      const item = this.outboundQueue.shift();
      try {
        this.ws.send(item.serialized);
      } catch {
        break;
      }
    }
  }

  handleUpstreamMessage(rawData) {
    let msg;
    try {
      const text = typeof rawData === 'string' ? rawData : rawData.toString('utf8');
      msg = JSON.parse(text);
    } catch {
      return;
    }

    if (!msg || typeof msg !== 'object') return;

    const isLoadWarning = msg.warning === 'load';
    const isLoadError =
      msg.status === 'error' && ['tooBusy', 'slowDown', 'highLoad'].includes(msg.error);

    if (isLoadWarning || isLoadError) {
      this.node.triggerSlowDown();
    }
    if (isLoadError) {
      this.node.recordFailure(`stateful:${msg.error}`);
    } else if (msg.type === 'response') {
      this.node.recordSuccess();
    }

    const internalId = msg.id;
    let outMsg = msg;
    let shouldCloseAfterSend = false;

    if (internalId !== undefined && internalId !== null && this.pendingIds.has(internalId)) {
      const meta = this.pendingIds.get(internalId);
      this.pendingIds.delete(internalId);
      outMsg = applyResponseId(msg, meta.originalId, meta.hasOriginalId);
      if (this.closingOnResponse && internalId === this.closeRequestInternalId) {
        shouldCloseAfterSend = true;
      }
    }

    sendWsJson(this.clientWs, outMsg);

    if (shouldCloseAfterSend) {
      this.close();
    }
  }

  closeGracefullyWithPathClose() {
    if (this.closed) return;
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify({ command: 'path_find', subcommand: 'close' }));
      } catch {
        // ignore
      }
    }
    this.close();
  }

  cleanupTimers() {
    if (this.ttlTimer) {
      clearTimeout(this.ttlTimer);
      this.ttlTimer = null;
    }
    if (this.closeResponseTimer) {
      clearTimeout(this.closeResponseTimer);
      this.closeResponseTimer = null;
    }
  }

  finalize() {
    if (this.closed) return;
    this.closed = true;
    this.cleanupTimers();
    this.pendingIds.clear();
    this.outboundQueue = [];
    this.releaseSlot();
    if (typeof this.onClosed === 'function') {
      this.onClosed(this);
    }
  }

  close() {
    if (this.closed) return;
    this.intentionalClose = true;
    this.cleanupTimers();

    const socket = this.ws;
    if (socket) {
      this.ws = null;
      socket.removeAllListeners();
      try {
        if (
          socket.readyState === WebSocket.OPEN ||
          socket.readyState === WebSocket.CONNECTING
        ) {
          socket.close(1000, 'Stateful channel closed');
        }
      } catch {
        try {
          socket.terminate();
        } catch {
          // ignore
        }
      }
    }

    this.finalize();
  }
}

export class ClientStatefulManager {
  constructor(clientWs, nodeManager, limits) {
    this.clientWs = clientWs;
    this.nodeManager = nodeManager;
    this.limits = limits;
    this.pathChannel = null;
    this.subscribeChannel = null;
    this.pathCreatingPromise = null;
    this.subscribeCreatingPromise = null;
    this.destroyed = false;
  }

  async handleMessage(payload, routeInfo) {
    if (this.destroyed) return;
    const { hasOriginalId, originalId } = extractRequestId(payload);

    if (routeInfo.statefulKind === 'path') {
      const isCloseSubcommand = payload.subcommand === 'close';

      if (isCloseSubcommand) {
        if (this.pathCreatingPromise) {
          try {
            await this.pathCreatingPromise;
          } catch {
            // ignore
          }
        }
        if (this.pathChannel && !this.pathChannel.closed) {
          try {
            await this.nodeManager.consumeTokensOnNode(
              this.pathChannel.node,
              routeInfo.weight,
            );
          } catch (err) {
            sendWsJson(
              this.clientWs,
              buildErrorResponse(
                originalId,
                hasOriginalId,
                err.code || 'slowDown',
                err.message || 'Rate limited',
              ),
            );
            return;
          }
          this.pathChannel.node.recordRequestStart();
          this.pathChannel.sendPayload(payload, { isPathClose: true });
          return;
        }

        // No active path_find session
        const resp = {
          status: 'success',
          type: 'response',
          result: {},
        };
        if (hasOriginalId) resp.id = originalId;
        sendWsJson(this.clientWs, resp);
        return;
      }

      // path_find create / status
      let channel = this.pathChannel;
      if (!channel || channel.closed) {
        if (!this.pathCreatingPromise) {
          this.pathCreatingPromise = this.createChannel('path', routeInfo.weight);
        }
        try {
          channel = await this.pathCreatingPromise;
        } catch (err) {
          sendWsJson(
            this.clientWs,
            buildErrorResponse(
              originalId,
              hasOriginalId,
              err.code || 'upstreamUnavailable',
              err.message || 'Failed to establish path_find channel',
            ),
          );
          return;
        } finally {
          this.pathCreatingPromise = null;
        }
      } else {
        try {
          await this.nodeManager.consumeTokensOnNode(channel.node, routeInfo.weight);
        } catch (err) {
          sendWsJson(
            this.clientWs,
            buildErrorResponse(
              originalId,
              hasOriginalId,
              err.code || 'slowDown',
              err.message || 'Rate limited',
            ),
          );
          return;
        }
      }

      if (this.destroyed || channel.closed) return;
      channel.node.recordRequestStart();
      channel.sendPayload(payload);
      return;
    }

    // subscribe / unsubscribe
    let channel = this.subscribeChannel;
    if (!channel || channel.closed) {
      if (!this.subscribeCreatingPromise) {
        this.subscribeCreatingPromise = this.createChannel(
          'subscribe',
          routeInfo.weight,
        );
      }
      try {
        channel = await this.subscribeCreatingPromise;
      } catch (err) {
        sendWsJson(
          this.clientWs,
          buildErrorResponse(
            originalId,
            hasOriginalId,
            err.code || 'upstreamUnavailable',
            err.message || 'Failed to establish subscribe channel',
          ),
        );
        return;
      } finally {
        this.subscribeCreatingPromise = null;
      }
    } else {
      try {
        await this.nodeManager.consumeTokensOnNode(channel.node, routeInfo.weight);
      } catch (err) {
        sendWsJson(
          this.clientWs,
          buildErrorResponse(
            originalId,
            hasOriginalId,
            err.code || 'slowDown',
            err.message || 'Rate limited',
          ),
        );
        return;
      }
    }

    if (this.destroyed || channel.closed) return;
    channel.node.recordRequestStart();
    channel.sendPayload(payload);
  }

  async createChannel(kind, weight) {
    const { node, releaseSlot } = await this.nodeManager.acquireStatefulNode(kind, weight);

    if (this.destroyed) {
      releaseSlot();
      throw Object.assign(new Error('Client disconnected'), {
        code: 'upstreamUnavailable',
      });
    }

    const ch = new StatefulChannel({
      kind,
      node,
      releaseSlot,
      clientWs: this.clientWs,
      limits: this.limits,
      onClosed: (closedCh) => {
        if (kind === 'path') {
          if (this.pathChannel === closedCh) {
            this.pathChannel = null;
          }
        } else if (this.subscribeChannel === closedCh) {
          this.subscribeChannel = null;
        }
      },
    });

    if (kind === 'path') {
      this.pathChannel = ch;
    } else {
      this.subscribeChannel = ch;
    }

    ch.connect();
    return ch;
  }

  destroy() {
    this.destroyed = true;
    if (this.pathChannel) {
      const ch = this.pathChannel;
      this.pathChannel = null;
      ch.close();
    }
    if (this.subscribeChannel) {
      const ch = this.subscribeChannel;
      this.subscribeChannel = null;
      ch.close();
    }
  }
}
