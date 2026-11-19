import assert from 'node:assert/strict';
import test from 'node:test';
import WebSocket from 'ws';
import { createConfig } from '../src/config.js';
import { MockXrpldNode } from '../src/mockXrpld.js';
import { ProxyServer } from '../src/proxyServer.js';

function connectClient(url) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.once('open', () => resolve(ws));
    ws.once('error', reject);
  });
}

function sendAndWait(ws, payload, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.off('message', onMsg);
      reject(new Error(`Timed out waiting for response to ${JSON.stringify(payload)}`));
    }, timeoutMs);

    const onMsg = (raw) => {
      const data = JSON.parse(raw.toString('utf8'));
      if (data.type === 'response') {
        clearTimeout(timer);
        ws.off('message', onMsg);
        resolve(data);
      }
    };

    ws.on('message', onMsg);
    ws.send(JSON.stringify(payload));
  });
}

async function setupCluster(nodeSpecs, overrides = {}) {
  const mocks = {};
  const nodes = [];
  for (const spec of nodeSpecs) {
    const mock = new MockXrpldNode({ id: spec.id, roles: spec.roles });
    const wsUrl = await mock.start(0);
    mocks[spec.id] = mock;
    nodes.push({
      poolSize: 1,
      maxPathConns: 5,
      maxSubscribeConns: 5,
      rateLimit: { ratePerSec: 50, burst: 50 },
      ...spec,
      wsUrl,
    });
  }

  const cfg = createConfig({
    server: { host: '127.0.0.1', port: 0, heartbeatIntervalMs: 0, statsLogIntervalMs: 0 },
    ...overrides,
    nodes,
  });

  const proxy = new ProxyServer(cfg);
  const addr = await proxy.start();
  const proxyUrl = `ws://127.0.0.1:${addr.port}/`;

  return {
    mocks,
    proxy,
    proxyUrl,
    async teardown() {
      await proxy.shutdown();
      await Promise.all(Object.values(mocks).map((m) => m.stop()));
    },
  };
}

test('10. Failover & circuit breaker: stopping a node routes traffic to healthy peer and trips circuit to OPEN', async () => {
  const env = await setupCluster(
    [
      { id: 'n-primary', roles: ['normal'], poolSize: 2, rateLimit: { ratePerSec: 20, burst: 40 } },
      { id: 'n-backup', roles: ['normal'], poolSize: 1, rateLimit: { ratePerSec: 20, burst: 10 } },
    ],
    {
      limits: {
        circuitBreakerThreshold: 3,
        nodeCooldownMs: 5000,
        reconnectBaseDelayMs: 80,
        reconnectMaxDelayMs: 200,
      },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);

    // Stop n-primary -> 2 pool sockets close (2 failures) + 1 reconnect attempt fails (3rd failure) => OPEN
    await env.mocks['n-primary'].stop();

    await new Promise((r) => setTimeout(r, 250));

    const stats = env.proxy.getStats();
    const primaryNode = stats.nodes.find((n) => n.id === 'n-primary');
    assert.equal(primaryNode.circuitState, 'OPEN');

    const resp = await sendAndWait(client, { id: 'failover-1', command: 'account_info' });
    assert.equal(resp.status, 'success');
    assert.equal(resp._mockNode, 'n-backup');

    client.close();
  } finally {
    await env.teardown();
  }
});

test('11. ReadCache Singleflight coalesces concurrent fee requests and restores each caller ID', async () => {
  const env = await setupCluster([{ id: 'n-cache', roles: ['normal'], poolSize: 1 }]);

  try {
    const [c1, c2, c3] = await Promise.all([
      connectClient(env.proxyUrl),
      connectClient(env.proxyUrl),
      connectClient(env.proxyUrl),
    ]);

    const [r1, r2, r3] = await Promise.all([
      sendAndWait(c1, { id: 0, command: 'fee' }),
      sendAndWait(c2, { id: 'fee-str', command: 'fee' }),
      sendAndWait(c3, { command: 'fee' }),
    ]);

    assert.equal(r1.id, 0);
    assert.equal(r2.id, 'fee-str');
    assert.equal('id' in r3, false);

    // Only 1 upstream fee request was sent due to Singleflight coalescing
    const feeCalls = env.mocks['n-cache'].receivedCommands.filter((c) => c.command === 'fee');
    assert.equal(feeCalls.length, 1);

    const cacheStats = env.proxy.getStats().cache;
    assert.equal(cacheStats.coalesced, 2);

    // Subsequent call within 1500ms hits cache
    const r4 = await sendAndWait(c1, { id: 99, command: 'fee' });
    assert.equal(r4.id, 99);
    assert.equal(env.proxy.getStats().cache.hits, 1);

    c1.close();
    c2.close();
    c3.close();
  } finally {
    await env.teardown();
  }
});

test('12. Adaptive backpressure halves token refill rate when upstream returns warning: "load"', async () => {
  const env = await setupCluster(
    [{ id: 'n-load', roles: ['normal'], rateLimit: { ratePerSec: 20, burst: 40 } }],
    {
      limits: {
        slowDownRecoveryMs: 2000,
      },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);
    env.mocks['n-load'].armLoadWarning();

    const resp = await sendAndWait(client, { id: 'lw-1', command: 'account_info' });
    assert.equal(resp.warning, 'load');

    const nodeStats = env.proxy.getStats().nodes[0];
    assert.equal(nodeStats.rateMultiplier, 0.5);
    assert.equal(nodeStats.effectiveRatePerSec, 10);
    assert.equal(nodeStats.isSlowedDown, true);

    client.close();
  } finally {
    await env.teardown();
  }
});

test('13. Unexpected disconnect of active subscribe upstream sends WS close code 1011 to client', async () => {
  const env = await setupCluster([
    { id: 'n-sub', roles: ['normal'], poolSize: 1, maxSubscribeConns: 5 },
  ]);

  try {
    const client = await connectClient(env.proxyUrl);

    const subResp = await sendAndWait(client, {
      id: 'sub-1',
      command: 'subscribe',
      streams: ['ledger'],
    });
    assert.equal(subResp.status, 'success');

    const closeCodePromise = new Promise((resolve) => {
      client.once('close', (code) => resolve(code));
    });

    // Abruptly terminate upstream node while subscription is active
    await env.mocks['n-sub'].stop();

    const closeCode = await closeCodePromise;
    assert.equal(closeCode, 1011);
  } finally {
    await env.teardown();
  }
});

test('14. Request timeout triggers upstreamTimeout and graceful shutdown sends 1001 to clients', async () => {
  // Custom slow mock server that never responds to simulate timeout
  const http = await import('node:http');
  const { WebSocketServer } = await import('ws');

  const slowHttp = http.createServer();
  const slowWss = new WebSocketServer({ server: slowHttp });
  slowWss.on('connection', () => {
    // Intentionally ignore messages to trigger upstreamTimeout
  });
  await new Promise((r) => slowHttp.listen(0, '127.0.0.1', r));
  const slowPort = slowHttp.address().port;

  const cfg = createConfig({
    server: { host: '127.0.0.1', port: 0, heartbeatIntervalMs: 0, statsLogIntervalMs: 0 },
    limits: {
      requestTimeoutMs: 120,
      circuitBreakerThreshold: 5,
    },
    nodes: [
      {
        id: 'n-slow',
        wsUrl: `ws://127.0.0.1:${slowPort}/`,
        roles: ['normal'],
        poolSize: 1,
        maxPathConns: 0,
        maxSubscribeConns: 0,
        rateLimit: { ratePerSec: 20, burst: 20 },
      },
    ],
  });

  const proxy = new ProxyServer(cfg);
  const addr = await proxy.start();
  const proxyUrl = `ws://127.0.0.1:${addr.port}/`;

  try {
    const client = await connectClient(proxyUrl);
    const timeoutResp = await sendAndWait(client, { id: 't1', command: 'account_info' });
    assert.equal(timeoutResp.status, 'error');
    assert.equal(timeoutResp.error, 'upstreamTimeout');
    assert.equal(timeoutResp.id, 't1');

    const closePromise = new Promise((resolve) => {
      client.once('close', (code) => resolve(code));
    });

    await proxy.shutdown();
    const closeCode = await closePromise;
    assert.equal(closeCode, 1001);
  } finally {
    await proxy.shutdown();
    slowWss.clients.forEach((ws) => ws.terminate());
    await new Promise((r) => slowWss.close(() => r()));
    await new Promise((r) => slowHttp.close(() => r()));
  }
});
