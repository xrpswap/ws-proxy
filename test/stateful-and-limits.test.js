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

test('5. path_find 1:1 isolation: closing client A channel releases slot without affecting client B', async () => {
  const env = await setupCluster([
    { id: 'n-path', roles: ['path'], poolSize: 1, maxPathConns: 2 },
  ]);

  try {
    const clientA = await connectClient(env.proxyUrl);
    const clientB = await connectClient(env.proxyUrl);

    const resA = await sendAndWait(clientA, {
      id: 'a-open',
      command: 'path_find',
      subcommand: 'create',
    });
    assert.equal(resA.status, 'success');

    const resB = await sendAndWait(clientB, {
      id: 'b-open',
      command: 'path_find',
      subcommand: 'create',
    });
    assert.equal(resB.status, 'success');

    // 1 pool ws + 2 stateful path ws = 3 active connections on mock node
    assert.equal(env.proxy.getStats().nodes[0].activePathConns, 2);

    const closeA = await sendAndWait(clientA, {
      id: 'a-close',
      command: 'path_find',
      subcommand: 'close',
    });
    assert.equal(closeA.status, 'success');
    assert.equal(closeA.result.closed, true);

    await new Promise((r) => setTimeout(r, 50));
    assert.equal(env.proxy.getStats().nodes[0].activePathConns, 1);

    // Client B channel still works!
    const statusB = await sendAndWait(clientB, {
      id: 'b-status',
      command: 'path_find',
      subcommand: 'status',
    });
    assert.equal(statusB.status, 'success');

    clientA.close();
    clientB.close();
  } finally {
    await env.teardown();
  }
});

test('6. Stateful slot quota fails fast with tooBusy when maxPathConns is reached', async () => {
  const env = await setupCluster([
    { id: 'n-path', roles: ['path'], poolSize: 1, maxPathConns: 1 },
  ]);

  try {
    const clientA = await connectClient(env.proxyUrl);
    const clientB = await connectClient(env.proxyUrl);

    const resA = await sendAndWait(clientA, {
      id: 1,
      command: 'path_find',
      subcommand: 'create',
    });
    assert.equal(resA.status, 'success');

    const connsBefore = env.mocks['n-path'].activeConnections.size;

    const resB = await sendAndWait(clientB, {
      id: 2,
      command: 'path_find',
      subcommand: 'create',
    });
    assert.equal(resB.status, 'error');
    assert.equal(resB.error, 'tooBusy');
    assert.equal(resB.id, 2);

    // Did not open an extra connection to backend
    assert.equal(env.mocks['n-path'].activeConnections.size, connsBefore);

    clientA.close();
    clientB.close();
  } finally {
    await env.teardown();
  }
});

test('7. Token bucket spillover routes to healthy node B when node A tokens are exhausted', async () => {
  const env = await setupCluster([
    {
      id: 'n-a',
      roles: ['normal'],
      poolSize: 1,
      rateLimit: { ratePerSec: 1, burst: 8 },
    },
    {
      id: 'n-b',
      roles: ['normal'],
      poolSize: 1,
      rateLimit: { ratePerSec: 1, burst: 20 },
    },
  ]);

  try {
    const client = await connectClient(env.proxyUrl);
    const handledBy = [];

    // book_offers has weight 4 -> n-b starts with 20 tokens (picks highest tokens first),
    // drops to 16, 12, 8 -> then spills back and forth across n-a and n-b!
    for (let i = 0; i < 6; i += 1) {
      const resp = await sendAndWait(client, { id: i, command: 'book_offers' });
      assert.equal(resp.status, 'success');
      handledBy.push(resp._mockNode);
    }

    assert.ok(handledBy.includes('n-a'), `Expected n-a in ${handledBy}`);
    assert.ok(handledBy.includes('n-b'), `Expected n-b in ${handledBy}`);

    client.close();
  } finally {
    await env.teardown();
  }
});

test('8. BoundedRateQueue rejects with slowDown when all node tokens are exhausted and queue wait expires', async () => {
  const env = await setupCluster(
    [
      {
        id: 'n-limited',
        roles: ['normal'],
        poolSize: 1,
        rateLimit: { ratePerSec: 0.5, burst: 4 },
      },
    ],
    {
      limits: {
        queueMaxWaitMs: 120,
        queueMaxLength: 2,
      },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);

    // First book_offers consumes all 4 burst tokens
    const first = await sendAndWait(client, { id: 'ok', command: 'book_offers' });
    assert.equal(first.status, 'success');

    // Subsequent burst exceeds burst & queue limits -> slowDown
    const results = await Promise.all([
      sendAndWait(client, { id: 'q1', command: 'book_offers' }),
      sendAndWait(client, { id: 'q2', command: 'book_offers' }),
      sendAndWait(client, { id: 'q3', command: 'book_offers' }),
    ]);

    assert.ok(results.every((r) => r.status === 'error' && r.error === 'slowDown'));

    client.close();
  } finally {
    await env.teardown();
  }
});

test('9. Per-IP connection limit rejects excess connections beyond maxConnectionsPerIp', async () => {
  const env = await setupCluster([{ id: 'n-normal', roles: ['normal'] }], {
    server: {
      host: '127.0.0.1',
      port: 0,
      maxConnectionsPerIp: 2,
      heartbeatIntervalMs: 0,
      statsLogIntervalMs: 0,
    },
  });

  try {
    const c1 = await connectClient(env.proxyUrl);
    const c2 = await connectClient(env.proxyUrl);

    await assert.rejects(() => connectClient(env.proxyUrl));

    c1.close();
    c2.close();
  } finally {
    await env.teardown();
  }
});
