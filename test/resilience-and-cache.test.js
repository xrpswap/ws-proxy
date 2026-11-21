import assert from 'node:assert/strict';
import test from 'node:test';
import WebSocket from 'ws';
import { createConfig } from '../src/config.js';
import { MockXrpldNode } from '../src/mockXrpld.js';
import { ProxyServer } from '../src/proxyServer.js';
import {
  decodeXrplClassicAddressToHex,
  encodeXrplAccountIdHexToClassic,
} from '../src/xrplCodec.js';

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
    const mock = new MockXrpldNode({
      id: spec.id,
      roles: spec.roles,
      ledgerIndex: spec.ledgerIndex,
      clioMode: spec.clioMode,
      strictLedgerCheck: spec.strictLedgerCheck,
      rejectSubscribe: spec.rejectSubscribe,
      blacklistedAddresses: spec.blacklistedAddresses,
      blacklistResponseStyle: spec.blacklistResponseStyle,
      submitResponder: spec.submitResponder,
    });
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
    ledgerStream: { enabled: false, shareWithClients: false },
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
    ledgerStream: { enabled: false, shareWithClients: false },
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

test('15. Clio auto-detection & non-mutating recovery: retries unmodified current request on rippled and cools down Clio node', async () => {
  const env = await setupCluster(
    [
      {
        id: 'n-clio-s2',
        roles: ['normal'],
        clioMode: true,
        rateLimit: { ratePerSec: 30, burst: 50 },
      },
      {
        id: 'n-rippled',
        roles: ['normal'],
        clioMode: false,
        rateLimit: { ratePerSec: 30, burst: 10 },
      },
    ],
    {
      limits: {
        clioCooldownMs: 5000,
        circuitBreakerThreshold: 2,
      },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);

    // First request lands on n-clio-s2 (highest tokens=50), gets Clio rejection,
    // transparently retries unmodified request on n-rippled and succeeds
    const r1 = await sendAndWait(client, {
      id: 'cur-1',
      command: 'account_lines',
      account: 'rTest',
      ledger_index: 'current',
    });
    assert.equal(r1.status, 'success');
    assert.equal(r1._mockNode, 'n-rippled');
    assert.equal(r1.id, 'cur-1');

    // Verify request payload forwarded to n-rippled was NOT mutated
    const rippledReq = env.mocks['n-rippled'].receivedCommands.find(
      (c) => c.command === 'account_lines',
    );
    assert.equal(rippledReq.ledger_index, 'current');

    // Verify Clio node was NOT tripped to OPEN circuit, only marked with current cooldown
    const clioStats = env.proxy.getStats().nodes.find((n) => n.id === 'n-clio-s2');
    assert.equal(clioStats.circuitState, 'CLOSED');
    assert.ok(clioStats.currentRejectedCooldownSec > 0);

    // Next ledger_index: "current" skips n-clio-s2 immediately without hitting Clio
    const clioCallsBefore = env.mocks['n-clio-s2'].receivedCommands.length;
    const r2 = await sendAndWait(client, {
      id: 'cur-2',
      command: 'account_lines',
      account: 'rTest',
      ledger_index: 'current',
    });
    assert.equal(r2.status, 'success');
    assert.equal(r2._mockNode, 'n-rippled');
    assert.equal(env.mocks['n-clio-s2'].receivedCommands.length, clioCallsBefore);

    // Validated queries still use high-capacity n-clio-s2
    const rVal = await sendAndWait(client, {
      id: 'val-1',
      command: 'account_lines',
      account: 'rTest',
      ledger_index: 'validated',
    });
    assert.equal(rVal.status, 'success');
    assert.equal(rVal._mockNode, 'n-clio-s2');

    client.close();
  } finally {
    await env.teardown();
  }
});

test('16. Strategy B: lgrNotFound triggers transparent retry on synced node and learns lastValidatedSeq filtering', async () => {
  const env = await setupCluster(
    [
      {
        id: 'n-lagging-s2',
        roles: ['normal', 'history'],
        ledgerIndex: 92450098,
        strictLedgerCheck: true,
        rateLimit: { ratePerSec: 30, burst: 50 },
      },
      {
        id: 'n-synced',
        roles: ['normal', 'history'],
        ledgerIndex: 92450100,
        strictLedgerCheck: true,
        rateLimit: { ratePerSec: 30, burst: 15 },
      },
    ],
    {
      cache: { enabled: false },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);

    // Client requests ledger 92450100: n-lagging-s2 has more tokens so tries first -> lgrNotFound,
    // proxy transparently retries exact payload on n-synced and succeeds!
    const r1 = await sendAndWait(client, {
      id: 'lgr-1',
      command: 'account_lines',
      account: 'rTest',
      ledger_index: 92450100,
    });
    assert.equal(r1.status, 'success');
    assert.equal(r1._mockNode, 'n-synced');
    assert.equal(r1.result.ledger_index, 92450100);

    // Refresh n-lagging-s2 telemetry via account_info at validated (92450098)
    const rLagInfo = await sendAndWait(client, {
      id: 'lag-info',
      command: 'account_info',
      account: 'rTest',
    });
    assert.equal(rLagInfo._mockNode, 'n-lagging-s2');

    const stats = env.proxy.getStats();
    assert.equal(
      stats.nodes.find((n) => n.id === 'n-lagging-s2').lastValidatedSeq,
      92450098,
    );
    assert.equal(
      stats.nodes.find((n) => n.id === 'n-synced').lastValidatedSeq,
      92450100,
    );

    // Now requesting 92450100 directly filters out n-lagging-s2 (92450098 < 92450100)
    const lagCallsBefore = env.mocks['n-lagging-s2'].receivedCommands.length;
    const r2 = await sendAndWait(client, {
      id: 'lgr-2',
      command: 'account_lines',
      account: 'rTest',
      ledger_index: 92450100,
    });
    assert.equal(r2.status, 'success');
    assert.equal(r2._mockNode, 'n-synced');
    assert.equal(env.mocks['n-lagging-s2'].receivedCommands.length, lagCallsBefore);

    client.close();
  } finally {
    await env.teardown();
  }
});

test('17. Strategy B safe-lag: ledger_closed prefers safe lagging node so subsequent requests succeed on all nodes', async () => {
  const env = await setupCluster(
    [
      {
        id: 'n-fast',
        roles: ['normal'],
        ledgerIndex: 92450105,
        strictLedgerCheck: true,
        rateLimit: { ratePerSec: 30, burst: 50 },
      },
      {
        id: 'n-s2-safe',
        roles: ['normal'],
        ledgerIndex: 92450102,
        strictLedgerCheck: true,
        rateLimit: { ratePerSec: 30, burst: 15 },
      },
    ],
    {
      cache: { enabled: false },
      routing: { preferSafeLagForLedgerClosed: true },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);

    // Seed node heights passively
    await sendAndWait(client, { id: 's1', command: 'account_info' }); // hits n-fast (105)
    await sendAndWait(client, { id: 's2', command: 'ledger_closed' }); // hits n-s2-safe (102)

    // Subsequent ledger_closed prefers n-s2-safe (102 <= 105, within maxSafeLedgerLag=15)
    // even when n-fast has more tokens!
    const lc = await sendAndWait(client, { id: 'lc-safe', command: 'ledger_closed' });
    assert.equal(lc.status, 'success');
    assert.equal(lc._mockNode, 'n-s2-safe');
    assert.equal(lc.result.ledger_index, 92450102);

    // Client queries account_lines with 92450102 -> succeeds on n-fast too!
    const al = await sendAndWait(client, {
      id: 'al-safe',
      command: 'account_lines',
      ledger_index: lc.result.ledger_index,
    });
    assert.equal(al.status, 'success');
    assert.equal(al.result.ledger_index, 92450102);

    client.close();
  } finally {
    await env.teardown();
  }
});

test('18. Upstream ledger stream pre-populates node heights so explicit ledger_index hits zero lgrNotFound', async () => {
  const env = await setupCluster(
    [
      {
        id: 'n-lag',
        roles: ['normal'],
        ledgerIndex: 92450098,
        strictLedgerCheck: true,
        rateLimit: { ratePerSec: 30, burst: 50 },
      },
      {
        id: 'n-synced',
        roles: ['normal'],
        ledgerIndex: 92450100,
        strictLedgerCheck: true,
        rateLimit: { ratePerSec: 30, burst: 15 },
      },
    ],
    {
      cache: { enabled: false },
      ledgerStream: { enabled: true, shareWithClients: true },
    },
  );

  try {
    // Wait briefly for pool[0] background ledger subscribe to populate initial heights
    await new Promise((r) => setTimeout(r, 80));

    const stats = env.proxy.getStats();
    assert.equal(stats.nodes.find((n) => n.id === 'n-lag').lastValidatedSeq, 92450098);
    assert.equal(stats.nodes.find((n) => n.id === 'n-synced').lastValidatedSeq, 92450100);

    const lagCommandsBefore = env.mocks['n-lag'].receivedCommands.length;
    const client = await connectClient(env.proxyUrl);

    // Even on the very first client request, n-lag is skipped -> 0 lgrNotFound!
    const res = await sendAndWait(client, {
      id: 'zero-lgr',
      command: 'account_lines',
      account: 'rTest',
      ledger_index: 92450100,
    });
    assert.equal(res.status, 'success');
    assert.equal(res._mockNode, 'n-synced');
    assert.equal(env.mocks['n-lag'].receivedCommands.length, lagCommandsBefore);

    client.close();
  } finally {
    await env.teardown();
  }
});

test('19. Shared ledger fan-out broadcasts monotonic safeBroadcastSeq and prevents lgrNotFound across nodes', async () => {
  const env = await setupCluster(
    [
      {
        id: 'n-fast',
        roles: ['normal'],
        ledgerIndex: 92450105,
        strictLedgerCheck: true,
        maxSubscribeConns: 0,
      },
      {
        id: 'n-safe',
        roles: ['normal'],
        ledgerIndex: 92450102,
        strictLedgerCheck: true,
        maxSubscribeConns: 0,
      },
      {
        id: 'n-outlier',
        roles: ['normal'],
        ledgerIndex: 92450070, // lag > maxSafeLedgerLag (15), ignored for safe watermark
        strictLedgerCheck: true,
        maxSubscribeConns: 0,
      },
    ],
    {
      cache: { enabled: false },
      ledgerStream: { enabled: true, shareWithClients: true, maxSnapshots: 10 },
    },
  );

  try {
    await new Promise((r) => setTimeout(r, 80));

    // Even with maxSubscribeConns: 0 on all nodes, 4 clients can subscribe to shared ledger stream
    const clients = await Promise.all([
      connectClient(env.proxyUrl),
      connectClient(env.proxyUrl),
      connectClient(env.proxyUrl),
      connectClient(env.proxyUrl),
    ]);

    const eventsByClient = clients.map(() => []);
    clients.forEach((ws, idx) => {
      ws.on('message', (raw) => {
        const msg = JSON.parse(raw.toString('utf8'));
        if (msg.type === 'ledgerClosed') {
          eventsByClient[idx].push(msg);
        }
      });
    });

    const subResponses = await Promise.all(
      clients.map((ws, i) =>
        sendAndWait(ws, { id: `sub-${i}`, command: 'subscribe', streams: ['ledger'] }),
      ),
    );

    for (const r of subResponses) {
      assert.equal(r.status, 'success');
      assert.equal(r.result.ledger_index, 92450102);
    }
    assert.equal(env.proxy.getStats().sharedLedgerSubscribers, 4);
    assert.ok(env.proxy.getStats().nodes.every((n) => n.activeSubscribeConns === 0));

    // Fast node advances to 106 -> safe watermark stays at 102, so no premature 106 push!
    env.mocks['n-fast'].emitLedgerClosed(92450106);
    await new Promise((r) => setTimeout(r, 40));
    assert.equal(eventsByClient[0].length, 0);

    // Safe node advances to 103 -> safe watermark advances to 103 -> all 4 clients receive #103 once!
    env.mocks['n-safe'].emitLedgerClosed(92450103);
    await new Promise((r) => setTimeout(r, 40));

    for (const evts of eventsByClient) {
      assert.equal(evts.length, 1);
      assert.equal(evts[0].ledger_index, 92450103);
    }

    // Subsequent account_lines query at broadcasted 92450103 succeeds on both n-fast and n-safe!
    const queryResp = await sendAndWait(clients[0], {
      id: 'q-103',
      command: 'account_lines',
      ledger_index: 92450103,
    });
    assert.equal(queryResp.status, 'success');
    assert.equal(queryResp.result.ledger_index, 92450103);

    clients.forEach((c) => c.close());
  } finally {
    await env.teardown();
  }
});

test('20. Shared ledger cleanup on unsubscribe and disconnect leaves zero leaked subscribers; mixed streams use 1:1 channel', async () => {
  const env = await setupCluster(
    [
      {
        id: 'n-main',
        roles: ['normal'],
        ledgerIndex: 92450200,
        maxSubscribeConns: 2,
      },
    ],
    {
      ledgerStream: { enabled: true, shareWithClients: true },
    },
  );

  try {
    await new Promise((r) => setTimeout(r, 80));

    const cShared = await connectClient(env.proxyUrl);
    const cMixed = await connectClient(env.proxyUrl);

    const r1 = await sendAndWait(cShared, {
      id: 'sh-1',
      command: 'subscribe',
      streams: ['ledger'],
    });
    assert.equal(r1.status, 'success');
    assert.equal(env.proxy.getStats().sharedLedgerSubscribers, 1);

    // Unsubscribe from shared ledger without creating a 1:1 stateful channel
    const u1 = await sendAndWait(cShared, {
      id: 'unsub-1',
      command: 'unsubscribe',
      streams: ['ledger'],
    });
    assert.equal(u1.status, 'success');
    assert.equal(env.proxy.getStats().sharedLedgerSubscribers, 0);
    assert.equal(env.proxy.getStats().nodes[0].activeSubscribeConns, 0);

    // Re-subscribe then abruptly close socket -> cleanupSession must clear count to 0
    await sendAndWait(cShared, { id: 'sh-2', command: 'subscribe', streams: ['ledger'] });
    assert.equal(env.proxy.getStats().sharedLedgerSubscribers, 1);

    // Mixed subscription (ledger + transactions) uses 1:1 StatefulChannel
    const rMix = await sendAndWait(cMixed, {
      id: 'mix-1',
      command: 'subscribe',
      streams: ['ledger', 'transactions'],
    });
    assert.equal(rMix.status, 'success');
    assert.equal(env.proxy.getStats().nodes[0].activeSubscribeConns, 1);
    assert.equal(env.proxy.getStats().sharedLedgerSubscribers, 1);

    cShared.close();
    cMixed.close();
    await new Promise((r) => setTimeout(r, 60));

    const finalStats = env.proxy.getStats();
    assert.equal(finalStats.sharedLedgerSubscribers, 0);
    assert.equal(finalStats.clientConnections, 0);
    assert.equal(finalStats.nodes[0].activeSubscribeConns, 0);
  } finally {
    await env.teardown();
  }
});

test('21. Internal ledger subscribe rejection does not trip circuit breaker and snapshot cache stays bounded', async () => {
  const env = await setupCluster(
    [
      {
        id: 'n-reject-sub',
        roles: ['normal'],
        ledgerIndex: 92450300,
        rejectSubscribe: true,
      },
      {
        id: 'n-ok',
        roles: ['normal'],
        ledgerIndex: 92450300,
      },
    ],
    {
      limits: { circuitBreakerThreshold: 1 },
      ledgerStream: { enabled: true, shareWithClients: true, maxSnapshots: 5 },
    },
  );

  try {
    await new Promise((r) => setTimeout(r, 80));

    const statsBefore = env.proxy.getStats();
    const rejectNodeStats = statsBefore.nodes.find((n) => n.id === 'n-reject-sub');
    assert.equal(rejectNodeStats.circuitState, 'CLOSED');
    assert.equal(rejectNodeStats.consecutiveFailures, 0);

    // Push 12 ledgers from n-ok -> snapshot cache must remain bounded to maxSnapshots (5)
    for (let seq = 92450301; seq <= 92450312; seq += 1) {
      env.mocks['n-ok'].emitLedgerClosed(seq);
    }
    await new Promise((r) => setTimeout(r, 50));

    const statsAfter = env.proxy.getStats();
    assert.ok(
      statsAfter.snapshotCacheSize <= 5,
      `Expected snapshotCacheSize <= 5, got ${statsAfter.snapshotCacheSize}`,
    );

    await env.proxy.shutdown();
    assert.equal(env.proxy.nodeManager.ledgerSnapshots.size, 0);
  } finally {
    await env.teardown();
  }
});

test('22. Half-open dead upstream connection (missed pongs) is terminated, reconnected, and re-subscribes ledger stream', async () => {
  const http = await import('node:http');
  const { WebSocketServer } = await import('ws');

  let connCount = 0;
  let firstServerSocket = null;
  let secondSubscribed = false;

  const srv = http.createServer();
  const wss = new WebSocketServer({ server: srv });
  wss.on('connection', (ws) => {
    connCount += 1;
    const myIndex = connCount;
    if (myIndex === 1) {
      firstServerSocket = ws;
    }

    ws.on('message', (raw) => {
      const req = JSON.parse(raw.toString('utf8'));
      if (myIndex === 1) {
        if (req.command === 'subscribe') {
          ws.send(
            JSON.stringify({
              id: req.id,
              status: 'success',
              type: 'response',
              result: { ledger_index: 92450500, validated_ledgers: '32570-92450500' },
            }),
          );
          // Simulate half-open network freeze: stop reading frames so no pong/close is ever sent
          ws._socket.pause();
          ws._socket.removeAllListeners('data');
        }
        return;
      }

      if (req.command === 'subscribe') {
        secondSubscribed = true;
        ws.send(
          JSON.stringify({
            id: req.id,
            status: 'success',
            type: 'response',
            result: { ledger_index: 92450505, validated_ledgers: '32570-92450505' },
          }),
        );
      } else {
        ws.send(
          JSON.stringify({
            id: req.id,
            status: 'success',
            type: 'response',
            result: { recovered: true },
          }),
        );
      }
    });
  });

  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  const port = srv.address().port;

  const cfg = createConfig({
    server: { host: '127.0.0.1', port: 0, heartbeatIntervalMs: 0, statsLogIntervalMs: 0 },
    ledgerStream: { enabled: true, shareWithClients: true },
    limits: {
      upstreamPingIntervalMs: 60,
      upstreamPongTimeoutMs: 30,
      upstreamMaxMissedPongs: 2,
      reconnectBaseDelayMs: 40,
      reconnectMaxDelayMs: 80,
      circuitBreakerThreshold: 5,
    },
    nodes: [
      {
        id: 'n-halfopen',
        wsUrl: `ws://127.0.0.1:${port}/`,
        roles: ['normal'],
        poolSize: 1,
        maxPathConns: 0,
        maxSubscribeConns: 0,
        rateLimit: { ratePerSec: 30, burst: 30 },
      },
    ],
  });

  const proxy = new ProxyServer(cfg);
  const addr = await proxy.start();
  const proxyUrl = `ws://127.0.0.1:${addr.port}/`;

  try {
    // Wait for initial connection -> 2 missed pongs -> terminate -> reconnect -> re-subscribe
    const deadline = Date.now() + 3000;
    while ((!secondSubscribed || connCount < 2) && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 30));
    }

    assert.ok(connCount >= 2, `Expected reconnect attempt, connCount=${connCount}`);
    assert.equal(secondSubscribed, true);
    assert.equal(proxy.getStats().nodes[0].lastValidatedSeq, 92450505);

    const client = await connectClient(proxyUrl);
    const resp = await sendAndWait(client, { id: 'after-rec', command: 'account_info' });
    assert.equal(resp.status, 'success');
    assert.equal(resp.result.recovered, true);
    client.close();
  } finally {
    if (firstServerSocket) {
      try {
        firstServerSocket.terminate();
      } catch {
        // ignore
      }
    }
    await proxy.shutdown();
    wss.clients.forEach((ws) => ws.terminate());
    await new Promise((r) => wss.close(() => r()));
    await new Promise((r) => srv.close(() => r()));
  }
});

test('23. Xaman Blacklist submit rejection retries on healthy peer, caches flagged AccountID, and never retries status=success', async () => {
  const flaggedAddr = 'rN1bCPAxHDvyJzvkUso1L2wvXufgE4gXPL';
  const flaggedHex = decodeXrplClassicAddressToHex(flaggedAddr);
  assert.ok(flaggedHex && flaggedHex.length === 40);
  assert.equal(encodeXrplAccountIdHexToClassic(flaggedHex), flaggedAddr);

  const cleanAddr = 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh';
  const cleanHex = decodeXrplClassicAddressToHex(cleanAddr);
  assert.ok(cleanHex && cleanHex.length === 40);

  let forceSuccessWithBlacklistedText = false;

  const env = await setupCluster(
    [
      {
        id: 'n-xrplcluster-bl',
        roles: ['submit'],
        rateLimit: { ratePerSec: 100, burst: 100 },
        blacklistedAddresses: [flaggedAddr],
        submitResponder: (req, nodeId) => {
          if (forceSuccessWithBlacklistedText) {
            return {
              status: 'success',
              type: 'response',
              result: {
                engine_result: 'tecPATH_DRY',
                engine_result_code: 128,
                engine_result_message: `Path could not send partial amount (Xaman Blacklist note for ${flaggedAddr})`,
                tx_json: { TransactionType: 'Payment', hash: 'ABCDEF' },
              },
              _mockNode: nodeId,
            };
          }
          return null;
        },
      },
      {
        id: 'n-s1',
        roles: ['submit'],
        rateLimit: { ratePerSec: 10, burst: 10 },
      },
    ],
    {
      limits: {
        circuitBreakerThreshold: 1,
        blacklistCacheTtlMs: 60000,
        blacklistCacheMaxEntries: 50,
      },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);

    // Byte-aligned Payment tx_blob embedding flagged 20-byte AccountID
    const flaggedBlob = `1200002280000000240000000161400000000000000A8314${flaggedHex}732102AABBCC`;
    const cleanBlob = `1200002280000000240000000261400000000000000A8314${cleanHex}732102AABBCC`;

    // 1. First submit hits n-xrplcluster-bl (higher tokens), gets blacklist error, transparently retries to n-s1
    const r1 = await sendAndWait(client, {
      id: 'sub-bl-1',
      command: 'submit',
      tx_blob: flaggedBlob,
    });
    assert.equal(r1.status, 'success');
    assert.equal(r1._mockNode, 'n-s1');

    const hitsAfterFirst = env.mocks['n-xrplcluster-bl'].receivedCommands.filter(
      (c) => c.command === 'submit',
    ).length;
    assert.equal(hitsAfterFirst, 1);

    const statsAfterFirst = env.proxy.getStats();
    const blNodeStats = statsAfterFirst.nodes.find((n) => n.id === 'n-xrplcluster-bl');
    assert.equal(blNodeStats.circuitState, 'CLOSED');
    assert.equal(blNodeStats.consecutiveFailures, 0);
    assert.equal(blNodeStats.blacklistedCount, 1);
    assert.equal(blNodeStats.blacklistRejections, 1);
    assert.equal(blNodeStats.retriedOut, 1);
    assert.equal(statsAfterFirst.totalRetries, 1);
    assert.equal(statsAfterFirst.blacklistRetries, 1);
    assert.equal(statsAfterFirst.lastRetry?.fromNodeId, 'n-xrplcluster-bl');
    assert.equal(statsAfterFirst.lastRetry?.toNodeId, 'n-s1');
    assert.equal(statsAfterFirst.lastRetry?.reason, 'blacklist');

    // 2. Second submit with same flagged tx_blob skips n-xrplcluster-bl entirely (0 extra hits)
    const r2 = await sendAndWait(client, {
      id: 'sub-bl-2',
      command: 'submit',
      tx_blob: flaggedBlob,
    });
    assert.equal(r2.status, 'success');
    assert.equal(r2._mockNode, 'n-s1');
    const hitsAfterSecond = env.mocks['n-xrplcluster-bl'].receivedCommands.filter(
      (c) => c.command === 'submit',
    ).length;
    assert.equal(hitsAfterSecond, 1);

    const statsAfterSecond = env.proxy.getStats();
    assert.equal(statsAfterSecond.blacklistBypassed, 1);
    assert.equal(
      statsAfterSecond.nodes.find((n) => n.id === 'n-xrplcluster-bl').blacklistBypassed,
      1,
    );

    // 3. Normal non-blacklisted tx_blob continues using n-xrplcluster-bl
    const rClean = await sendAndWait(client, {
      id: 'sub-clean-1',
      command: 'submit',
      tx_blob: cleanBlob,
    });
    assert.equal(rClean.status, 'success');
    assert.equal(rClean._mockNode, 'n-xrplcluster-bl');

    // 4. status === 'success' with tecPATH_DRY (even mentioning blacklist words) MUST NEVER retry
    forceSuccessWithBlacklistedText = true;
    const s1HitsBefore = env.mocks['n-s1'].receivedCommands.filter(
      (c) => c.command === 'submit',
    ).length;
    const rTec = await sendAndWait(client, {
      id: 'sub-tec-1',
      command: 'submit',
      tx_blob: cleanBlob,
    });
    assert.equal(rTec.status, 'success');
    assert.equal(rTec.result.engine_result, 'tecPATH_DRY');
    assert.equal(rTec._mockNode, 'n-xrplcluster-bl');
    const s1HitsAfter = env.mocks['n-s1'].receivedCommands.filter(
      (c) => c.command === 'submit',
    ).length;
    assert.equal(s1HitsAfter, s1HitsBefore);

    // 5. Legacy status === 'error' rpc_error format is also supported
    forceSuccessWithBlacklistedText = false;
    const rpcErrAddr = encodeXrplAccountIdHexToClassic('44'.repeat(20));
    const rpcErrHex = decodeXrplClassicAddressToHex(rpcErrAddr);
    env.mocks['n-xrplcluster-bl'].blacklistResponseStyle = 'rpc_error';
    env.mocks['n-xrplcluster-bl'].blacklistedTargets.set(rpcErrAddr, {
      classic: rpcErrAddr,
      accountIdHex: rpcErrHex,
    });
    const rpcErrBlob = `1200072200000000240000000564D5038D7EA4C68000${'00'.repeat(20)}${rpcErrHex}6540000000000F4240`;
    const rRpcErr = await sendAndWait(client, {
      id: 'sub-rpc-err-1',
      command: 'submit',
      tx_blob: rpcErrBlob,
    });
    assert.equal(rRpcErr.status, 'success');
    assert.equal(rRpcErr._mockNode, 'n-s1');
    assert.equal(env.proxy.getStats().totalRetries, 2);

    client.close();
  } finally {
    await env.teardown();
  }
});

test('24. Blacklist negative cache enforces bounded maxEntries eviction and TTL expiration', async () => {
  const addr1 = encodeXrplAccountIdHexToClassic('11'.repeat(20));
  const addr2 = encodeXrplAccountIdHexToClassic('22'.repeat(20));
  const addr3 = encodeXrplAccountIdHexToClassic('33'.repeat(20));
  assert.ok(addr1 && addr2 && addr3);

  const env = await setupCluster(
    [
      {
        id: 'n-bl-bounded',
        roles: ['submit'],
        rateLimit: { ratePerSec: 100, burst: 100 },
        blacklistedAddresses: [addr1, addr2, addr3],
      },
      {
        id: 'n-clean-peer',
        roles: ['submit'],
        rateLimit: { ratePerSec: 10, burst: 10 },
      },
    ],
    {
      limits: {
        blacklistCacheMaxEntries: 2,
        blacklistCacheTtlMs: 120,
      },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);

    for (const [idx, addr] of [addr1, addr2, addr3].entries()) {
      const resp = await sendAndWait(client, {
        id: `bl-cap-${idx}`,
        command: 'submit',
        tx_json: { TransactionType: 'Payment', Destination: addr },
      });
      assert.equal(resp.status, 'success');
      assert.equal(resp._mockNode, 'n-clean-peer');
    }

    // LRU cap must hold blacklistedCount <= 2 (addr1 evicted when addr3 was inserted)
    const runtime = env.proxy.nodeManager.getNode('n-bl-bounded');
    assert.equal(runtime.blacklistedTargets.size, 2);
    assert.equal(runtime.blacklistedTargets.has(addr1), false);
    assert.equal(runtime.blacklistedTargets.has(addr2), true);
    assert.equal(runtime.blacklistedTargets.has(addr3), true);

    // Wait past blacklistCacheTtlMs (120ms) -> cache expires & node re-accepts traffic
    await new Promise((r) => setTimeout(r, 160));
    const statsAfterTtl = env.proxy.getStats();
    const blStats = statsAfterTtl.nodes.find((n) => n.id === 'n-bl-bounded');
    assert.equal(blStats.blacklistedCount, 0);

    // Next submit with addr2 will hit n-bl-bounded again (then failover to n-clean-peer)
    const beforeHits = env.mocks['n-bl-bounded'].receivedCommands.filter(
      (c) => c.command === 'submit',
    ).length;
    const afterTtlResp = await sendAndWait(client, {
      id: 'bl-after-ttl',
      command: 'submit',
      tx_json: { TransactionType: 'Payment', Destination: addr2 },
    });
    assert.equal(afterTtlResp.status, 'success');
    const afterHits = env.mocks['n-bl-bounded'].receivedCommands.filter(
      (c) => c.command === 'submit',
    ).length;
    assert.equal(afterHits, beforeHits + 1);

    client.close();
  } finally {
    await env.teardown();
  }
});

test('25. Permanent blacklist cache (ttl=0) and multi-node chain failover (3 blacklisting submit nodes -> s1/s2/honeycluster)', async () => {
  const flaggedIssuer = 'rN1bCPAxHDvyJzvkUso1L2wvXufgE4gXPL';
  const flaggedHex = decodeXrplClassicAddressToHex(flaggedIssuer);
  assert.ok(flaggedHex && flaggedHex.length === 40);

  const env = await setupCluster(
    [
      {
        id: 'n-xrplcluster',
        roles: ['submit'],
        rateLimit: { ratePerSec: 100, burst: 100 },
        blacklistedAddresses: [flaggedIssuer],
      },
      {
        id: 'n-xrplws',
        roles: ['submit'],
        rateLimit: { ratePerSec: 90, burst: 90 },
        blacklistedAddresses: [flaggedIssuer],
      },
      {
        id: 'n-xrpllink',
        roles: ['submit'],
        rateLimit: { ratePerSec: 80, burst: 80 },
        blacklistedAddresses: [flaggedIssuer],
      },
      {
        id: 'n-s1',
        roles: ['submit'],
        rateLimit: { ratePerSec: 20, burst: 20 },
      },
      {
        id: 'n-s2',
        roles: ['submit'],
        rateLimit: { ratePerSec: 20, burst: 20 },
      },
      {
        id: 'n-honey',
        roles: ['submit'],
        rateLimit: { ratePerSec: 20, burst: 20 },
      },
    ],
    {
      limits: {
        ledgerRetryMaxAttempts: 2,
        blacklistCacheTtlMs: 0, // Permanent
        blacklistCacheMaxEntries: 1000,
      },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);

    // Simulated OfferCreate tx_blob containing a 48-byte IOU Amount (TakerPays/TakerGets with 20-byte Issuer AccountID)
    const offerBlob = `12000722000000002400000005D520000000000000000000000000000000000000000000544F4B454E000000${flaggedHex}6540000000000F4240732102AABBCC`;

    // 1. First OfferCreate fails on n-xrplcluster -> n-xrplws -> n-xrpllink (3 rejections > ledgerRetryMaxAttempts=2) and succeeds on n-s1/n-s2/n-honey
    const r1 = await sendAndWait(client, {
      id: 'offer-bl-1',
      command: 'submit',
      tx_blob: offerBlob,
    });
    assert.equal(r1.status, 'success');
    assert.ok(['n-s1', 'n-s2', 'n-honey'].includes(r1._mockNode));

    for (const blId of ['n-xrplcluster', 'n-xrplws', 'n-xrpllink']) {
      const runtime = env.proxy.nodeManager.getNode(blId);
      assert.equal(runtime.blacklistedTargets.size, 1);
      assert.equal(runtime.blacklistedTargets.get(flaggedIssuer)?.expiresAt, null);
    }

    // 2. Permanent cache must NOT expire after delay
    await new Promise((r) => setTimeout(r, 150));
    const stats = env.proxy.getStats();
    for (const blId of ['n-xrplcluster', 'n-xrplws', 'n-xrpllink']) {
      const st = stats.nodes.find((n) => n.id === blId);
      assert.equal(st.blacklistedCount, 1);
      assert.equal(st.circuitState, 'CLOSED');
    }

    const beforeHits = {
      cluster: env.mocks['n-xrplcluster'].receivedCommands.length,
      ws: env.mocks['n-xrplws'].receivedCommands.length,
      link: env.mocks['n-xrpllink'].receivedCommands.length,
    };

    // 3. Subsequent OfferCreate routes straight to n-s1 / n-s2 / n-honey with 0 hits on any blacklisting node
    const r2 = await sendAndWait(client, {
      id: 'offer-bl-2',
      command: 'submit',
      tx_blob: offerBlob,
    });
    assert.equal(r2.status, 'success');
    assert.ok(['n-s1', 'n-s2', 'n-honey'].includes(r2._mockNode));
    assert.equal(env.mocks['n-xrplcluster'].receivedCommands.length, beforeHits.cluster);
    assert.equal(env.mocks['n-xrplws'].receivedCommands.length, beforeHits.ws);
    assert.equal(env.mocks['n-xrpllink'].receivedCommands.length, beforeHits.link);

    client.close();
  } finally {
    await env.teardown();
  }
});

test('26. Upstream tooBusy/slowDown error retries transparently on healthy peer and applies backpressure', async () => {
  const env = await setupCluster(
    [
      {
        id: 'n-busy-clio',
        roles: ['normal'],
        clioMode: true,
        rateLimit: { ratePerSec: 20, burst: 50 },
      },
      {
        id: 'n-healthy',
        roles: ['normal'],
        rateLimit: { ratePerSec: 20, burst: 20 },
      },
    ],
    {
      limits: {
        circuitBreakerThreshold: 3,
        slowDownRecoveryMs: 5000,
      },
    },
  );

  try {
    const client = await connectClient(env.proxyUrl);
    env.mocks['n-busy-clio'].armLoadError('tooBusy');

    const res = await sendAndWait(client, {
      id: 1131,
      command: 'account_info',
      account: 'rBQkEKFMnBjc19DRaY7ThLJ2woqQManbTt',
      ledger_index: 92450050,
    });

    assert.equal(res.status, 'success');
    assert.equal(res.id, 1131);
    assert.equal(res._mockNode, 'n-healthy');

    const stats = env.proxy.getStats();
    assert.equal(stats.totalRetries, 1);
    assert.equal(stats.lastRetry?.reason, 'tooBusy');
    assert.equal(stats.lastRetry?.fromNodeId, 'n-busy-clio');
    assert.equal(stats.lastRetry?.toNodeId, 'n-healthy');

    const busyStats = stats.nodes.find((n) => n.id === 'n-busy-clio');
    assert.equal(busyStats.isSlowedDown, true);
    assert.equal(busyStats.rateMultiplier, 0.5);
    assert.equal(busyStats.consecutiveFailures, 1);
    assert.equal(busyStats.retriedOut, 1);

    client.close();
  } finally {
    await env.teardown();
  }
});




