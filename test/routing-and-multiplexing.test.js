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

test('1. Command routing directs commands to matching node roles', async () => {
  const env = await setupCluster([
    { id: 'n-normal', roles: ['normal'] },
    { id: 'n-history', roles: ['history'] },
    { id: 'n-submit', roles: ['submit'] },
    { id: 'n-path', roles: ['path'] },
  ]);

  try {
    const client = await connectClient(env.proxyUrl);

    const rNormal = await sendAndWait(client, { id: 'r1', command: 'server_info' });
    assert.equal(rNormal._mockNode, 'n-normal');
    assert.equal(rNormal.id, 'r1');

    const rHistory = await sendAndWait(client, {
      id: 'r2',
      command: 'account_tx',
      account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
    });
    assert.equal(rHistory._mockNode, 'n-history');

    const rSubmit = await sendAndWait(client, {
      id: 'r3',
      command: 'submit',
      tx_blob: 'DEADBEEF',
    });
    assert.equal(rSubmit._mockNode, 'n-submit');

    const rRipplePath = await sendAndWait(client, {
      id: 'r4',
      command: 'ripple_path_find',
    });
    assert.equal(rRipplePath._mockNode, 'n-path');

    const rPathFind = await sendAndWait(client, {
      id: 'r5',
      command: 'path_find',
      subcommand: 'create',
    });
    assert.equal(rPathFind._mockNode, 'n-path');

    client.close();
  } finally {
    await env.teardown();
  }
});

test('2. Explicit numeric ledger_index or ledger_hash routes non-stateful requests to history', async () => {
  const env = await setupCluster([
    { id: 'n-normal', roles: ['normal'] },
    { id: 'n-history', roles: ['history'] },
  ]);

  try {
    const client = await connectClient(env.proxyUrl);

    const rValidated = await sendAndWait(client, {
      id: 10,
      command: 'account_info',
      ledger_index: 'validated',
    });
    assert.equal(rValidated._mockNode, 'n-normal');

    const rNumericLedger = await sendAndWait(client, {
      id: 11,
      command: 'account_info',
      ledger_index: 92450000,
    });
    assert.equal(rNumericLedger._mockNode, 'n-history');

    const rHashLedger = await sendAndWait(client, {
      id: 12,
      command: 'account_info',
      ledger_hash: '4BC50C9B0D8515D3EAAE1E74B29A95804346C491EE1A95BF25E4AAB854A6A652',
    });
    assert.equal(rHashLedger._mockNode, 'n-history');

    client.close();
  } finally {
    await env.teardown();
  }
});

test('3. Boundary IDs (id: 0, id: "", and omitted id) and invalid JSON are preserved accurately', async () => {
  const env = await setupCluster([{ id: 'n-normal', roles: ['normal'] }]);

  try {
    const client = await connectClient(env.proxyUrl);

    const rZero = await sendAndWait(client, { id: 0, command: 'account_info' });
    assert.equal(rZero.id, 0);
    assert.equal('id' in rZero, true);

    const rEmptyStr = await sendAndWait(client, { id: '', command: 'account_info' });
    assert.equal(rEmptyStr.id, '');
    assert.equal('id' in rEmptyStr, true);

    const rNoId = await sendAndWait(client, { command: 'account_info' });
    assert.equal('id' in rNoId, false);

    // Invalid JSON error check
    const errPromise = new Promise((resolve) => {
      client.once('message', (raw) => resolve(JSON.parse(raw.toString('utf8'))));
    });
    client.send('{broken json');
    const errResp = await errPromise;
    assert.equal(errResp.status, 'error');
    assert.equal(errResp.error, 'jsonInvalid');

    client.close();
  } finally {
    await env.teardown();
  }
});

test('4. Multiplexing pool rewrites IDs so concurrent clients with id: 1 never cross-talk', async () => {
  const env = await setupCluster([{ id: 'n-shared', roles: ['normal', 'history'], poolSize: 1 }]);

  try {
    const [c1, c2, c3] = await Promise.all([
      connectClient(env.proxyUrl),
      connectClient(env.proxyUrl),
      connectClient(env.proxyUrl),
    ]);

    const [r1, r2, r3] = await Promise.all([
      sendAndWait(c1, { id: 1, command: 'account_info', account: 'rClientOne' }),
      sendAndWait(c2, { id: 1, command: 'account_tx', account: 'rClientTwo' }),
      sendAndWait(c3, { id: 1, command: 'ping' }),
    ]);

    assert.equal(r1.id, 1);
    assert.equal(r1.result.account_data.Account, 'rClientOne');

    assert.equal(r2.id, 1);
    assert.equal(r2.result.account, 'rClientTwo');
    assert.ok(Array.isArray(r2.result.transactions));

    assert.equal(r3.id, 1);
    assert.equal(r3.result.command, 'ping');

    c1.close();
    c2.close();
    c3.close();
  } finally {
    await env.teardown();
  }
});
