import express from 'express';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer as createViteServer } from 'vite';
import { config as baseConfig, createConfig } from './src/config.js';
import { MockXrpldNode } from './src/mockXrpld.js';
import { ProxyServer } from './src/proxyServer.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const useRealUpstream = process.env.USE_REAL_UPSTREAM === 'true';
const port = Number(process.env.PROXY_PORT || process.env.PORT || baseConfig.server.port || 3000);
const host = process.env.PROXY_HOST || baseConfig.server.host || '0.0.0.0';

async function main() {
  const mockNodes = new Map<string, MockXrpldNode>();

  let proxyConfig;
  if (useRealUpstream) {
    proxyConfig = createConfig({
      server: { host, port },
    });
  } else {
    const resolvedNodes = [];
    for (const nodeCfg of baseConfig.nodes) {
      const mock = new MockXrpldNode({
        id: nodeCfg.id,
        roles: nodeCfg.roles,
      });
      const wsUrl = await mock.start(0);
      mockNodes.set(nodeCfg.id, mock);
      resolvedNodes.push({
        ...nodeCfg,
        wsUrl,
      });
    }
    proxyConfig = createConfig({
      server: { host, port },
      nodes: resolvedNodes,
    });
  }

  const app = express();
  app.use(express.json());

  const proxy = new ProxyServer(proxyConfig);

  app.get('/api/stats', (_req, res) => {
    const stats = proxy.getStats();
    const enrichedNodes = stats.nodes.map((n: Record<string, unknown>) => {
      const mock = mockNodes.get(String(n.id));
      return {
        ...n,
        mockMode: !useRealUpstream,
        mockOnline: mock ? mock.online : n.isOnline,
        loadWarningArmed: mock ? mock.loadWarningArmed : false,
      };
    });
    res.json({
      ...stats,
      mockMode: !useRealUpstream,
      nodes: enrichedNodes,
    });
  });

  app.post('/api/nodes/:id/toggle-online', async (req, res) => {
    const nodeId = req.params.id;
    const nodeRuntime = proxy.nodeManager.getNode(nodeId);
    if (!nodeRuntime) {
      res.status(404).json({ error: `Unknown node: ${nodeId}` });
      return;
    }

    const mock = mockNodes.get(nodeId);
    if (!mock) {
      res.status(400).json({ error: 'toggle-online is only available in mock upstream mode' });
      return;
    }

    if (mock.online) {
      await mock.stop();
      res.json({ ok: true, id: nodeId, online: false, circuitState: nodeRuntime.circuitState });
      return;
    }

    await mock.start();
    nodeRuntime.setCircuitState('CLOSED');
    nodeRuntime.forceReconnectPool();
    res.json({ ok: true, id: nodeId, online: true, circuitState: nodeRuntime.circuitState });
  });

  app.post('/api/nodes/:id/circuit', (req, res) => {
    const nodeId = req.params.id;
    const nodeRuntime = proxy.nodeManager.getNode(nodeId);
    if (!nodeRuntime) {
      res.status(404).json({ error: `Unknown node: ${nodeId}` });
      return;
    }

    const desiredState = req.body?.state;
    if (desiredState !== 'OPEN' && desiredState !== 'CLOSED') {
      res.status(400).json({ error: 'Body must include { "state": "OPEN" | "CLOSED" }' });
      return;
    }

    nodeRuntime.setCircuitState(desiredState);
    if (desiredState === 'CLOSED') {
      nodeRuntime.forceReconnectPool();
    }
    res.json({ ok: true, id: nodeId, circuitState: nodeRuntime.circuitState });
  });

  app.post('/api/nodes/:id/load-warning', (req, res) => {
    const nodeId = req.params.id;
    const nodeRuntime = proxy.nodeManager.getNode(nodeId);
    if (!nodeRuntime) {
      res.status(404).json({ error: `Unknown node: ${nodeId}` });
      return;
    }

    const mock = mockNodes.get(nodeId);
    if (!mock) {
      res.status(400).json({ error: 'load-warning injection is only available in mock upstream mode' });
      return;
    }

    mock.armLoadWarning();
    res.json({ ok: true, id: nodeId, loadWarningArmed: true });
  });

  app.post('/api/nodes/probe-ledger', async (_req, res) => {
    const results = await proxy.nodeManager.probeAllLedgers();
    res.json({ ok: true, results });
  });

  const httpServer = http.createServer(app);

  let vite: Awaited<ReturnType<typeof createViteServer>> | null = null;
  const isProduction = process.env.NODE_ENV === 'production';

  if (!isProduction) {
    vite = await createViteServer({
      server: {
        middlewareMode: true,
        hmr: false,
        ws: false,
      },
      appType: 'custom',
    });
    app.use(vite.middlewares);

    app.get(['/', '/proxy-dashboard', '/proxy-dashboard/*'], async (req, res, next) => {
      try {
        const indexPath = path.resolve(__dirname, 'index.html');
        const template = fs.readFileSync(indexPath, 'utf-8');
        const html = await vite!.transformIndexHtml(req.originalUrl, template);
        res.status(200).set({ 'Content-Type': 'text/html' }).end(html);
      } catch (err) {
        next(err);
      }
    });
  } else {
    const distDir = path.resolve(__dirname, 'dist');
    app.use(express.static(distDir));
    app.get(['/', '/proxy-dashboard', '/proxy-dashboard/*'], (_req, res) => {
      res.sendFile(path.join(distDir, 'index.html'));
    });
  }

  await proxy.start({ httpServer });

  const initialProbeTimer = setTimeout(() => {
    void proxy.nodeManager.probeAllLedgers();
  }, 600);
  if (typeof initialProbeTimer.unref === 'function') {
    initialProbeTimer.unref();
  }

  await new Promise<void>((resolve) => {
    httpServer.listen(port, host, () => {
      console.log(`[server] xrpld-ws-proxy listening on http://${host}:${port} (ws://localhost:${port})`);
      resolve();
    });
  });

  let isShuttingDown = false;
  const shutdown = async (signal: string) => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log(`[server] Received ${signal}, shutting down gracefully...`);

    try {
      await proxy.shutdown();
      if (vite) {
        await vite.close();
      }
      for (const mock of mockNodes.values()) {
        await mock.stop();
      }
      await new Promise<void>((resolve) => {
        httpServer.close(() => resolve());
      });
    } catch (err) {
      console.error('[server] Error during shutdown:', err);
    } finally {
      process.exit(0);
    }
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('[server] Fatal startup error:', err);
  process.exit(1);
});
