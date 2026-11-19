import http from 'node:http';
import { WebSocketServer } from 'ws';

export class MockXrpldNode {
  constructor({ id = 'mock-node', roles = ['normal'], ledgerIndex = 92450100 } = {}) {
    this.id = id;
    this.roles = roles;
    this.ledgerIndex = ledgerIndex;
    this.server = null;
    this.wss = null;
    this.port = 0;
    this.online = false;
    this.loadWarningArmed = false;
    this.receivedCommands = [];
    this.activeConnections = new Set();
    this.streamTimers = new Map();
  }

  get wsUrl() {
    return `ws://127.0.0.1:${this.port}/`;
  }

  async start(port = 0) {
    if (this.online && this.server) {
      return this.wsUrl;
    }
    const listenPort = this.port > 0 ? this.port : port;
    this.server = http.createServer((_req, res) => {
      res.writeHead(426);
      res.end('WebSocket required');
    });
    this.wss = new WebSocketServer({ server: this.server });

    this.wss.on('connection', (ws) => {
      this.activeConnections.add(ws);

      const onMessage = (raw) => {
        let req;
        try {
          req = JSON.parse(typeof raw === 'string' ? raw : raw.toString('utf8'));
        } catch {
          return;
        }
        this.receivedCommands.push(req);
        this.handleRequest(ws, req);
      };

      const onClose = () => {
        this.activeConnections.delete(ws);
        const timer = this.streamTimers.get(ws);
        if (timer) {
          clearInterval(timer);
          this.streamTimers.delete(ws);
        }
        ws.off('message', onMessage);
        ws.off('close', onClose);
      };

      ws.on('message', onMessage);
      ws.on('close', onClose);
      ws.on('error', () => {});
    });

    await new Promise((resolve, reject) => {
      const onErr = (err) => {
        this.server.off('listening', onListen);
        reject(err);
      };
      const onListen = () => {
        this.server.off('error', onErr);
        const addr = this.server.address();
        this.port = addr.port;
        this.online = true;
        resolve();
      };
      this.server.once('error', onErr);
      this.server.once('listening', onListen);
      this.server.listen(listenPort, '127.0.0.1');
    });

    return this.wsUrl;
  }

  armLoadWarning() {
    this.loadWarningArmed = true;
  }

  handleRequest(ws, req) {
    const cmd = req?.command;
    const response = {
      status: 'success',
      type: 'response',
      result: {},
      _mockNode: this.id,
    };

    if (req.id !== undefined && req.id !== null) {
      response.id = req.id;
    }

    if (this.loadWarningArmed) {
      this.loadWarningArmed = false;
      response.warning = 'load';
    }

    switch (cmd) {
      case 'server_info':
        response.result = {
          info: {
            build_version: '2.3.0',
            complete_ledgers: '32570-92450100',
            hostid: this.id,
            io_latency_ms: 1,
            server_state: 'full',
            validated_ledger: {
              seq: this.ledgerIndex,
              hash: '4BC50C9B0D8515D3EAAE1E74B29A95804346C491EE1A95BF25E4AAB854A6A652',
            },
          },
        };
        break;
      case 'fee':
        response.result = {
          current_ledger_size: '56',
          drops: {
            base_fee: '10',
            median_fee: '5000',
            minimum_fee: '10',
            open_ledger_fee: '10',
          },
          ledger_current_index: this.ledgerIndex + 1,
        };
        break;
      case 'ledger_closed':
        response.result = {
          ledger_hash: '4BC50C9B0D8515D3EAAE1E74B29A95804346C491EE1A95BF25E4AAB854A6A652',
          ledger_index: this.ledgerIndex,
        };
        break;
      case 'account_info':
        response.result = {
          account_data: {
            Account: req.account || 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
            Balance: '100000000',
            Sequence: 42,
          },
          ledger_index: req.ledger_index || this.ledgerIndex,
          validated: true,
        };
        break;
      case 'account_tx':
      case 'tx':
      case 'tx_history':
      case 'transaction_entry':
      case 'nft_history':
      case 'book_changes':
        response.result = {
          account: req.account || 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
          ledger_index_min: 32570,
          ledger_index_max: this.ledgerIndex,
          transactions: [],
          validated: true,
        };
        break;
      case 'submit':
      case 'submit_multisigned':
      case 'sign_and_submit':
        response.result = {
          engine_result: 'tesSUCCESS',
          engine_result_code: 0,
          engine_result_message: 'The transaction was applied.',
          tx_json: {
            TransactionType: 'Payment',
            hash: 'E08D6E9754025BA2534A78707605E0601F03ACE063687A0CA1BDDACFCD1698C7',
          },
        };
        break;
      case 'ripple_path_find':
      case 'book_offers':
        response.result = {
          alternatives: [],
          offers: [],
          ledger_current_index: this.ledgerIndex,
        };
        break;
      case 'path_find':
        if (req.subcommand === 'close') {
          const timer = this.streamTimers.get(ws);
          if (timer) {
            clearInterval(timer);
            this.streamTimers.delete(ws);
          }
          response.result = { closed: true };
        } else {
          response.result = {
            alternatives: [
              {
                paths_computed: [],
                source_amount: req.source_amount || '1000000',
              },
            ],
            source_account: req.source_account || 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
            destination_account: req.destination_account || 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh',
          };
          if (!this.streamTimers.has(ws)) {
            const timer = setInterval(() => {
              if (ws.readyState === 1) {
                ws.send(
                  JSON.stringify({
                    type: 'path_find',
                    alternatives: response.result.alternatives,
                    _mockNode: this.id,
                  }),
                );
              }
            }, 3500);
            if (typeof timer.unref === 'function') timer.unref();
            this.streamTimers.set(ws, timer);
          }
        }
        break;
      case 'subscribe':
        response.result = {
          ledger_index: this.ledgerIndex,
          validated_ledgers: `32570-${this.ledgerIndex}`,
        };
        if (!this.streamTimers.has(ws)) {
          const timer = setInterval(() => {
            if (ws.readyState === 1) {
              this.ledgerIndex += 1;
              ws.send(
                JSON.stringify({
                  type: 'ledgerClosed',
                  ledger_index: this.ledgerIndex,
                  ledger_time: Math.floor(Date.now() / 1000),
                  _mockNode: this.id,
                }),
              );
            }
          }, 4000);
          if (typeof timer.unref === 'function') timer.unref();
          this.streamTimers.set(ws, timer);
        }
        break;
      case 'unsubscribe': {
        const timer = this.streamTimers.get(ws);
        if (timer) {
          clearInterval(timer);
          this.streamTimers.delete(ws);
        }
        response.result = {};
        break;
      }
      case 'ping':
      default:
        response.result = { role: this.roles[0] || 'normal', command: cmd };
        break;
    }

    if (ws.readyState === 1) {
      ws.send(JSON.stringify(response));
    }
  }

  async stop() {
    this.online = false;
    for (const timer of this.streamTimers.values()) {
      clearInterval(timer);
    }
    this.streamTimers.clear();

    if (this.wss) {
      const wss = this.wss;
      this.wss = null;
      for (const ws of Array.from(wss.clients)) {
        try {
          ws.terminate();
        } catch {
          // ignore
        }
      }
      await new Promise((resolve) => wss.close(() => resolve()));
      wss.removeAllListeners();
    }

    for (const ws of Array.from(this.activeConnections)) {
      try {
        ws.terminate();
      } catch {
        // ignore
      }
    }
    this.activeConnections.clear();

    if (this.server) {
      const srv = this.server;
      this.server = null;
      srv.closeAllConnections?.();
      await new Promise((resolve) => srv.close(() => resolve()));
    }
  }
}
