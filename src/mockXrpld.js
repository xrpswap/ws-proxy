import http from 'node:http';
import { WebSocketServer } from 'ws';
import { decodeXrplClassicAddressToHex, payloadMatchesBlacklistedTargets } from './xrplCodec.js';

export class MockXrpldNode {
  constructor({
    id = 'mock-node',
    roles = ['normal'],
    ledgerIndex = 92450100,
    clioMode = false,
    strictLedgerCheck = false,
    rejectSubscribe = false,
    blacklistedAddresses = [],
    blacklistResponseStyle = 'telBLACKLISTED',
    submitResponder = null,
  } = {}) {
    this.id = id;
    this.roles = roles;
    this.ledgerIndex = ledgerIndex;
    this.clioMode = clioMode;
    this.strictLedgerCheck = strictLedgerCheck;
    this.rejectSubscribe = rejectSubscribe;
    this.blacklistResponseStyle = blacklistResponseStyle;
    this.submitResponder = submitResponder;
    this.blacklistedTargets = new Map();
    for (const addr of blacklistedAddresses) {
      const accountIdHex = decodeXrplClassicAddressToHex(addr);
      if (accountIdHex) {
        this.blacklistedTargets.set(addr, { classic: addr, accountIdHex });
      }
    }
    this.server = null;
    this.wss = null;
    this.port = 0;
    this.online = false;
    this.loadWarningArmed = false;
    this.loadErrorArmed = null;
    this.receivedCommands = [];
    this.activeConnections = new Set();
    this.streamTimers = new Map();
    this.ledgerSubscribers = new Set();
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
        this.ledgerSubscribers.delete(ws);
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

  armLoadError(errorCode = 'tooBusy') {
    this.loadErrorArmed = errorCode;
  }

  buildLedgerEvent(seq = this.ledgerIndex) {
    return {
      type: 'ledgerClosed',
      ledger_index: seq,
      ledger_hash: `4BC50C9B0D8515D3EAAE1E74B29A95804346C491EE1A95BF25E4AAB8${String(seq).padStart(8, '0')}`,
      ledger_time: Math.floor(Date.now() / 1000),
      fee_base: 10,
      fee_ref: 10,
      reserve_base: 10000000,
      reserve_inc: 2000000,
      txn_count: 12,
      validated_ledgers: `32570-${seq}`,
      _mockNode: this.id,
    };
  }

  emitLedgerClosed(newSeq = this.ledgerIndex + 1) {
    this.ledgerIndex = newSeq;
    const evt = JSON.stringify(this.buildLedgerEvent(newSeq));
    for (const ws of this.ledgerSubscribers) {
      if (ws.readyState === 1) {
        ws.send(evt);
      }
    }
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

    if (this.loadErrorArmed) {
      const errorCode = this.loadErrorArmed;
      this.loadErrorArmed = null;
      const busyErr = {
        status: 'error',
        type: 'response',
        error: errorCode,
        error_code: errorCode === 'tooBusy' ? 9 : 10,
        error_message: 'The server is too busy to help you now.',
        request: req,
        ...(this.clioMode
          ? {
              warnings: [
                {
                  id: 2001,
                  message:
                    "This is a clio server. clio only serves validated data. If you want to talk to rippled, include 'ledger_index':'current' in your request",
                },
              ],
            }
          : {}),
        _mockNode: this.id,
      };
      if (req.id !== undefined && req.id !== null) busyErr.id = req.id;
      if (ws.readyState === 1) ws.send(JSON.stringify(busyErr));
      return;
    }

    if (this.loadWarningArmed) {
      this.loadWarningArmed = false;
      response.warning = 'load';
    }

    if (
      this.clioMode &&
      (req?.ledger_index === 'current' || cmd === 'ledger_current')
    ) {
      const clioErr = {
        status: 'error',
        type: 'response',
        error: 'invalidParams',
        error_message:
          "This is a clio server. clio only serves validated data. If you want to talk to rippled, include 'ledger_index':'current' in your request",
        _mockNode: this.id,
      };
      if (req.id !== undefined && req.id !== null) clioErr.id = req.id;
      if (ws.readyState === 1) ws.send(JSON.stringify(clioErr));
      return;
    }

    if (
      this.strictLedgerCheck &&
      typeof req?.ledger_index === 'number' &&
      req.ledger_index > this.ledgerIndex
    ) {
      const lgrErr = {
        status: 'error',
        type: 'response',
        error: 'lgrNotFound',
        error_message: 'ledgerNotFound',
        _mockNode: this.id,
      };
      if (req.id !== undefined && req.id !== null) lgrErr.id = req.id;
      if (ws.readyState === 1) ws.send(JSON.stringify(lgrErr));
      return;
    }

    switch (cmd) {
      case 'server_info':
        response.result = {
          info: {
            build_version: '2.3.0',
            ...(this.clioMode ? { clio_version: '2.2.0' } : {}),
            complete_ledgers: `32570-${this.ledgerIndex}`,
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
          ledger_index:
            typeof req.ledger_index === 'number' ? req.ledger_index : this.ledgerIndex,
          validated: true,
        };
        break;
      case 'account_lines':
        response.result = {
          account: req.account || 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
          lines: [],
          ledger_index:
            typeof req.ledger_index === 'number' ? req.ledger_index : this.ledgerIndex,
          validated: req.ledger_index !== 'current',
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
      case 'sign_and_submit': {
        if (typeof this.submitResponder === 'function') {
          const custom = this.submitResponder(req, this.id);
          if (custom) {
            if (req.id !== undefined && req.id !== null) custom.id = req.id;
            if (ws.readyState === 1) ws.send(JSON.stringify(custom));
            return;
          }
        }
        if (this.blacklistedTargets.size > 0) {
          for (const entry of this.blacklistedTargets.values()) {
            const singleMap = new Map([[entry.classic, entry]]);
            if (payloadMatchesBlacklistedTargets(req, singleMap)) {
              const blMsg = `Transaction involves a blacklisted address, object, or token (${entry.classic} on Xaman Blacklist.rippleflagged).`;
              const blResp =
                this.blacklistResponseStyle === 'rpc_error'
                  ? {
                      status: 'error',
                      type: 'response',
                      error: 'invalidTransaction',
                      error_message: blMsg,
                      _mockNode: this.id,
                    }
                  : {
                      status: 'success',
                      type: 'response',
                      result: {
                        accepted: false,
                        applied: false,
                        broadcast: false,
                        kept: false,
                        queued: false,
                        engine_result: 'telBLACKLISTED',
                        engine_result_code: -399,
                        engine_result_message: blMsg,
                        tx_blob: req.tx_blob,
                        tx_json: req.tx_json || {},
                      },
                      _mockNode: this.id,
                    };
              if (req.id !== undefined && req.id !== null) blResp.id = req.id;
              if (ws.readyState === 1) ws.send(JSON.stringify(blResp));
              return;
            }
          }
        }
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
      }
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
      case 'subscribe': {
        if (this.rejectSubscribe) {
          const errResp = {
            status: 'error',
            type: 'response',
            error: 'tooBusy',
            error_message: 'Mock subscribe rejected',
            _mockNode: this.id,
          };
          if (req.id !== undefined && req.id !== null) errResp.id = req.id;
          if (ws.readyState === 1) ws.send(JSON.stringify(errResp));
          return;
        }
        const evt = this.buildLedgerEvent(this.ledgerIndex);
        response.result = {
          ledger_index: evt.ledger_index,
          ledger_hash: evt.ledger_hash,
          ledger_time: evt.ledger_time,
          fee_base: evt.fee_base,
          fee_ref: evt.fee_ref,
          reserve_base: evt.reserve_base,
          reserve_inc: evt.reserve_inc,
          validated_ledgers: evt.validated_ledgers,
        };
        if (Array.isArray(req?.streams) && req.streams.includes('ledger')) {
          this.ledgerSubscribers.add(ws);
        }
        if (!this.streamTimers.has(ws)) {
          const timer = setInterval(() => {
            if (ws.readyState === 1) {
              this.ledgerIndex += 1;
              ws.send(JSON.stringify(this.buildLedgerEvent(this.ledgerIndex)));
            }
          }, 4000);
          if (typeof timer.unref === 'function') timer.unref();
          this.streamTimers.set(ws, timer);
        }
        break;
      }
      case 'unsubscribe': {
        this.ledgerSubscribers.delete(ws);
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
    this.ledgerSubscribers.clear();
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
