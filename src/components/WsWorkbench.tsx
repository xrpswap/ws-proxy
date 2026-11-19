import { Play, Plug, Send, Trash2, Unplug, Zap } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';

interface FrameEntry {
  seq: number;
  ts: string;
  direction: 'OUTBOUND' | 'INBOUND' | 'SYSTEM';
  source: string;
  rttMs?: number;
  summary: string;
  payload: unknown;
}

interface PresetCommand {
  key: string;
  label: string;
  roleHint: string;
  payload: Record<string, unknown>;
}

const PRESET_COMMANDS: PresetCommand[] = [
  {
    key: 'server_info',
    label: 'server_info',
    roleHint: 'normal · cached',
    payload: { id: 'srv-1', command: 'server_info' },
  },
  {
    key: 'fee',
    label: 'fee',
    roleHint: 'normal · cached',
    payload: { id: 'fee-1', command: 'fee' },
  },
  {
    key: 'account_tx',
    label: 'account_tx',
    roleHint: 'history',
    payload: {
      id: 'hist-1',
      command: 'account_tx',
      account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
      limit: 5,
    },
  },
  {
    key: 'submit',
    label: 'submit',
    roleHint: 'submit',
    payload: {
      id: 'sub-1',
      command: 'submit',
      tx_blob: '1200002280000000240000000161400000000000000A',
    },
  },
  {
    key: 'ripple_path_find',
    label: 'ripple_path_find',
    roleHint: 'path · stateless',
    payload: {
      id: 'rpf-1',
      command: 'ripple_path_find',
      source_account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
      destination_account: 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh',
      destination_amount: '1000000',
    },
  },
  {
    key: 'path_find_create',
    label: 'path_find create',
    roleHint: 'path · stateful',
    payload: {
      id: 'pf-create',
      command: 'path_find',
      subcommand: 'create',
      source_account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
      destination_account: 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh',
      destination_amount: '1000000',
    },
  },
  {
    key: 'path_find_close',
    label: 'path_find close',
    roleHint: 'path · close',
    payload: {
      id: 'pf-close',
      command: 'path_find',
      subcommand: 'close',
    },
  },
  {
    key: 'subscribe',
    label: 'subscribe',
    roleHint: 'normal · stateful',
    payload: {
      id: 'ws-sub',
      command: 'subscribe',
      streams: ['ledger'],
    },
  },
  {
    key: 'unsubscribe',
    label: 'unsubscribe',
    roleHint: 'normal · stateful',
    payload: {
      id: 'ws-unsub',
      command: 'unsubscribe',
      streams: ['ledger'],
    },
  },
  {
    key: 'id_zero',
    label: 'id: 0 boundary',
    roleHint: 'normal · falsy id',
    payload: {
      id: 0,
      command: 'account_info',
      account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
      ledger_index: 'validated',
    },
  },
  {
    key: 'explicit_ledger',
    label: 'explicit ledger_index',
    roleHint: 'history · numeric',
    payload: {
      id: 'ledger-hist',
      command: 'account_info',
      account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
      ledger_index: 92450000,
    },
  },
];

function getDefaultWsUrl() {
  if (typeof window === 'undefined') return 'ws://localhost:3000';
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${proto}//${window.location.host}`;
}

function nowTimeLabel() {
  const d = new Date();
  return d.toISOString().slice(11, 23);
}

export function WsWorkbench() {
  const [endpointUrl, setEndpointUrl] = useState(getDefaultWsUrl);
  const [connStatus, setConnStatus] = useState<
    'disconnected' | 'connecting' | 'connected'
  >('disconnected');
  const [activePreset, setActivePreset] = useState<string>('server_info');
  const [payloadText, setPayloadText] = useState<string>(() =>
    JSON.stringify(PRESET_COMMANDS[0].payload, null, 2),
  );
  const [editorError, setEditorError] = useState<string | null>(null);
  const [frames, setFrames] = useState<FrameEntry[]>([]);
  const [frameFilter, setFrameFilter] = useState<
    'ALL' | 'OUTBOUND' | 'INBOUND' | 'SYSTEM'
  >('ALL');
  const [runningBatch, setRunningBatch] = useState<boolean>(false);

  const wsRef = useRef<WebSocket | null>(null);
  const seqRef = useRef<number>(1);
  const inflightStartsRef = useRef<Map<string, number>>(new Map());

  const appendFrame = (
    direction: FrameEntry['direction'],
    source: string,
    summary: string,
    payload: unknown,
    rttMs?: number,
  ) => {
    const entry: FrameEntry = {
      seq: seqRef.current++,
      ts: nowTimeLabel(),
      direction,
      source,
      summary,
      payload,
      rttMs,
    };
    setFrames((prev) => [entry, ...prev].slice(0, 150));
  };

  useEffect(() => {
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, []);

  const connect = () => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      return;
    }
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }

    setConnStatus('connecting');
    try {
      const ws = new WebSocket(endpointUrl.trim());
      wsRef.current = ws;

      ws.onopen = () => {
        if (wsRef.current !== ws) return;
        setConnStatus('connected');
        appendFrame('SYSTEM', 'Workbench', 'WebSocket connected', {
          endpoint: endpointUrl.trim(),
        });
      };

      ws.onmessage = (evt) => {
        if (wsRef.current !== ws) return;
        let parsed: Record<string, unknown>;
        try {
          parsed = JSON.parse(String(evt.data));
        } catch {
          appendFrame('INBOUND', 'Proxy', 'Raw text frame', String(evt.data));
          return;
        }

        let rttMs: number | undefined;
        if (parsed.id !== undefined && parsed.id !== null) {
          const idKey = JSON.stringify(parsed.id);
          const startedAt = inflightStartsRef.current.get(idKey);
          if (startedAt !== undefined) {
            rttMs = Math.round(performance.now() - startedAt);
            inflightStartsRef.current.delete(idKey);
          }
        }

        const summary =
          typeof parsed.type === 'string' && parsed.type !== 'response'
            ? `Stream push: ${parsed.type}`
            : parsed.status === 'error'
              ? `Error: ${String(parsed.error || 'unknown')}`
              : `Response${parsed._mockNode ? ` via ${String(parsed._mockNode)}` : ''}`;

        appendFrame('INBOUND', 'Proxy', summary, parsed, rttMs);
      };

      ws.onerror = () => {
        if (wsRef.current !== ws) return;
        appendFrame('SYSTEM', 'Workbench', 'WebSocket transport error', {
          endpoint: endpointUrl.trim(),
        });
      };

      ws.onclose = (evt) => {
        if (wsRef.current === ws) {
          wsRef.current = null;
          setConnStatus('disconnected');
        }
        appendFrame('SYSTEM', 'Workbench', `Disconnected (code ${evt.code})`, {
          code: evt.code,
          reason: evt.reason || 'closed',
        });
      };
    } catch (err) {
      setConnStatus('disconnected');
      appendFrame('SYSTEM', 'Workbench', 'Failed to open WebSocket', {
        error: err instanceof Error ? err.message : 'Invalid URL',
      });
    }
  };

  const disconnect = () => {
    if (wsRef.current) {
      wsRef.current.close(1000, 'User disconnected');
      wsRef.current = null;
    }
    setConnStatus('disconnected');
  };

  const handleSelectPreset = (preset: PresetCommand) => {
    setActivePreset(preset.key);
    setPayloadText(JSON.stringify(preset.payload, null, 2));
    setEditorError(null);
  };

  const handleFormatJson = () => {
    try {
      const parsed = JSON.parse(payloadText);
      setPayloadText(JSON.stringify(parsed, null, 2));
      setEditorError(null);
    } catch (err) {
      setEditorError(err instanceof Error ? err.message : 'Invalid JSON');
    }
  };

  const sendCurrentPayload = () => {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(payloadText);
      setEditorError(null);
    } catch (err) {
      setEditorError(err instanceof Error ? err.message : 'Invalid JSON');
      return;
    }

    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setEditorError('Connect to a proxy WebSocket endpoint first.');
      return;
    }

    if (parsed.id !== undefined && parsed.id !== null) {
      inflightStartsRef.current.set(JSON.stringify(parsed.id), performance.now());
    }
    ws.send(JSON.stringify(parsed));
    appendFrame(
      'OUTBOUND',
      'Active WS',
      `Command: ${String(parsed.command || 'unknown')}`,
      parsed,
    );
  };

  const runConcurrentIdTest = async () => {
    if (runningBatch) return;
    setRunningBatch(true);
    appendFrame('SYSTEM', 'Concurrent Test', 'Opening 2 independent sockets sending id: 1 simultaneously', {
      url: endpointUrl.trim(),
    });

    const openSocket = (label: string, payload: Record<string, unknown>) =>
      new Promise<{ label: string; response: Record<string, unknown>; rttMs: number }>(
        (resolve, reject) => {
          const sock = new WebSocket(endpointUrl.trim());
          const timeout = setTimeout(() => {
            sock.close();
            reject(new Error(`${label} timed out`));
          }, 8000);

          let sentAt = 0;
          sock.onopen = () => {
            sentAt = performance.now();
            appendFrame('OUTBOUND', label, `${String(payload.command)} (id: 1)`, payload);
            sock.send(JSON.stringify(payload));
          };
          sock.onmessage = (evt) => {
            clearTimeout(timeout);
            const rttMs = Math.round(performance.now() - sentAt);
            try {
              const data = JSON.parse(String(evt.data));
              appendFrame(
                'INBOUND',
                label,
                `Received id=${String(data.id)} (${data._mockNode ? String(data._mockNode) : 'proxy'})`,
                data,
                rttMs,
              );
              sock.close(1000);
              resolve({ label, response: data, rttMs });
            } catch (e) {
              sock.close();
              reject(e);
            }
          };
          sock.onerror = () => {
            clearTimeout(timeout);
            reject(new Error(`${label} connection failed`));
          };
        },
      );

    try {
      const [resA, resB] = await Promise.all([
        openSocket('Client A', {
          id: 1,
          command: 'account_info',
          account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
        }),
        openSocket('Client B', {
          id: 1,
          command: 'account_tx',
          account: 'rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh',
        }),
      ]);

      const aMatches = Boolean((resA.response.result as Record<string, unknown>)?.account_data);
      const bMatches = Array.isArray(
        (resB.response.result as Record<string, unknown>)?.transactions,
      );

      appendFrame(
        'SYSTEM',
        'Concurrent Test',
        aMatches && bMatches && resA.response.id === 1 && resB.response.id === 1
          ? 'PASS: Both clients received id=1 with isolated payloads'
          : 'Completed concurrent id=1 check',
        {
          clientA_id: resA.response.id,
          clientA_has_account_data: aMatches,
          clientB_id: resB.response.id,
          clientB_has_transactions: bMatches,
        },
      );
    } catch (err) {
      appendFrame('SYSTEM', 'Concurrent Test', 'Failed', {
        error: err instanceof Error ? err.message : 'Unknown error',
      });
    } finally {
      setRunningBatch(false);
    }
  };

  const runBurst12x = async () => {
    if (runningBatch) return;
    setRunningBatch(true);

    let sock = wsRef.current;
    let ownsSocket = false;
    if (!sock || sock.readyState !== WebSocket.OPEN) {
      ownsSocket = true;
      sock = new WebSocket(endpointUrl.trim());
      try {
        await new Promise<void>((resolve, reject) => {
          const t = setTimeout(() => reject(new Error('Connect timeout')), 5000);
          sock!.onopen = () => {
            clearTimeout(t);
            resolve();
          };
          sock!.onerror = () => {
            clearTimeout(t);
            reject(new Error('Connect error'));
          };
        });
      } catch (err) {
        appendFrame('SYSTEM', 'Burst 12x', 'Failed to connect', {
          error: err instanceof Error ? err.message : 'Connect error',
        });
        setRunningBatch(false);
        return;
      }
    }

    appendFrame(
      'SYSTEM',
      'Burst 12x',
      'Dispatching 12x weight-4 book_offers requests to observe spillover',
      { count: 12, weightPerRequest: 4, totalWeight: 48 },
    );

    const startMap = new Map<string, number>();
    const nodeHistogram: Record<string, number> = {};
    let received = 0;

    await new Promise<void>((resolve) => {
      const doneTimer = setTimeout(() => resolve(), 6000);

      const handleBurstMsg = (evt: MessageEvent) => {
        try {
          const data = JSON.parse(String(evt.data));
          if (typeof data.id === 'string' && data.id.startsWith('burst-')) {
            const started = startMap.get(data.id);
            const rttMs = started !== undefined ? Math.round(performance.now() - started) : undefined;
            const nodeLabel = data._mockNode ? String(data._mockNode) : data.error || 'proxy';
            nodeHistogram[nodeLabel] = (nodeHistogram[nodeLabel] || 0) + 1;
            appendFrame(
              'INBOUND',
              'Burst 12x',
              `${data.id} → ${nodeLabel}`,
              data,
              rttMs,
            );
            received += 1;
            if (received >= 12) {
              clearTimeout(doneTimer);
              if (ownsSocket) sock?.close(1000);
              else sock?.removeEventListener('message', handleBurstMsg);
              resolve();
            }
          }
        } catch {
          // ignore
        }
      };

      sock!.addEventListener('message', handleBurstMsg);

      for (let i = 1; i <= 12; i += 1) {
        const id = `burst-${i}`;
        const req = {
          id,
          command: 'book_offers',
          taker_gets: { currency: 'XRP' },
          taker_pays: {
            currency: 'USD',
            issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B',
          },
          limit: 5,
        };
        startMap.set(id, performance.now());
        appendFrame('OUTBOUND', 'Burst 12x', `book_offers (${id})`, req);
        sock!.send(JSON.stringify(req));
      }
    });

    appendFrame('SYSTEM', 'Burst 12x', 'Spillover distribution summary', {
      received,
      distribution: nodeHistogram,
    });
    setRunningBatch(false);
  };

  const filteredFrames = useMemo(() => {
    if (frameFilter === 'ALL') return frames;
    return frames.filter((f) => f.direction === frameFilter);
  }, [frames, frameFilter]);

  return (
    <div className="space-y-5">
      {/* Connection Bar */}
      <div className="bg-white border border-slate-200 rounded-lg p-4 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-1 min-w-[260px] items-center gap-3">
          <div className="flex items-center gap-2 text-xs font-medium whitespace-nowrap">
            <span
              className={`w-2 h-2 rounded-full ${
                connStatus === 'connected'
                  ? 'bg-emerald-600'
                  : connStatus === 'connecting'
                    ? 'bg-amber-500 animate-pulse'
                    : 'bg-slate-300'
              }`}
            />
            <span className="text-slate-700 capitalize">{connStatus}</span>
          </div>
          <input
            type="text"
            value={endpointUrl}
            onChange={(e) => setEndpointUrl(e.target.value)}
            placeholder="ws://localhost:3000 or wss://proxy.domain.com"
            aria-label="WebSocket Proxy Endpoint"
            className="flex-1 px-3 py-2 text-xs font-mono bg-slate-50 border border-slate-200 rounded-md text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-600/20 focus:border-blue-600"
          />
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {connStatus === 'connected' ? (
            <button
              type="button"
              onClick={disconnect}
              className="inline-flex items-center gap-1.5 px-3.5 py-2 text-xs font-medium rounded-md border border-slate-200 bg-white text-slate-700 hover:bg-slate-50 cursor-pointer whitespace-nowrap shrink-0"
            >
              <Unplug className="w-3.5 h-3.5" />
              <span>Disconnect</span>
            </button>
          ) : (
            <button
              type="button"
              onClick={connect}
              disabled={connStatus === 'connecting'}
              className="inline-flex items-center gap-1.5 px-3.5 py-2 text-xs font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 cursor-pointer whitespace-nowrap shrink-0 disabled:opacity-50"
            >
              <Plug className="w-3.5 h-3.5" />
              <span>{connStatus === 'connecting' ? 'Connecting...' : 'Connect'}</span>
            </button>
          )}

          <button
            type="button"
            disabled={runningBatch}
            onClick={runConcurrentIdTest}
            className="inline-flex items-center gap-1.5 px-3.5 py-2 text-xs font-medium rounded-md border border-slate-200 bg-white text-slate-800 hover:bg-slate-50 cursor-pointer whitespace-nowrap shrink-0 disabled:opacity-50"
          >
            <Play className="w-3.5 h-3.5 text-blue-600" />
            <span>Test Concurrent id: 1</span>
          </button>

          <button
            type="button"
            disabled={runningBatch}
            onClick={runBurst12x}
            className="inline-flex items-center gap-1.5 px-3.5 py-2 text-xs font-medium rounded-md border border-slate-200 bg-white text-slate-800 hover:bg-slate-50 cursor-pointer whitespace-nowrap shrink-0 disabled:opacity-50"
          >
            <Zap className="w-3.5 h-3.5 text-amber-600" />
            <span>Burst 12x</span>
          </button>
        </div>
      </div>

      {/* Main Split: Presets + Editor | Frame Inspector */}
      <div className="grid grid-cols-1 xl:grid-cols-12 gap-5 items-stretch">
        {/* Left: Presets & JSON Editor */}
        <div className="xl:col-span-5 bg-white border border-slate-200 rounded-lg flex flex-col overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-200">
            <h2 className="text-sm font-semibold text-slate-900">
              XRPL Command Templates
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              11 presets covering routing roles, stateful channels, and ID boundaries
            </p>
          </div>

          <div className="p-3 border-b border-slate-200 grid grid-cols-2 sm:grid-cols-3 gap-1.5 max-h-[240px] overflow-y-auto bg-slate-50/50">
            {PRESET_COMMANDS.map((preset) => {
              const active = activePreset === preset.key;
              return (
                <button
                  key={preset.key}
                  type="button"
                  onClick={() => handleSelectPreset(preset)}
                  className={`text-left px-2.5 py-2 rounded border transition-colors cursor-pointer ${
                    active
                      ? 'bg-blue-50 border-blue-300 text-blue-900'
                      : 'bg-white border-slate-200 text-slate-700 hover:bg-slate-50'
                  }`}
                >
                  <div className="text-xs font-mono font-medium truncate">
                    {preset.label}
                  </div>
                  <div className="text-[11px] text-slate-500 truncate mt-0.5">
                    {preset.roleHint}
                  </div>
                </button>
              );
            })}
          </div>

          <div className="p-4 flex-1 flex flex-col gap-3">
            <div className="flex items-center justify-between">
              <label
                htmlFor="ws-json-editor"
                className="text-xs font-medium text-slate-700"
              >
                JSON Request Payload
              </label>
              <button
                type="button"
                onClick={handleFormatJson}
                className="text-xs text-slate-600 hover:text-slate-900 cursor-pointer whitespace-nowrap"
              >
                Format JSON
              </button>
            </div>

            <textarea
              id="ws-json-editor"
              value={payloadText}
              onChange={(e) => setPayloadText(e.target.value)}
              rows={11}
              spellCheck={false}
              className="w-full flex-1 min-h-[220px] p-3 font-mono text-xs bg-slate-900 text-slate-100 rounded-md border border-slate-800 focus:outline-none focus:ring-2 focus:ring-blue-600/30"
            />

            {editorError && (
              <div className="text-xs text-red-700 font-medium">{editorError}</div>
            )}

            <div className="flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={sendCurrentPayload}
                disabled={connStatus !== 'connected'}
                className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-medium rounded-md bg-blue-600 text-white hover:bg-blue-700 cursor-pointer whitespace-nowrap shrink-0 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                <Send className="w-3.5 h-3.5" />
                <span>Send Frame</span>
              </button>
            </div>
          </div>
        </div>

        {/* Right: Frame Inspector */}
        <div className="xl:col-span-7 bg-white border border-slate-200 rounded-lg flex flex-col overflow-hidden min-h-[520px]">
          <div className="px-4 py-3 border-b border-slate-200 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-semibold text-slate-900">
                Frame Inspector
              </h2>
              <p className="text-xs text-slate-500 mt-0.5 font-mono tabular-nums">
                {frames.length} captured frame{frames.length === 1 ? '' : 's'}
              </p>
            </div>

            <div className="flex items-center gap-2">
              <div className="flex items-center gap-1 p-1 bg-slate-100 rounded-md">
                {(['ALL', 'OUTBOUND', 'INBOUND', 'SYSTEM'] as const).map((tab) => (
                  <button
                    key={tab}
                    type="button"
                    onClick={() => setFrameFilter(tab)}
                    className={`px-2.5 py-1 text-xs font-medium rounded transition-colors cursor-pointer whitespace-nowrap ${
                      frameFilter === tab
                        ? 'bg-white text-slate-900 shadow-xs'
                        : 'text-slate-600 hover:text-slate-900'
                    }`}
                  >
                    {tab === 'ALL' ? 'All' : tab}
                  </button>
                ))}
              </div>

              <button
                type="button"
                onClick={() => setFrames([])}
                className="inline-flex items-center gap-1 px-2.5 py-1.5 text-xs font-medium text-slate-600 hover:text-slate-900 border border-slate-200 rounded-md cursor-pointer whitespace-nowrap"
              >
                <Trash2 className="w-3.5 h-3.5" />
                <span>Clear</span>
              </button>
            </div>
          </div>

          <div className="flex-1 max-h-[540px] overflow-y-auto divide-y divide-slate-100 bg-slate-50/40">
            {filteredFrames.length === 0 ? (
              <div className="p-10 text-center text-sm text-slate-500">
                Connect to the proxy endpoint and send a preset command, or run{' '}
                <span className="font-medium text-slate-700">Test Concurrent id: 1</span>{' '}
                or <span className="font-medium text-slate-700">Burst 12x</span> to inspect frames.
              </div>
            ) : (
              filteredFrames.map((frame) => (
                <div key={frame.seq} className="p-3.5 hover:bg-white transition-colors">
                  <div className="flex flex-wrap items-center justify-between gap-2 text-xs mb-2">
                    <div className="flex items-center gap-2 font-mono">
                      <span
                        className={`font-semibold ${
                          frame.direction === 'OUTBOUND'
                            ? 'text-blue-700'
                            : frame.direction === 'INBOUND'
                              ? 'text-emerald-700'
                              : 'text-amber-700'
                        }`}
                      >
                        {frame.direction}
                      </span>
                      <span className="text-slate-300" aria-hidden="true">·</span>
                      <span className="text-slate-600">{frame.source}</span>
                      <span className="text-slate-300" aria-hidden="true">·</span>
                      <span className="text-slate-900 font-sans font-medium">
                        {frame.summary}
                      </span>
                    </div>

                    <div className="flex items-center gap-2 font-mono tabular-nums text-slate-500">
                      {frame.rttMs !== undefined && (
                        <>
                          <span className="text-slate-700 font-medium">
                            {frame.rttMs} ms
                          </span>
                          <span className="text-slate-300" aria-hidden="true">·</span>
                        </>
                      )}
                      <span>{frame.ts}</span>
                    </div>
                  </div>
                  <pre className="p-2.5 bg-slate-900 text-slate-100 rounded text-[11px] font-mono overflow-x-auto leading-relaxed">
                    {typeof frame.payload === 'string'
                      ? frame.payload
                      : JSON.stringify(frame.payload, null, 2)}
                  </pre>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
