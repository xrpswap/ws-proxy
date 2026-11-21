import { AlertTriangle, CheckCircle2, PauseCircle, Power, RefreshCw, ShieldAlert, Zap } from 'lucide-react';
import { useState } from 'react';
import type { NodeStats, ProxyStats } from '../types';

interface NodeTopologyTableProps {
  nodes: NodeStats[];
  maxValidatedSeq?: number | null;
  maxSafeLedgerLag?: number;
  totalRetries?: number;
  blacklistBypassed?: number;
  lastRetry?: ProxyStats['lastRetry'];
  readOnly?: boolean;
  onActionComplete?: () => void;
}

export function NodeTopologyTable({
  nodes,
  maxValidatedSeq: maxValidatedSeqProp,
  maxSafeLedgerLag = 15,
  totalRetries = 0,
  blacklistBypassed = 0,
  lastRetry = null,
  readOnly = false,
  onActionComplete,
}: NodeTopologyTableProps) {
  const [busyNode, setBusyNode] = useState<string | null>(null);
  const [probingLedgers, setProbingLedgers] = useState<boolean>(false);
  const [actionError, setActionError] = useState<string | null>(null);

  const computedMaxSeq = nodes.reduce<number | null>((max, n) => {
    if (typeof n.lastValidatedSeq !== 'number') return max;
    return max === null || n.lastValidatedSeq > max ? n.lastValidatedSeq : max;
  }, null);
  const maxValidatedSeq =
    maxValidatedSeqProp !== undefined ? maxValidatedSeqProp : computedMaxSeq;

  const probeLedgers = async () => {
    setProbingLedgers(true);
    setActionError(null);
    try {
      const res = await fetch('/api/nodes/probe-ledger', { method: 'POST' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Probe failed (${res.status})`);
      }
      onActionComplete?.();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : 'Ledger probe failed');
    } finally {
      setProbingLedgers(false);
    }
  };

  const runNodeAction = async (
    nodeId: string,
    endpoint: string,
    body?: Record<string, unknown>,
  ) => {
    setBusyNode(nodeId);
    setActionError(null);
    try {
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: body ? { 'Content-Type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || `Request failed (${res.status})`);
      }
      onActionComplete?.();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : 'Action failed');
    } finally {
      setBusyNode(null);
    }
  };

  const renderStateLabel = (node: NodeStats) => {
    if (!node.isOnline && node.circuitState !== 'OPEN') {
      return (
        <div className="inline-flex items-center gap-1.5 text-xs font-medium text-red-700 whitespace-nowrap">
          <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
          <span>OFFLINE</span>
        </div>
      );
    }
    if (node.circuitState === 'OPEN') {
      return (
        <div className="inline-flex items-center gap-1.5 text-xs font-medium text-red-700 whitespace-nowrap">
          <ShieldAlert className="w-3.5 h-3.5 shrink-0" />
          <span className="font-mono tabular-nums">
            OPEN ({node.cooldownRemainingSec}s)
          </span>
        </div>
      );
    }
    if (node.circuitState === 'HALF_OPEN') {
      return (
        <div className="inline-flex items-center gap-1.5 text-xs font-medium text-amber-700 whitespace-nowrap">
          <PauseCircle className="w-3.5 h-3.5 shrink-0" />
          <span>HALF_OPEN</span>
        </div>
      );
    }
    return (
      <div className="inline-flex items-center gap-1.5 text-xs font-medium text-emerald-700 whitespace-nowrap">
        <CheckCircle2 className="w-3.5 h-3.5 shrink-0" />
        <span>CLOSED</span>
      </div>
    );
  };

  return (
    <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
      <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold text-slate-900">
            Upstream Node Pools & Circuit State
          </h2>
          <p className="text-xs text-slate-500 mt-0.5">
            Role capabilities, stateful slot allocation, weighted token buckets, and failover state
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-4 shrink-0">
          {actionError && (
            <div className="text-xs text-red-700 font-medium">{actionError}</div>
          )}
          <div className="text-xs font-mono tabular-nums text-slate-600 whitespace-nowrap">
            Failover retries:{' '}
            <span className={totalRetries > 0 ? 'text-slate-900 font-semibold' : 'text-slate-500'}>
              {totalRetries}
            </span>
            {' · '}BL skipped:{' '}
            <span
              className={blacklistBypassed > 0 ? 'text-slate-900 font-semibold' : 'text-slate-500'}
            >
              {blacklistBypassed}
            </span>
            {lastRetry && (
              <span className="text-slate-500">
                {' '}
                ({lastRetry.reason}: {lastRetry.fromNodeId} → {lastRetry.toNodeId})
              </span>
            )}
          </div>
          {!readOnly && (
            <button
              type="button"
              disabled={probingLedgers}
              onClick={() => void probeLedgers()}
              title="Query server_info across online upstream pools to refresh validated ledger heights"
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-700 bg-white border border-slate-200 rounded-md hover:bg-slate-50 transition-colors whitespace-nowrap shrink-0 cursor-pointer disabled:opacity-40"
            >
              <RefreshCw className={`w-3 h-3 ${probingLedgers ? 'animate-spin' : ''}`} />
              <span>{probingLedgers ? 'Probing...' : 'Probe Ledgers'}</span>
            </button>
          )}
        </div>
      </div>

      {nodes.length === 0 ? (
        <div className="p-8 text-center text-sm text-slate-500">
          No upstream nodes configured.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50/70 text-[12px] font-medium text-slate-500">
                <th className="py-3 px-4 whitespace-nowrap">Node / Endpoint</th>
                <th className="py-3 px-4 whitespace-nowrap">Roles</th>
                <th className="py-3 px-4 whitespace-nowrap">Circuit Status</th>
                <th className="py-3 px-4 whitespace-nowrap text-right">Ledger Height</th>
                <th className="py-3 px-4 whitespace-nowrap text-right">Pool WS</th>
                <th className="py-3 px-4 whitespace-nowrap text-right">Path Slots</th>
                <th className="py-3 px-4 whitespace-nowrap text-right">Sub Slots</th>
                <th className="py-3 px-4 whitespace-nowrap text-right">Tokens / Rate</th>
                <th className="py-3 px-4 whitespace-nowrap text-right">Req / Err / QPS</th>
                {!readOnly && (
                  <th className="py-3 px-4 whitespace-nowrap text-right">Fault Injection</th>
                )}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 text-sm">
              {nodes.map((node) => {
                const isBusy = busyNode === node.id;
                const isMockOnline = node.mockOnline ?? node.isOnline;
                return (
                  <tr key={node.id} className="hover:bg-slate-50/80 transition-colors">
                    <td className="py-3 px-4 align-middle">
                      <div className="font-medium text-slate-900 whitespace-nowrap">
                        {node.id}
                      </div>
                      <div className="text-xs text-slate-500 font-mono truncate max-w-[210px]">
                        {node.wsUrl}
                      </div>
                    </td>

                    <td className="py-3 px-4 align-middle">
                      <div className="text-xs font-mono text-slate-700 whitespace-nowrap">
                        {node.roles.join(' · ')}
                      </div>
                    </td>

                    <td className="py-3 px-4 align-middle">
                      {renderStateLabel(node)}
                      {node.isSlowedDown && (
                        <div className="text-[11px] font-mono text-amber-700 mt-0.5 whitespace-nowrap">
                          Backpressure {node.rateMultiplier}x
                        </div>
                      )}
                      {(node.currentRejectedCooldownSec ?? 0) > 0 && (
                        <div className="text-[11px] font-mono text-amber-700 mt-0.5 whitespace-nowrap tabular-nums">
                          Clio no-current ({node.currentRejectedCooldownSec}s)
                        </div>
                      )}
                      {((node.blacklistedCount ?? 0) > 0 || (node.blacklistRejections ?? 0) > 0) && (
                        <div className="text-[11px] font-mono text-slate-600 mt-0.5 whitespace-nowrap tabular-nums">
                          Blacklist: {node.blacklistedCount ?? 0} cached · {node.blacklistRejections ?? 0} retried / {node.blacklistBypassed ?? 0} skipped
                        </div>
                      )}
                      {(node.retriedOut ?? 0) > (node.blacklistRejections ?? 0) && (
                        <div className="text-[11px] font-mono text-slate-500 mt-0.5 whitespace-nowrap tabular-nums">
                          Failover retries: {(node.retriedOut ?? 0) - (node.blacklistRejections ?? 0)}
                        </div>
                      )}
                    </td>

                    <td className="py-3 px-4 align-middle text-right font-mono tabular-nums text-xs whitespace-nowrap">
                      {typeof node.lastValidatedSeq === 'number' ? (
                        <>
                          <div className="text-slate-900 font-medium">
                            #{node.lastValidatedSeq}
                          </div>
                          <div className="text-[11px] mt-0.5">
                            {(() => {
                              const lag =
                                maxValidatedSeq !== null
                                  ? Math.max(0, maxValidatedSeq - node.lastValidatedSeq)
                                  : null;
                              const lagLabel =
                                lag === null
                                  ? null
                                  : lag === 0
                                    ? { text: 'Synced', cls: 'text-emerald-700' }
                                    : lag <= maxSafeLedgerLag
                                      ? { text: `-${lag} safe`, cls: 'text-slate-500' }
                                      : { text: `-${lag} lag`, cls: 'text-amber-700' };
                              return (
                                <>
                                  {lagLabel && (
                                    <span className={lagLabel.cls}>{lagLabel.text}</span>
                                  )}
                                  {node.clioVersion && (
                                    <span className="text-slate-400">
                                      {lagLabel ? ' · ' : ''}Clio
                                    </span>
                                  )}
                                </>
                              );
                            })()}
                          </div>
                        </>
                      ) : (
                        <span className="text-slate-400">—</span>
                      )}
                    </td>

                    <td className="py-3 px-4 align-middle text-right font-mono tabular-nums text-xs text-slate-700 whitespace-nowrap">
                      {node.activePoolConns} / {node.poolSize}
                    </td>

                    <td className="py-3 px-4 align-middle text-right font-mono tabular-nums text-xs text-slate-700 whitespace-nowrap">
                      {node.maxPathConns === 0 ? (
                        <span className="text-slate-400">0</span>
                      ) : (
                        `${node.activePathConns} / ${node.maxPathConns}`
                      )}
                    </td>

                    <td className="py-3 px-4 align-middle text-right font-mono tabular-nums text-xs text-slate-700 whitespace-nowrap">
                      {node.maxSubscribeConns === 0 ? (
                        <span className="text-slate-400">0</span>
                      ) : (
                        `${node.activeSubscribeConns} / ${node.maxSubscribeConns}`
                      )}
                    </td>

                    <td className="py-3 px-4 align-middle text-right font-mono tabular-nums text-xs whitespace-nowrap">
                      <div className="text-slate-900 font-medium">
                        {node.tokens.toFixed(1)} / {node.burst}
                      </div>
                      <div
                        className={
                          node.isSlowedDown ? 'text-amber-700' : 'text-slate-500'
                        }
                      >
                        {node.effectiveRatePerSec}/s
                      </div>
                    </td>

                    <td className="py-3 px-4 align-middle text-right font-mono tabular-nums text-xs whitespace-nowrap">
                      <span className="text-slate-900">{node.totalRequests}</span>
                      <span className="text-slate-300 mx-1">/</span>
                      <span
                        className={
                          node.totalErrors > 0 ? 'text-red-600 font-medium' : 'text-slate-500'
                        }
                      >
                        {node.totalErrors}
                      </span>
                      <span className="text-slate-300 mx-1">/</span>
                      <span className="text-blue-600 font-medium">{node.qps.toFixed(1)}</span>
                    </td>

                    {!readOnly && (
                      <td className="py-3 px-4 align-middle text-right whitespace-nowrap">
                        <div className="inline-flex items-center justify-end gap-1.5">
                          <button
                            type="button"
                            disabled={isBusy || !node.mockMode}
                            onClick={() =>
                              runNodeAction(node.id, `/api/nodes/${node.id}/toggle-online`)
                            }
                            title={
                              node.mockMode
                                ? isMockOnline
                                  ? 'Stop mock upstream node'
                                  : 'Start mock upstream node'
                                : 'Only available in mock mode'
                            }
                            className={`inline-flex items-center gap-1 px-2.5 py-1.5 text-xs font-medium rounded border transition-colors whitespace-nowrap shrink-0 cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
                              isMockOnline
                                ? 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
                                : 'border-emerald-200 bg-emerald-50 text-emerald-700 hover:bg-emerald-100'
                            }`}
                          >
                            <Power className="w-3 h-3" />
                            <span>{isMockOnline ? 'Stop' : 'Start'}</span>
                          </button>

                          <button
                            type="button"
                            disabled={isBusy}
                            onClick={() =>
                              runNodeAction(node.id, `/api/nodes/${node.id}/circuit`, {
                                state: node.circuitState === 'OPEN' ? 'CLOSED' : 'OPEN',
                              })
                            }
                            className={`inline-flex items-center gap-1 px-2.5 py-1.5 text-xs font-medium rounded border transition-colors whitespace-nowrap shrink-0 cursor-pointer disabled:opacity-40 ${
                              node.circuitState === 'OPEN'
                                ? 'border-blue-200 bg-blue-50 text-blue-700 hover:bg-blue-100'
                                : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
                            }`}
                          >
                            <RefreshCw className="w-3 h-3" />
                            <span>{node.circuitState === 'OPEN' ? 'Reset' : 'Trip'}</span>
                          </button>

                          <button
                            type="button"
                            disabled={isBusy || !node.mockMode || !isMockOnline}
                            onClick={() =>
                              runNodeAction(node.id, `/api/nodes/${node.id}/load-warning`)
                            }
                            title="Inject warning: load in next response"
                            className={`inline-flex items-center gap-1 px-2.5 py-1.5 text-xs font-medium rounded border transition-colors whitespace-nowrap shrink-0 cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
                              node.loadWarningArmed
                                ? 'border-amber-300 bg-amber-50 text-amber-800'
                                : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
                            }`}
                          >
                            <Zap className="w-3 h-3" />
                            <span>{node.loadWarningArmed ? 'Armed' : 'Load'}</span>
                          </button>
                        </div>
                      </td>
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
