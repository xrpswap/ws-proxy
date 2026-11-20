import type { ProxyStats } from '../types';

interface RoutingConfigViewProps {
  stats: ProxyStats | null;
}

export function RoutingConfigView({ stats }: RoutingConfigViewProps) {
  if (!stats) {
    return (
      <div className="bg-white border border-slate-200 rounded-lg p-8 text-sm text-slate-500">
        Loading configuration...
      </div>
    );
  }

  const weightGroups: Record<number, string[]> = {};
  for (const [cmd, weight] of Object.entries(stats.routing.commandWeights)) {
    if (!weightGroups[weight]) weightGroups[weight] = [];
    weightGroups[weight].push(cmd);
  }

  const limitRows = [
    { label: 'Request Timeout', value: `${stats.limits.requestTimeoutMs} ms` },
    { label: 'Circuit Breaker Threshold', value: `${stats.limits.circuitBreakerThreshold} failures` },
    { label: 'Node Cooldown', value: `${stats.limits.nodeCooldownMs} ms` },
    { label: 'Slowdown Recovery Window', value: `${stats.limits.slowDownRecoveryMs} ms` },
    { label: 'Max Path TTL', value: `${stats.limits.maxPathTtlMs} ms` },
    { label: 'Rate Queue Max Wait', value: `${stats.limits.queueMaxWaitMs} ms` },
    { label: 'Rate Queue Max Length', value: `${stats.limits.queueMaxLength} requests` },
    { label: 'Upstream Keepalive Ping', value: `${stats.limits.upstreamPingIntervalMs} ms` },
    {
      label: 'Reconnect Backoff Range',
      value: `${stats.limits.reconnectBaseDelayMs} ms – ${stats.limits.reconnectMaxDelayMs} ms`,
    },
    { label: 'Max Safe Ledger Lag', value: `${stats.limits.maxSafeLedgerLag ?? 15} ledgers` },
    { label: 'Clio No-Current Cooldown', value: `${stats.limits.clioCooldownMs ?? 120000} ms` },
    { label: 'Cross-Node Ledger Retries', value: `${stats.limits.ledgerRetryMaxAttempts ?? 2} attempts` },
    { label: 'Max Connections / IP', value: `${stats.server.maxConnectionsPerIp}` },
    {
      label: 'Max WS Payload',
      value: `${Math.round(stats.server.maxPayloadBytes / 1024)} KB`,
    },
    {
      label: 'Trust X-Forwarded-For',
      value: stats.server.trustProxy ? 'Enabled' : 'Disabled',
    },
  ];

  return (
    <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
      <div className="lg:col-span-7 space-y-6">
        <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between gap-4">
            <div>
              <h2 className="text-base font-semibold text-slate-900">
                Role Command Mappings
              </h2>
              <p className="text-xs text-slate-500 mt-0.5">
                Active instruction routing matrix by backend node capability
              </p>
            </div>
            <div className="text-xs font-mono text-slate-700 whitespace-nowrap text-right">
              <div>
                Explicit Ledger → History:{' '}
                <span
                  className={
                    stats.routing.routeExplicitLedgerToHistory
                      ? 'text-emerald-700 font-semibold'
                      : 'text-slate-500'
                  }
                >
                  {stats.routing.routeExplicitLedgerToHistory ? 'ON' : 'OFF'}
                </span>
              </div>
              <div className="text-[11px] text-slate-500 mt-0.5">
                Safe Lag (ledger_closed):{' '}
                <span
                  className={
                    stats.routing.preferSafeLagForLedgerClosed
                      ? 'text-emerald-700 font-semibold'
                      : 'text-slate-500'
                  }
                >
                  {stats.routing.preferSafeLagForLedgerClosed ? 'ON' : 'OFF'}
                </span>
              </div>
            </div>
          </div>

          <div className="divide-y divide-slate-100">
            <div className="p-4 grid grid-cols-1 sm:grid-cols-4 gap-2">
              <div className="text-xs font-semibold text-slate-900">path</div>
              <div className="sm:col-span-3 font-mono text-xs text-slate-700 leading-relaxed">
                {stats.routing.pathCommands.join(' · ')}
              </div>
            </div>
            <div className="p-4 grid grid-cols-1 sm:grid-cols-4 gap-2">
              <div className="text-xs font-semibold text-slate-900">history</div>
              <div className="sm:col-span-3 font-mono text-xs text-slate-700 leading-relaxed">
                {stats.routing.historyCommands.join(' · ')}
                {stats.routing.routeExplicitLedgerToHistory && (
                  <span className="block text-slate-500 mt-1">
                    + non-stateful requests with explicit ledger_hash or numeric ledger_index
                  </span>
                )}
              </div>
            </div>
            <div className="p-4 grid grid-cols-1 sm:grid-cols-4 gap-2">
              <div className="text-xs font-semibold text-slate-900">submit</div>
              <div className="sm:col-span-3 font-mono text-xs text-slate-700 leading-relaxed">
                {stats.routing.submitCommands.join(' · ')}
              </div>
            </div>
            <div className="p-4 grid grid-cols-1 sm:grid-cols-4 gap-2">
              <div className="text-xs font-semibold text-slate-900">stateful (1:1)</div>
              <div className="sm:col-span-3 font-mono text-xs text-slate-700 leading-relaxed">
                {stats.routing.statefulCommands.join(' · ')}
              </div>
            </div>
            <div className="p-4 grid grid-cols-1 sm:grid-cols-4 gap-2">
              <div className="text-xs font-semibold text-slate-900">normal (default)</div>
              <div className="sm:col-span-3 text-xs text-slate-600">
                Fallback role for all remaining commands (e.g., server_info, fee, account_info, book_offers, ping, simulate)
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between gap-4">
            <div>
              <h2 className="text-base font-semibold text-slate-900">
                Token Weights & Read Micro-Cache
              </h2>
              <p className="text-xs text-slate-500 mt-0.5">
                Per-command token bucket deductions and Singleflight cache rules
              </p>
            </div>
            <div className="text-xs font-mono text-slate-600 whitespace-nowrap">
              Default Weight: {stats.routing.defaultWeight}
            </div>
          </div>

          <div className="divide-y divide-slate-100">
            {Object.keys(weightGroups)
              .map(Number)
              .sort((a, b) => a - b)
              .map((w) => (
                <div key={w} className="p-4 grid grid-cols-1 sm:grid-cols-4 gap-2">
                  <div className="text-xs font-mono font-semibold text-slate-900 tabular-nums">
                    Weight {w}
                  </div>
                  <div className="sm:col-span-3 font-mono text-xs text-slate-600 leading-relaxed">
                    {weightGroups[w].join(' · ')}
                  </div>
                </div>
              ))}
            <div className="p-4 grid grid-cols-1 sm:grid-cols-4 gap-2 bg-slate-50/60">
              <div className="text-xs font-semibold text-slate-900">ReadCache</div>
              <div className="sm:col-span-3 font-mono text-xs text-slate-700">
                {stats.cache.enabled ? 'Enabled' : 'Disabled'} · {stats.cache.ttlMs}ms TTL ·{' '}
                {stats.cache.commands.join(' · ')}
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="lg:col-span-5">
        <div className="bg-white border border-slate-200 rounded-lg overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-200">
            <h2 className="text-base font-semibold text-slate-900">
              Protection Thresholds & Limits
            </h2>
            <p className="text-xs text-slate-500 mt-0.5">
              Active runtime guardrails for timeouts, queues, and circuit breaking
            </p>
          </div>
          <div className="divide-y divide-slate-100">
            {limitRows.map((row) => (
              <div
                key={row.label}
                className="px-5 py-3 flex items-center justify-between gap-4 text-xs"
              >
                <span className="text-slate-600">{row.label}</span>
                <span className="font-mono font-medium text-slate-900 tabular-nums whitespace-nowrap">
                  {row.value}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
