import type { ProxyStats } from '../types';

interface KpiStripProps {
  stats: ProxyStats | null;
  loading: boolean;
}

export function KpiStrip({ stats, loading }: KpiStripProps) {
  if (loading && !stats) {
    return (
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
        {[0, 1, 2, 3, 4].map((i) => (
          <div
            key={i}
            className="bg-white border border-slate-200 rounded-lg p-5 h-[106px] animate-pulse"
          >
            <div className="h-3.5 w-28 bg-slate-200 rounded mb-4" />
            <div className="h-7 w-24 bg-slate-200 rounded mb-2" />
            <div className="h-3 w-36 bg-slate-100 rounded" />
          </div>
        ))}
      </div>
    );
  }

  const healthyNodes = stats?.healthyNodes ?? 0;
  const totalNodes = stats?.totalNodes ?? 0;
  const clientConnections = stats?.clientConnections ?? 0;
  const uniqueIps = stats?.uniqueIps ?? 0;
  const totalQps = stats?.totalQps ?? 0;
  const pendingRequests = stats?.pendingRequests ?? 0;
  const rateQueueLength = stats?.rateQueueLength ?? 0;
  const cacheHits = stats?.cache.hits ?? 0;
  const cacheCoalesced = stats?.cache.coalesced ?? 0;

  const maxValidatedSeq = stats?.maxValidatedSeq ?? null;
  const minValidatedSeq = stats?.minValidatedSeq ?? null;
  const safeValidatedSeq = stats?.safeValidatedSeq ?? null;
  const sampledLedgerNodes = stats?.sampledLedgerNodes ?? 0;
  const maxLagSpread =
    maxValidatedSeq !== null && minValidatedSeq !== null
      ? Math.max(0, maxValidatedSeq - minValidatedSeq)
      : null;

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
      <div className="bg-white border border-slate-200 rounded-lg p-5">
        <div className="text-xs font-medium text-slate-500">Healthy Upstream Nodes</div>
        <div className="mt-2 flex items-baseline gap-2 font-mono tabular-nums">
          <span className="text-2xl font-semibold text-slate-900">{healthyNodes}</span>
          <span className="text-sm text-slate-400">/ {totalNodes}</span>
        </div>
        <div className="mt-1.5 text-xs text-slate-500 font-mono tabular-nums">
          {totalNodes - healthyNodes === 0
            ? 'All pools nominal'
            : `${totalNodes - healthyNodes} degraded or open`}
        </div>
      </div>

      <div className="bg-white border border-slate-200 rounded-lg p-5">
        <div className="text-xs font-medium text-slate-500">Cluster Ledger Height</div>
        <div className="mt-2 flex items-baseline gap-2 font-mono tabular-nums">
          <span className="text-2xl font-semibold text-slate-900">
            {maxValidatedSeq !== null ? `#${maxValidatedSeq}` : '—'}
          </span>
        </div>
        <div className="mt-1.5 text-xs text-slate-500 font-mono tabular-nums truncate">
          {maxValidatedSeq !== null
            ? `Safe #${safeValidatedSeq ?? maxValidatedSeq} · Δ${maxLagSpread ?? 0} · ${sampledLedgerNodes}/${totalNodes}`
            : `0/${totalNodes} nodes sampled`}
        </div>
      </div>

      <div className="bg-white border border-slate-200 rounded-lg p-5">
        <div className="text-xs font-medium text-slate-500">Client Connections & QPS</div>
        <div className="mt-2 flex items-baseline gap-3 font-mono tabular-nums">
          <span className="text-2xl font-semibold text-slate-900">{clientConnections}</span>
          <span className="text-xs text-slate-400">clients</span>
          <span className="text-slate-300" aria-hidden="true">/</span>
          <span className="text-lg font-semibold text-blue-600">{totalQps.toFixed(1)}</span>
          <span className="text-xs text-slate-400">req/s</span>
        </div>
        <div className="mt-1.5 text-xs text-slate-500 font-mono tabular-nums">
          {uniqueIps} unique source IP{uniqueIps === 1 ? '' : 's'}
        </div>
      </div>

      <div className="bg-white border border-slate-200 rounded-lg p-5">
        <div className="text-xs font-medium text-slate-500">Pool In-Flight & Queue</div>
        <div className="mt-2 flex items-baseline gap-3 font-mono tabular-nums">
          <span className="text-2xl font-semibold text-slate-900">{pendingRequests}</span>
          <span className="text-xs text-slate-400">pending</span>
          <span className="text-slate-300" aria-hidden="true">/</span>
          <span className="text-lg font-semibold text-slate-700">{rateQueueLength}</span>
          <span className="text-xs text-slate-400">queued</span>
        </div>
        <div className="mt-1.5 text-xs text-slate-500 font-mono tabular-nums">
          Next Proxy ID #{stats?.nextProxyId ?? 1}
        </div>
      </div>

      <div className="bg-white border border-slate-200 rounded-lg p-5">
        <div className="text-xs font-medium text-slate-500">Micro-Cache & Singleflight</div>
        <div className="mt-2 flex items-baseline gap-3 font-mono tabular-nums">
          <span className="text-2xl font-semibold text-slate-900">{cacheHits}</span>
          <span className="text-xs text-slate-400">hits</span>
          <span className="text-slate-300" aria-hidden="true">/</span>
          <span className="text-lg font-semibold text-emerald-700">{cacheCoalesced}</span>
          <span className="text-xs text-slate-400">coalesced</span>
        </div>
        <div className="mt-1.5 text-xs text-slate-500 font-mono tabular-nums">
          {stats?.cache.ttlMs ?? 1500}ms TTL · {stats?.cache.size ?? 0} cached keys
        </div>
      </div>
    </div>
  );
}
