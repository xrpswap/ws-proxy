import { Activity, ArrowRight, Network, RefreshCw, Sliders, Terminal } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import { KpiStrip } from './components/KpiStrip';
import { NodeTopologyTable } from './components/NodeTopologyTable';
import { RoutingConfigView } from './components/RoutingConfigView';
import { WsWorkbench } from './components/WsWorkbench';
import type { ProxyStats } from './types';

type ConsoleTab = 'topology' | 'workbench' | 'routing';

function getPathRoute(): 'readonly' | 'dashboard' {
  if (typeof window === 'undefined') return 'readonly';
  return window.location.pathname.startsWith('/proxy-dashboard')
    ? 'dashboard'
    : 'readonly';
}

export default function App() {
  const [route, setRoute] = useState<'readonly' | 'dashboard'>(getPathRoute);
  const [activeTab, setActiveTab] = useState<ConsoleTab>('topology');
  const [stats, setStats] = useState<ProxyStats | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const navigate = (target: 'readonly' | 'dashboard') => {
    const targetPath = target === 'dashboard' ? '/proxy-dashboard' : '/';
    if (window.location.pathname !== targetPath) {
      window.history.pushState({}, '', targetPath);
    }
    setRoute(target);
  };

  useEffect(() => {
    const onPopState = () => {
      setRoute(getPathRoute());
    };
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, []);

  const fetchStats = useCallback(async () => {
    try {
      const res = await fetch('/api/stats');
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const data: ProxyStats = await res.json();
      setStats(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load telemetry');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void fetchStats();
    const timer = setInterval(() => {
      void fetchStats();
    }, 1500);
    return () => clearInterval(timer);
  }, [fetchStats]);

  if (route === 'readonly') {
    return (
      <div className="min-h-screen bg-slate-50 text-slate-900 flex flex-col">
        <header className="bg-white border-b border-slate-200 px-6 py-4">
          <div className="max-w-7xl mx-auto flex items-center justify-between gap-8">
            <a
              href="/"
              onClick={(e) => {
                e.preventDefault();
                navigate('readonly');
              }}
              className="text-lg font-bold tracking-tight text-slate-900 whitespace-nowrap shrink-0 font-display"
            >
              xrpld-ws-proxy
            </a>

            <nav className="hidden md:flex items-center gap-6 text-sm font-medium text-slate-600">
              <a
                href="/"
                onClick={(e) => {
                  e.preventDefault();
                  navigate('readonly');
                }}
                className="text-slate-900 whitespace-nowrap shrink-0"
              >
                Overview
              </a>
              <a
                href="/proxy-dashboard"
                onClick={(e) => {
                  e.preventDefault();
                  setActiveTab('topology');
                  navigate('dashboard');
                }}
                className="hover:text-slate-900 transition-colors whitespace-nowrap shrink-0"
              >
                Node Topology
              </a>
              <a
                href="/proxy-dashboard"
                onClick={(e) => {
                  e.preventDefault();
                  setActiveTab('workbench');
                  navigate('dashboard');
                }}
                className="hover:text-slate-900 transition-colors whitespace-nowrap shrink-0"
              >
                WS Workbench
              </a>
              <a
                href="/proxy-dashboard"
                onClick={(e) => {
                  e.preventDefault();
                  setActiveTab('routing');
                  navigate('dashboard');
                }}
                className="hover:text-slate-900 transition-colors whitespace-nowrap shrink-0"
              >
                Routing Rules
              </a>
            </nav>

            <div className="shrink-0">
              <button
                type="button"
                onClick={() => navigate('dashboard')}
                className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 transition-colors whitespace-nowrap shrink-0 cursor-pointer"
              >
                <span>Control Console</span>
                <ArrowRight className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>
        </header>

        <main className="flex-1 max-w-7xl w-full mx-auto px-6 py-8 space-y-6">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <h1 className="text-2xl font-bold tracking-tight text-slate-900">
                Read-Only Proxy Telemetry
              </h1>
              <p className="text-sm text-slate-500 mt-1">
                Live operational metrics across role-routed upstream pools, rate limiters, and micro-cache
              </p>
            </div>

            <div className="text-xs font-mono text-slate-500 tabular-nums">
              {error ? (
                <span className="text-red-700 font-medium">Telemetry error: {error}</span>
              ) : (
                <span>Polling /api/stats every 1500ms</span>
              )}
            </div>
          </div>

          <KpiStrip stats={stats} loading={loading} />

          <NodeTopologyTable
            nodes={stats?.nodes ?? []}
            maxValidatedSeq={stats?.maxValidatedSeq}
            maxSafeLedgerLag={stats?.limits?.maxSafeLedgerLag}
            totalRetries={stats?.totalRetries}
            blacklistBypassed={stats?.blacklistBypassed}
            lastRetry={stats?.lastRetry}
            readOnly
          />
        </main>
      </div>
    );
  }

  const activeViewLabel =
    activeTab === 'topology'
      ? 'Node Topology'
      : activeTab === 'workbench'
        ? 'WS Workbench'
        : 'Routing & Config';

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 flex flex-col lg:flex-row">
      {/* Sidebar */}
      <aside className="lg:w-[256px] shrink-0 bg-white border-b lg:border-b-0 lg:border-r border-slate-200 flex flex-col">
        <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between">
          <a
            href="/"
            onClick={(e) => {
              e.preventDefault();
              navigate('readonly');
            }}
            className="text-base font-bold tracking-tight text-slate-900 font-display whitespace-nowrap"
          >
            xrpld-ws-proxy
          </a>
        </div>

        <nav className="p-3 flex lg:flex-col gap-1 overflow-x-auto">
          <button
            type="button"
            onClick={() => setActiveTab('topology')}
            className={`flex items-center gap-2.5 px-3 py-2.5 text-sm font-medium rounded-md transition-colors whitespace-nowrap shrink-0 cursor-pointer ${
              activeTab === 'topology'
                ? 'bg-blue-50 text-blue-700'
                : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
            }`}
          >
            <Network className="w-4 h-4 shrink-0" />
            <span>Node Topology</span>
          </button>

          <button
            type="button"
            onClick={() => setActiveTab('workbench')}
            className={`flex items-center gap-2.5 px-3 py-2.5 text-sm font-medium rounded-md transition-colors whitespace-nowrap shrink-0 cursor-pointer ${
              activeTab === 'workbench'
                ? 'bg-blue-50 text-blue-700'
                : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
            }`}
          >
            <Terminal className="w-4 h-4 shrink-0" />
            <span>WS Workbench</span>
          </button>

          <button
            type="button"
            onClick={() => setActiveTab('routing')}
            className={`flex items-center gap-2.5 px-3 py-2.5 text-sm font-medium rounded-md transition-colors whitespace-nowrap shrink-0 cursor-pointer ${
              activeTab === 'routing'
                ? 'bg-blue-50 text-blue-700'
                : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
            }`}
          >
            <Sliders className="w-4 h-4 shrink-0" />
            <span>Routing & Config</span>
          </button>
        </nav>

        <div className="mt-auto p-4 border-t border-slate-200 hidden lg:block space-y-3">
          <div className="text-xs text-slate-500 space-y-1 font-mono tabular-nums">
            <div className="flex justify-between">
              <span>Upstream Mode</span>
              <span className="text-slate-900 font-medium">
                {stats?.mockMode ? 'Mock Cluster' : 'Public/Live'}
              </span>
            </div>
            <div className="flex justify-between">
              <span>Port</span>
              <span className="text-slate-900 font-medium">
                {stats?.server.port ?? 3000}
              </span>
            </div>
          </div>

          <a
            href="/"
            onClick={(e) => {
              e.preventDefault();
              navigate('readonly');
            }}
            className="block w-full text-center px-3 py-2 text-xs font-medium text-slate-700 bg-slate-100 hover:bg-slate-200 rounded-md transition-colors whitespace-nowrap"
          >
            Read-Only View
          </a>
        </div>
      </aside>

      {/* Main Content */}
      <div className="flex-1 min-w-0 flex flex-col">
        {/* Top Metrics Overview Bar */}
        <header className="bg-white border-b border-slate-200 px-6 py-3.5 flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-2 text-sm">
            <span className="text-slate-500">Control Console</span>
            <span className="text-slate-300" aria-hidden="true">/</span>
            <span className="font-semibold text-slate-900">{activeViewLabel}</span>
          </div>

          <div className="flex flex-wrap items-center gap-5 text-xs font-mono tabular-nums text-slate-600">
            <div className="flex items-center gap-1.5">
              <Activity className="w-3.5 h-3.5 text-emerald-600" />
              <span>
                Healthy {stats?.healthyNodes ?? 0}/{stats?.totalNodes ?? 0}
              </span>
            </div>
            <div>
              Ledger{' '}
              <span className="text-slate-900 font-medium">
                {stats?.maxValidatedSeq != null ? `#${stats.maxValidatedSeq}` : '—'}
              </span>
            </div>
            <div>Clients {stats?.clientConnections ?? 0}</div>
            <div>QPS {(stats?.totalQps ?? 0).toFixed(1)}</div>
            <div>Pending {stats?.pendingRequests ?? 0}</div>
            <button
              type="button"
              onClick={() => void fetchStats()}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 font-sans text-xs font-medium text-slate-700 bg-white border border-slate-200 rounded-md hover:bg-slate-50 cursor-pointer whitespace-nowrap shrink-0"
            >
              <RefreshCw className="w-3 h-3" />
              <span>Refresh</span>
            </button>
          </div>
        </header>

        <main className="flex-1 p-6 space-y-6 max-w-[1440px] w-full">
          {error && (
            <div className="px-4 py-3 bg-red-50 border border-red-200 rounded-md text-xs font-medium text-red-800">
              Telemetry request failed: {error}
            </div>
          )}

          {activeTab === 'topology' && (
            <>
              <KpiStrip stats={stats} loading={loading} />
              <NodeTopologyTable
                nodes={stats?.nodes ?? []}
                maxValidatedSeq={stats?.maxValidatedSeq}
                maxSafeLedgerLag={stats?.limits?.maxSafeLedgerLag}
                totalRetries={stats?.totalRetries}
                blacklistBypassed={stats?.blacklistBypassed}
                lastRetry={stats?.lastRetry}
                onActionComplete={() => void fetchStats()}
              />
            </>
          )}

          {activeTab === 'workbench' && <WsWorkbench />}

          {activeTab === 'routing' && <RoutingConfigView stats={stats} />}
        </main>
      </div>
    </div>
  );
}
