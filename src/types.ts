export interface NodeStats {
  id: string;
  wsUrl: string;
  roles: string[];
  circuitState: 'CLOSED' | 'OPEN' | 'HALF_OPEN';
  isOnline: boolean;
  mockMode?: boolean;
  mockOnline?: boolean;
  loadWarningArmed?: boolean;
  cooldownRemainingSec: number;
  consecutiveFailures: number;
  poolSize: number;
  activePoolConns: number;
  activePathConns: number;
  maxPathConns: number;
  activeSubscribeConns: number;
  maxSubscribeConns: number;
  tokens: number;
  burst: number;
  baseRatePerSec: number;
  effectiveRatePerSec: number;
  rateMultiplier: number;
  isSlowedDown: boolean;
  totalRequests: number;
  totalErrors: number;
  qps: number;
  lastLoadWarningAt: number | null;
}

export interface ProxyStats {
  clientConnections: number;
  uniqueIps: number;
  totalNodes: number;
  healthyNodes: number;
  totalQps: number;
  pendingRequests: number;
  nextProxyId: number;
  rateQueueLength: number;
  mockMode?: boolean;
  cache: {
    enabled: boolean;
    ttlMs: number;
    commands: string[];
    hits: number;
    misses: number;
    coalesced: number;
    size: number;
    inflight: number;
  };
  nodes: NodeStats[];
  routing: {
    pathCommands: string[];
    historyCommands: string[];
    submitCommands: string[];
    statefulCommands: string[];
    routeExplicitLedgerToHistory: boolean;
    defaultWeight: number;
    commandWeights: Record<string, number>;
  };
  limits: {
    requestTimeoutMs: number;
    nodeCooldownMs: number;
    maxPathTtlMs: number;
    queueMaxWaitMs: number;
    queueMaxLength: number;
    circuitBreakerThreshold: number;
    slowDownRecoveryMs: number;
    upstreamPingIntervalMs: number;
    reconnectBaseDelayMs: number;
    reconnectMaxDelayMs: number;
  };
  server: {
    host: string;
    port: number;
    maxPayloadBytes: number;
    maxConnectionsPerIp: number;
    trustProxy: boolean;
  };
}
