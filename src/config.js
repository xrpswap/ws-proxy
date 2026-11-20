const DEFAULT_COMMAND_WEIGHTS = {
  // Weight 1: lightweight read & ping
  ping: 1,
  fee: 1,
  server_info: 1,
  server_state: 1,
  ledger_closed: 1,
  ledger_current: 1,
  random: 1,
  manifest: 1,

  // Weight 2: standard account / ledger / subscribe / submit
  account_info: 2,
  account_objects: 2,
  account_currencies: 2,
  account_channels: 2,
  ledger: 2,
  ledger_entry: 2,
  gateway_balances: 2,
  noripple_check: 2,
  subscribe: 2,
  unsubscribe: 2,
  submit: 2,
  submit_multisigned: 2,
  sign_and_submit: 2,

  // Weight 4: heavy collection / orderbook / simulation
  account_lines: 4,
  account_offers: 4,
  account_nfts: 4,
  book_offers: 4,
  ledger_data: 4,
  nft_info: 4,
  simulate: 4,

  // Weight 5: history & path finding
  tx: 5,
  account_tx: 5,
  tx_history: 5,
  transaction_entry: 5,
  nft_history: 5,
  book_changes: 5,
  ripple_path_find: 5,
  path_find: 5,
};

export const config = {
  server: {
    host: process.env.PROXY_HOST || '0.0.0.0',
    port: Number(process.env.PROXY_PORT || 3000),
    maxPayloadBytes: 1024 * 1024, // 1MB
    maxConnectionsPerIp: 20,
    trustProxy: process.env.TRUST_PROXY === 'true',
    heartbeatIntervalMs: 30000,
    statsLogIntervalMs: 60000,
  },
  limits: {
    requestTimeoutMs: 15000,
    nodeCooldownMs: 30000,
    maxPathTtlMs: 120000,
    queueMaxWaitMs: 1800,
    queueMaxLength: 200,
    circuitBreakerThreshold: 3,
    slowDownRecoveryMs: 15000,
    upstreamPingIntervalMs: 25000,
    reconnectBaseDelayMs: 500,
    reconnectMaxDelayMs: 15000,
    clioCooldownMs: 120000,
    ledgerRetryMaxAttempts: 2,
    maxSafeLedgerLag: 15,
  },
  routing: {
    pathCommands: ['path_find', 'ripple_path_find'],
    historyCommands: [
      'tx',
      'account_tx',
      'tx_history',
      'transaction_entry',
      'nft_history',
      'book_changes',
    ],
    submitCommands: ['submit', 'submit_multisigned', 'sign_and_submit'],
    statefulCommands: ['path_find', 'subscribe', 'unsubscribe'],
    routeExplicitLedgerToHistory: false,
    preferSafeLagForLedgerClosed: true,
    defaultWeight: 2,
    commandWeights: { ...DEFAULT_COMMAND_WEIGHTS },
  },
  cache: {
    enabled: true,
    ttlMs: 1500,
    commands: ['fee', 'server_info', 'ledger_closed'],
  },
  nodes: [
    {
      id: 'xrplcluster',
      wsUrl: 'wss://xrplcluster.com/',
      roles: ['normal', 'path'],
      poolSize: 1,
      maxPathConns: 2,
      maxSubscribeConns: 0,
      rateLimit: { ratePerSec: 5, burst: 15 },
    },
    {
      id: 'xrpl.ws',
      wsUrl: 'wss://xrpl.ws/',
      roles: ['normal', 'path'],
      poolSize: 1,
      maxPathConns: 2,
      maxSubscribeConns: 2,
      rateLimit: { ratePerSec: 5, burst: 16 },
    },
    {
      id: 'xrpl.link',
      wsUrl: 'wss://xrpl.link/',
      roles: ['normal', 'history'],
      poolSize: 1,
      maxPathConns: 0,
      maxSubscribeConns: 2,
      rateLimit: { ratePerSec: 5, burst: 20 },
    },
    {
      id: 'ripple-s1',
      wsUrl: 'wss://s1.ripple.com/',
      roles: ['normal', 'history', 'submit'],
      poolSize: 1,
      maxPathConns: 0,
      maxSubscribeConns: 0,
      rateLimit: { ratePerSec: 10, burst: 20 },
    },
    {
      id: 'ripple-s2',
      wsUrl: 'wss://s2.ripple.com/',
      roles: ['normal', 'history'],
      poolSize: 1,
      maxPathConns: 0,
      maxSubscribeConns: 5,
      rateLimit: { ratePerSec: 10, burst: 25 },
    },
    {
      id: 'honeycluster',
      wsUrl: 'wss://honeycluster.io/',
      roles: ['normal', 'history', 'submit'],
      poolSize: 1,
      maxPathConns: 0,
      maxSubscribeConns: 5,
      rateLimit: { ratePerSec: 10, burst: 20 },
    },
  ],
};

function cloneNode(node) {
  return {
    id: node.id,
    wsUrl: node.wsUrl,
    roles: Array.isArray(node.roles) ? [...node.roles] : ['normal'],
    poolSize: node.poolSize ?? 2,
    maxPathConns: node.maxPathConns ?? 0,
    maxSubscribeConns: node.maxSubscribeConns ?? 0,
    rateLimit: {
      ratePerSec: node.rateLimit?.ratePerSec ?? 15,
      burst: node.rateLimit?.burst ?? 30,
    },
  };
}

export function createConfig(overrides = {}) {
  return {
    server: {
      ...config.server,
      ...(overrides.server || {}),
    },
    limits: {
      ...config.limits,
      ...(overrides.limits || {}),
    },
    routing: {
      ...config.routing,
      ...(overrides.routing || {}),
      pathCommands: overrides.routing?.pathCommands
        ? [...overrides.routing.pathCommands]
        : [...config.routing.pathCommands],
      historyCommands: overrides.routing?.historyCommands
        ? [...overrides.routing.historyCommands]
        : [...config.routing.historyCommands],
      submitCommands: overrides.routing?.submitCommands
        ? [...overrides.routing.submitCommands]
        : [...config.routing.submitCommands],
      statefulCommands: overrides.routing?.statefulCommands
        ? [...overrides.routing.statefulCommands]
        : [...config.routing.statefulCommands],
      commandWeights: {
        ...config.routing.commandWeights,
        ...(overrides.routing?.commandWeights || {}),
      },
    },
    cache: {
      ...config.cache,
      ...(overrides.cache || {}),
      commands: overrides.cache?.commands
        ? [...overrides.cache.commands]
        : [...config.cache.commands],
    },
    nodes: Array.isArray(overrides.nodes)
      ? overrides.nodes.map(cloneNode)
      : config.nodes.map(cloneNode),
  };
}
