function parseExplicitLedgerIndex(payload) {
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  if (typeof payload.ledger_index === 'number' && Number.isFinite(payload.ledger_index)) {
    return Math.trunc(payload.ledger_index);
  }
  if (typeof payload.ledger_index === 'string' && /^\d+$/.test(payload.ledger_index.trim())) {
    return Number(payload.ledger_index.trim());
  }
  return null;
}

function hasExplicitLedger(payload) {
  if (!payload || typeof payload !== 'object') {
    return false;
  }
  if (typeof payload.ledger_hash === 'string' && payload.ledger_hash.trim().length > 0) {
    return true;
  }
  return parseExplicitLedgerIndex(payload) !== null;
}

export function classifyRequest(payload, routingConfig, cacheConfig) {
  const command = typeof payload?.command === 'string' ? payload.command : '';
  const isStateful = routingConfig.statefulCommands.includes(command);
  const statefulKind =
    command === 'path_find'
      ? 'path'
      : command === 'subscribe' || command === 'unsubscribe'
        ? 'subscribe'
        : null;

  let role = 'normal';
  if (routingConfig.pathCommands.includes(command)) {
    role = 'path';
  } else if (routingConfig.historyCommands.includes(command)) {
    role = 'history';
  } else if (routingConfig.submitCommands.includes(command)) {
    role = 'submit';
  } else if (
    !isStateful &&
    routingConfig.routeExplicitLedgerToHistory &&
    hasExplicitLedger(payload)
  ) {
    role = 'history';
  }

  const weight =
    routingConfig.commandWeights?.[command] ?? routingConfig.defaultWeight ?? 2;

  const isCacheable =
    !isStateful &&
    Boolean(cacheConfig?.enabled) &&
    Array.isArray(cacheConfig.commands) &&
    cacheConfig.commands.includes(command);

  const requestedLedgerIndex = parseExplicitLedgerIndex(payload);
  const requiresCurrent =
    payload?.ledger_index === 'current' || command === 'ledger_current';
  const preferSafeLag =
    Boolean(routingConfig.preferSafeLagForLedgerClosed) &&
    (command === 'ledger_closed' || command === 'server_info');

  return {
    command,
    role,
    isStateful,
    statefulKind,
    weight,
    isCacheable,
    requestedLedgerIndex,
    requiresCurrent,
    preferSafeLag,
  };
}
