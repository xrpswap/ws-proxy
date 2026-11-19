import { applyResponseId, extractRequestId } from './errors.js';

function stableStringify(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }
  const keys = Object.keys(value).sort();
  return `{${keys
    .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
    .join(',')}}`;
}

function cloneJson(value) {
  if (value === null || typeof value !== 'object') {
    return value;
  }
  return structuredClone(value);
}

function stripId(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return payload;
  }
  const copy = cloneJson(payload);
  delete copy.id;
  return copy;
}

export class ReadCache {
  constructor(options = {}) {
    this.enabled = options.enabled !== false;
    this.ttlMs = Number(options.ttlMs ?? 1500);
    this.commands = new Set(options.commands || ['fee', 'server_info', 'ledger_closed']);
    this.entries = new Map();
    this.inflight = new Map();
    this.hits = 0;
    this.misses = 0;
    this.coalesced = 0;
  }

  isCacheable(command) {
    return this.enabled && this.commands.has(command);
  }

  makeKey(payload) {
    return stableStringify(stripId(payload));
  }

  async getOrFetch(payload, fetcher) {
    const { hasOriginalId, originalId } = extractRequestId(payload);

    if (!this.isCacheable(payload?.command)) {
      const raw = await fetcher();
      return applyResponseId(cloneJson(raw), originalId, hasOriginalId);
    }

    const key = this.makeKey(payload);
    const now = Date.now();
    const cached = this.entries.get(key);
    if (cached) {
      if (now < cached.expiresAt) {
        this.hits += 1;
        return applyResponseId(cloneJson(cached.value), originalId, hasOriginalId);
      }
      this.entries.delete(key);
    }

    const existingInflight = this.inflight.get(key);
    if (existingInflight) {
      this.coalesced += 1;
      const shared = await existingInflight;
      return applyResponseId(cloneJson(shared), originalId, hasOriginalId);
    }

    this.misses += 1;
    const promise = (async () => {
      const raw = await fetcher();
      const stripped = stripId(raw);
      if (stripped && stripped.status !== 'error') {
        this.entries.set(key, {
          value: stripped,
          expiresAt: Date.now() + this.ttlMs,
        });
      }
      return stripped;
    })();

    this.inflight.set(key, promise);
    try {
      const shared = await promise;
      return applyResponseId(cloneJson(shared), originalId, hasOriginalId);
    } finally {
      if (this.inflight.get(key) === promise) {
        this.inflight.delete(key);
      }
    }
  }

  clear() {
    this.entries.clear();
    this.inflight.clear();
  }

  getStats() {
    const now = Date.now();
    for (const [key, entry] of this.entries.entries()) {
      if (now >= entry.expiresAt) {
        this.entries.delete(key);
      }
    }
    return {
      enabled: this.enabled,
      ttlMs: this.ttlMs,
      commands: Array.from(this.commands),
      hits: this.hits,
      misses: this.misses,
      coalesced: this.coalesced,
      size: this.entries.size,
      inflight: this.inflight.size,
    };
  }
}
