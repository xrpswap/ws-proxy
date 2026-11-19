export class TokenBucket {
  constructor({ ratePerSec = 15, burst = 30, slowDownRecoveryMs = 15000 } = {}) {
    this.baseRatePerSec = ratePerSec;
    this.burst = burst;
    this.slowDownRecoveryMs = slowDownRecoveryMs;
    this.tokens = burst;
    this.lastRefillTs = Date.now();
    this.rateMultiplier = 1;
    this.recoveryTimer = null;
    this.slowDownUntil = null;
  }

  get effectiveRate() {
    return this.baseRatePerSec * this.rateMultiplier;
  }

  refill(now = Date.now()) {
    const elapsedMs = Math.max(0, now - this.lastRefillTs);
    if (elapsedMs > 0) {
      const added = (elapsedMs / 1000) * this.effectiveRate;
      this.tokens = Math.min(this.burst, this.tokens + added);
      this.lastRefillTs = now;
    }
    return this.tokens;
  }

  getAvailableTokens(now = Date.now()) {
    return this.refill(now);
  }

  tryConsume(weight = 1, now = Date.now()) {
    this.refill(now);
    if (this.tokens >= weight) {
      this.tokens -= weight;
      return true;
    }
    return false;
  }

  triggerSlowDown(recoveryMs = this.slowDownRecoveryMs) {
    this.refill();
    this.rateMultiplier = Math.max(0.25, this.rateMultiplier * 0.5);
    this.slowDownUntil = Date.now() + recoveryMs;
    if (this.recoveryTimer) {
      clearTimeout(this.recoveryTimer);
    }
    this.recoveryTimer = setTimeout(() => {
      this.refill();
      this.rateMultiplier = 1;
      this.slowDownUntil = null;
      this.recoveryTimer = null;
    }, recoveryMs);
    if (typeof this.recoveryTimer.unref === 'function') {
      this.recoveryTimer.unref();
    }
  }

  resetSlowDown() {
    if (this.recoveryTimer) {
      clearTimeout(this.recoveryTimer);
      this.recoveryTimer = null;
    }
    this.rateMultiplier = 1;
    this.slowDownUntil = null;
  }

  destroy() {
    this.resetSlowDown();
  }
}

export class BoundedRateQueue {
  constructor({ maxLength = 200, maxWaitMs = 800, pollIntervalMs = 20 } = {}) {
    this.maxLength = maxLength;
    this.maxWaitMs = maxWaitMs;
    this.pollIntervalMs = pollIntervalMs;
    this.queue = [];
    this.pollTimer = null;
    this.closed = false;
  }

  get length() {
    return this.queue.length;
  }

  enqueue(attemptFn) {
    if (this.closed) {
      return Promise.reject(
        Object.assign(new Error('Proxy is shutting down'), { code: 'upstreamUnavailable' }),
      );
    }
    if (this.queue.length >= this.maxLength) {
      return Promise.reject(
        Object.assign(new Error('Rate limit queue is full'), { code: 'slowDown' }),
      );
    }

    return new Promise((resolve, reject) => {
      const entry = {
        attemptFn,
        resolve,
        reject,
        timer: null,
        settled: false,
      };

      entry.timer = setTimeout(() => {
        if (entry.settled) return;
        entry.settled = true;
        this.removeEntry(entry);
        reject(
          Object.assign(new Error('Request timed out waiting for rate limit tokens'), {
            code: 'slowDown',
          }),
        );
      }, this.maxWaitMs);

      if (typeof entry.timer.unref === 'function') {
        entry.timer.unref();
      }

      this.queue.push(entry);
      this.ensurePolling();
    });
  }

  removeEntry(entry) {
    const idx = this.queue.indexOf(entry);
    if (idx !== -1) {
      this.queue.splice(idx, 1);
    }
    if (this.queue.length === 0 && this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  ensurePolling() {
    if (this.pollTimer || this.queue.length === 0 || this.closed) {
      return;
    }
    this.pollTimer = setInterval(() => {
      this.drain();
    }, this.pollIntervalMs);
    if (typeof this.pollTimer.unref === 'function') {
      this.pollTimer.unref();
    }
  }

  drain() {
    if (this.closed || this.queue.length === 0) {
      if (this.pollTimer) {
        clearInterval(this.pollTimer);
        this.pollTimer = null;
      }
      return;
    }

    let index = 0;
    while (index < this.queue.length) {
      const entry = this.queue[index];
      if (entry.settled) {
        this.queue.splice(index, 1);
        continue;
      }
      try {
        const result = entry.attemptFn();
        if (result !== null && result !== undefined) {
          entry.settled = true;
          clearTimeout(entry.timer);
          this.queue.splice(index, 1);
          entry.resolve(result);
          continue;
        }
        index += 1;
      } catch (err) {
        entry.settled = true;
        clearTimeout(entry.timer);
        this.queue.splice(index, 1);
        entry.reject(err);
      }
    }

    if (this.queue.length === 0 && this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  clear() {
    this.closed = true;
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    while (this.queue.length > 0) {
      const entry = this.queue.shift();
      if (entry && !entry.settled) {
        entry.settled = true;
        clearTimeout(entry.timer);
        entry.reject(
          Object.assign(new Error('Proxy is shutting down'), { code: 'upstreamUnavailable' }),
        );
      }
    }
  }
}
