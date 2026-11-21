import crypto from 'node:crypto';

const XRPL_ALPHABET = 'rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz';
const ALPHABET_MAP = new Map();
for (let i = 0; i < XRPL_ALPHABET.length; i += 1) {
  ALPHABET_MAP.set(XRPL_ALPHABET[i], i);
}

const CLASSIC_ADDRESS_REGEX =
  /(?<![1-9A-HJ-NP-Za-km-z])r[1-9A-HJ-NP-Za-km-z]{24,34}(?![1-9A-HJ-NP-Za-km-z])/g;
const BLACKLIST_MESSAGE_REGEX = /blacklist|blocklist|rippleflagged/i;

function doubleSha256(buf) {
  const first = crypto.createHash('sha256').update(buf).digest();
  return crypto.createHash('sha256').update(first).digest();
}

export function decodeXrplClassicAddressToHex(address) {
  if (typeof address !== 'string' || address.length < 25 || address.length > 35 || address[0] !== 'r') {
    return null;
  }

  let num = 0n;
  for (let i = 0; i < address.length; i += 1) {
    const val = ALPHABET_MAP.get(address[i]);
    if (val === undefined) {
      return null;
    }
    num = num * 58n + BigInt(val);
  }

  const rawBytes = [];
  while (num > 0n) {
    rawBytes.push(Number(num & 0xffn));
    num >>= 8n;
  }
  rawBytes.reverse();

  let leadingZeros = 0;
  while (leadingZeros < address.length && address[leadingZeros] === XRPL_ALPHABET[0]) {
    leadingZeros += 1;
  }

  const decoded = Buffer.alloc(leadingZeros + rawBytes.length);
  for (let i = 0; i < rawBytes.length; i += 1) {
    decoded[leadingZeros + i] = rawBytes[i];
  }

  // Standard XRPL classic address: 1-byte version (0x00) + 20-byte AccountID + 4-byte checksum = 25 bytes
  if (decoded.length !== 25 || decoded[0] !== 0x00) {
    return null;
  }

  const payload = decoded.subarray(0, 21);
  const checksum = decoded.subarray(21, 25);
  const expected = doubleSha256(payload).subarray(0, 4);
  if (!checksum.equals(expected)) {
    return null;
  }

  return decoded.subarray(1, 21).toString('hex').toUpperCase();
}

export function encodeXrplAccountIdHexToClassic(accountIdHex) {
  if (typeof accountIdHex !== 'string' || !/^[0-9a-fA-F]{40}$/.test(accountIdHex)) {
    return null;
  }
  const accountBytes = Buffer.from(accountIdHex, 'hex');
  const payload = Buffer.concat([Buffer.from([0x00]), accountBytes]);
  const checksum = doubleSha256(payload).subarray(0, 4);
  const full = Buffer.concat([payload, checksum]);

  let num = 0n;
  for (const byte of full) {
    num = (num << 8n) + BigInt(byte);
  }

  const chars = [];
  while (num > 0n) {
    const rem = Number(num % 58n);
    chars.push(XRPL_ALPHABET[rem]);
    num /= 58n;
  }

  for (const byte of full) {
    if (byte === 0) {
      chars.push(XRPL_ALPHABET[0]);
    } else {
      break;
    }
  }

  return chars.reverse().join('');
}

export function extractBlacklistRejection(msg) {
  if (!msg || typeof msg !== 'object') {
    return { isBlacklistError: false, addresses: [] };
  }

  let errorTextFields = [];
  let isSafeLocalRejection = false;

  if (msg.status === 'error') {
    isSafeLocalRejection = true;
    errorTextFields = [
      msg.error_message,
      msg.error,
      msg.message,
      msg.error_exception,
      msg.result?.error_message,
      msg.result?.engine_result_message,
    ];
  } else if (msg.status === 'success' && msg.result && typeof msg.result === 'object') {
    const res = msg.result;
    const engineResult = typeof res.engine_result === 'string' ? res.engine_result : '';
    // Strictly forbid retry if accepted/applied/broadcast/kept/queued or tes*/tec*/ter*
    const touchedLedgerOrQueue =
      res.accepted === true ||
      res.applied === true ||
      res.broadcast === true ||
      res.kept === true ||
      res.queued === true ||
      /^(tes|tec|ter)/i.test(engineResult);

    if (!touchedLedgerOrQueue && (engineResult === 'telBLACKLISTED' || /^(tel|tem|tef)/i.test(engineResult))) {
      isSafeLocalRejection = true;
      errorTextFields = [
        res.engine_result,
        res.engine_result_message,
        res.error_message,
        res.error,
        msg.error_message,
      ];
    }
  }

  if (!isSafeLocalRejection) {
    return { isBlacklistError: false, addresses: [] };
  }

  const combinedText = errorTextFields
    .filter((v) => typeof v === 'string' && v.length > 0)
    .join(' ');

  if (!BLACKLIST_MESSAGE_REGEX.test(combinedText)) {
    return { isBlacklistError: false, addresses: [] };
  }

  const matches = combinedText.match(CLASSIC_ADDRESS_REGEX) || [];
  const seen = new Set();
  const addresses = [];

  for (const classic of matches) {
    if (seen.has(classic)) continue;
    seen.add(classic);
    const accountIdHex = decodeXrplClassicAddressToHex(classic);
    if (accountIdHex) {
      addresses.push({ classic, accountIdHex });
    }
  }

  return { isBlacklistError: true, addresses };
}

export function payloadMatchesBlacklistedTargets(payload, targetEntries) {
  if (!payload || typeof payload !== 'object' || !targetEntries || targetEntries.size === 0) {
    return false;
  }

  const upperBlob =
    typeof payload.tx_blob === 'string' && payload.tx_blob.length >= 40
      ? payload.tx_blob.toUpperCase()
      : null;

  const jsonText =
    payload.tx_json && typeof payload.tx_json === 'object'
      ? JSON.stringify(payload.tx_json)
      : null;

  for (const entry of targetEntries.values()) {
    if (upperBlob && entry.accountIdHex) {
      let pos = 0;
      while (pos <= upperBlob.length - 40) {
        const idx = upperBlob.indexOf(entry.accountIdHex, pos);
        if (idx === -1) break;
        if (idx % 2 === 0) {
          return true;
        }
        pos = idx + 1;
      }
    }
    if (jsonText && entry.classic && jsonText.includes(entry.classic)) {
      return true;
    }
  }

  return false;
}

function readVlLength(buf, offset) {
  if (offset >= buf.length) return null;
  const b1 = buf[offset];
  if (b1 <= 192) {
    return { length: b1, bytesRead: 1 };
  }
  if (b1 <= 240) {
    if (offset + 1 >= buf.length) return null;
    const b2 = buf[offset + 1];
    return { length: 193 + (b1 - 193) * 256 + b2, bytesRead: 2 };
  }
  if (b1 <= 254) {
    if (offset + 2 >= buf.length) return null;
    const b2 = buf[offset + 1];
    const b3 = buf[offset + 2];
    return { length: 12481 + (b1 - 241) * 65536 + (b2 << 8) + b3, bytesRead: 3 };
  }
  return null;
}

export function extractAccountFromTxBlob(txBlob) {
  if (typeof txBlob !== 'string' || txBlob.length < 44 || txBlob.length % 2 !== 0) {
    return null;
  }
  if (!/^[0-9a-fA-F]+$/.test(txBlob)) {
    return null;
  }

  const buf = Buffer.from(txBlob, 'hex');
  let offset = 0;

  while (offset < buf.length) {
    const header = buf[offset++];
    let typeCode = header >> 4;
    let fieldCode = header & 0x0f;

    if (typeCode === 0) {
      if (offset >= buf.length) return null;
      typeCode = buf[offset++];
    }
    if (fieldCode === 0) {
      if (offset >= buf.length) return null;
      fieldCode = buf[offset++];
    }

    if (typeCode === 1) {
      offset += 2; // UInt16
    } else if (typeCode === 2) {
      offset += 4; // UInt32
    } else if (typeCode === 3) {
      offset += 8; // UInt64
    } else if (typeCode === 4) {
      offset += 16; // Hash128
    } else if (typeCode === 5) {
      offset += 32; // Hash256
    } else if (typeCode === 6) {
      if (offset >= buf.length) return null;
      const isIou = (buf[offset] & 0x80) !== 0;
      offset += isIou ? 48 : 8; // Amount
    } else if (typeCode === 7) {
      const vl = readVlLength(buf, offset);
      if (!vl) return null;
      offset += vl.bytesRead + vl.length; // Blob
    } else if (typeCode === 8) {
      const vl = readVlLength(buf, offset);
      if (!vl) return null;
      offset += vl.bytesRead;
      if (offset + vl.length > buf.length) return null;
      if (fieldCode === 1 && vl.length === 20) {
        return encodeXrplAccountIdHexToClassic(
          buf.subarray(offset, offset + 20).toString('hex'),
        );
      }
      offset += vl.length;
    } else {
      return null;
    }
  }

  return null;
}

export function extractAccountFromRequest(payload, isSubmit = false) {
  if (!payload || typeof payload !== 'object') return null;

  if (payload.command === 'account_info') {
    if (typeof payload.account === 'string' && decodeXrplClassicAddressToHex(payload.account)) {
      return payload.account;
    }
    return null;
  }

  if (isSubmit) {
    const jsonAccount = payload.tx_json?.Account;
    if (typeof jsonAccount === 'string' && decodeXrplClassicAddressToHex(jsonAccount)) {
      return jsonAccount;
    }
    if (typeof payload.account === 'string' && decodeXrplClassicAddressToHex(payload.account)) {
      return payload.account;
    }
    if (typeof payload.tx_blob === 'string') {
      return extractAccountFromTxBlob(payload.tx_blob);
    }
  }

  return null;
}

export function isSuccessfulSubmitResponse(msg) {
  if (!msg || typeof msg !== 'object' || msg.status !== 'success') {
    return false;
  }
  const res = msg.result;
  if (!res || typeof res !== 'object') {
    return false;
  }
  const engineResult = typeof res.engine_result === 'string' ? res.engine_result : '';
  return (
    res.accepted === true ||
    res.applied === true ||
    res.broadcast === true ||
    res.kept === true ||
    res.queued === true ||
    /^(tes|ter)/i.test(engineResult)
  );
}

