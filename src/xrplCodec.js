import crypto from 'node:crypto';

const XRPL_ALPHABET = 'rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz';
const ALPHABET_MAP = new Map();
for (let i = 0; i < XRPL_ALPHABET.length; i += 1) {
  ALPHABET_MAP.set(XRPL_ALPHABET[i], i);
}

const CLASSIC_ADDRESS_REGEX = /\br[1-9A-HJ-NP-Za-km-z]{24,34}\b/g;
const BLACKLIST_MESSAGE_REGEX = /blacklisted|xaman\s+blacklist|rippleflagged/i;

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
  if (!msg || typeof msg !== 'object' || msg.status !== 'error') {
    return { isBlacklistError: false, addresses: [] };
  }

  const combinedText = [msg.error_message, msg.error]
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
