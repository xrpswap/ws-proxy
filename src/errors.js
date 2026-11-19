import WebSocket from 'ws';

export function extractRequestId(payload) {
  if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
    const hasOriginalId = payload.id !== undefined && payload.id !== null;
    return {
      hasOriginalId,
      originalId: hasOriginalId ? payload.id : undefined,
    };
  }
  return { hasOriginalId: false, originalId: undefined };
}

export function applyResponseId(response, originalId, hasOriginalId) {
  const copy =
    response && typeof response === 'object' && !Array.isArray(response)
      ? { ...response }
      : { result: response };

  if (hasOriginalId) {
    copy.id = originalId;
  } else {
    delete copy.id;
  }
  return copy;
}

export function buildErrorResponse(originalId, hasOriginalId, errorCode, errorMessage) {
  const payload = {
    status: 'error',
    type: 'response',
    error: errorCode,
    error_message: errorMessage,
  };
  if (hasOriginalId) {
    payload.id = originalId;
  }
  return payload;
}

export function sendWsJson(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return false;
  }
  try {
    ws.send(JSON.stringify(payload));
    return true;
  } catch {
    return false;
  }
}
