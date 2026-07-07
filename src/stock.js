export function applyMessage(currentStock, message) {
  const kind = message?.kind

  if (kind === 'snapshot') {
    if (!isRecord(message.payload)) {
      throw new Error('invalid snapshot payload')
    }

    if (!isRecord(message.payload.stock)) {
      throw new Error('invalid snapshot payload.stock')
    }

    return { ...message.payload.stock }
  }

  if (kind === 'delta') {
    if (!isRecord(message.payload)) {
      throw new Error('invalid delta payload')
    }

    const { set, unset } = message.payload

    if (set !== undefined && !isRecord(set)) {
      throw new Error('invalid delta payload.set')
    }

    if (unset !== undefined && !Array.isArray(unset)) {
      throw new Error('invalid delta payload.unset')
    }

    const next = { ...(currentStock ?? {}) }

    for (const [sku, value] of Object.entries(set ?? {})) {
      next[sku] = value
    }

    for (const sku of unset ?? []) {
      delete next[sku]
    }

    return next
  }

  throw new Error(`unsupported message kind: ${kind}`)
}

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

export function canonicalJson(value) {
  if (Array.isArray(value)) {
    return value.map(canonicalJson)
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, child]) => [key, canonicalJson(child)]),
    )
  }

  return value
}

export function sameJson(left, right) {
  return JSON.stringify(canonicalJson(left)) === JSON.stringify(canonicalJson(right))
}
