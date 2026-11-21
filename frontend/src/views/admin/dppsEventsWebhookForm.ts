/** Pure helpers for the events webhook settings form. */

export function isHttpsUrl(value: string): boolean {
  try {
    const u = new URL(value.trim())
    return u.protocol === 'https:' && u.hostname !== ''
  } catch {
    return false
  }
}

export function isSnapshotTime(value: string): boolean {
  return value === '' || /^([01]\d|2[0-3]):[0-5]\d$/.test(value)
}

/** Returns the parsed IDs, or null when any entry is not a positive whole number. */
export function parseUserIds(text: string): number[] | null {
  const parts = text
    .split(',')
    .map((p) => p.trim())
    .filter((p) => p !== '')
  const ids: number[] = []
  for (const p of parts) {
    if (!/^\d+$/.test(p)) return null
    const n = Number(p)
    if (!Number.isSafeInteger(n) || n <= 0) return null
    if (!ids.includes(n)) ids.push(n)
  }
  return ids
}

/** 32 random bytes as 64 hex characters. */
export function generateSecretHex(): string {
  const bytes = new Uint8Array(32)
  crypto.getRandomValues(bytes)
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}
