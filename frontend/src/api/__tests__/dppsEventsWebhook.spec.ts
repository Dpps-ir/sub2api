import { beforeEach, describe, expect, it, vi } from 'vitest'

const { get, put } = vi.hoisted(() => ({ get: vi.fn(), put: vi.fn() }))

vi.mock('@/api/client', () => ({ apiClient: { get, put } }))

import dppsEventsWebhookAPI from '@/api/admin/dppsEventsWebhook'
import { dppsRoutes } from '@/router/dppsRoutes'

// The backend registers these under /api/v1/admin/dpps (backend/internal/server/routes/dpps_admin.go).
describe('dppsEventsWebhook API', () => {
  beforeEach(() => {
    get.mockReset()
    put.mockReset()
  })

  it('reads and writes the settings under /admin/dpps/events-webhook', async () => {
    get.mockResolvedValue({ data: { config: {}, secret_configured: false, all_event_types: [] } })
    put.mockResolvedValue({ data: { enabled: true } })
    await dppsEventsWebhookAPI.get()
    await dppsEventsWebhookAPI.update({
      enabled: true,
      url: 'https://x.example/hook',
      event_types: [],
      snapshot_time_utc: '',
      balance_threshold: 0,
      balance_user_ids: []
    })
    expect(get).toHaveBeenCalledWith('/admin/dpps/events-webhook')
    expect(put.mock.calls[0][0]).toBe('/admin/dpps/events-webhook')
  })

  it('has an admin-only page route with a title key', () => {
    expect(dppsRoutes).toHaveLength(1)
    expect(dppsRoutes[0].path).toBe('/admin/dpps/events-webhook')
    expect(dppsRoutes[0].name).toBe('AdminDppsEventsWebhook')
    expect(dppsRoutes[0].meta).toMatchObject({
      requiresAuth: true,
      requiresAdmin: true,
      titleKey: 'admin.dppsEventsWebhook.title'
    })
  })
})
