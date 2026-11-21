import { enableAutoUnmount, flushPromises, mount } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import DppsEventsWebhookView from '../DppsEventsWebhookView.vue'
import {
  generateSecretHex,
  isHttpsUrl,
  isSnapshotTime,
  parseUserIds
} from '../dppsEventsWebhookForm'

enableAutoUnmount(afterEach)

const { getState, update, stepUpRun, showError, showSuccess } = vi.hoisted(() => ({
  getState: vi.fn(),
  update: vi.fn(),
  stepUpRun: vi.fn((action: () => Promise<unknown>) => action()),
  showError: vi.fn(),
  showSuccess: vi.fn()
}))

vi.mock('@/api/admin/dppsEventsWebhook', () => ({
  default: { get: getState, update }
}))
vi.mock('@/stores', () => ({
  useAppStore: () => ({ showError, showSuccess, showInfo: vi.fn() })
}))
vi.mock('@/composables/useClipboard', () => ({
  useClipboard: () => ({ copyToClipboard: vi.fn() })
}))
vi.mock('@/composables/useStepUp', () => ({
  useStepUp: () => ({ run: stepUpRun }),
  isStepUpBlocked: () => false,
  isStepUpCancelled: () => false,
  stepUpBlockReason: () => ''
}))
vi.mock('vue-i18n', async (importOriginal) => ({
  ...(await importOriginal<typeof import('vue-i18n')>()),
  useI18n: () => ({ t: (key: string) => key })
}))

const state = {
  config: {
    enabled: true,
    url: 'https://dpps.example/hook',
    event_types: [] as string[],
    snapshot_time_utc: '03:30',
    balance_threshold: 5,
    balance_user_ids: [12, 40]
  },
  secret_configured: true,
  all_event_types: ['usage.recorded', 'balance.low', 'snapshot.daily']
}

async function mountView() {
  const w = mount(DppsEventsWebhookView, {
    global: {
      stubs: {
        AppLayout: { template: '<div><slot /></div>' },
        TotpStepUpDialog: true,
        LoadingSpinner: true,
        Toggle: true
      }
    }
  })
  await flushPromises()
  return w
}

describe('events webhook helpers', () => {
  it('accepts only https URLs', () => {
    expect(isHttpsUrl('https://a.example/x')).toBe(true)
    expect(isHttpsUrl('http://a.example')).toBe(false)
    expect(isHttpsUrl('not a url')).toBe(false)
  })
  it('validates HH:MM', () => {
    expect(isSnapshotTime('')).toBe(true)
    expect(isSnapshotTime('23:59')).toBe(true)
    expect(isSnapshotTime('24:00')).toBe(false)
    expect(isSnapshotTime('3:30')).toBe(false)
  })
  it('parses user ids', () => {
    expect(parseUserIds('')).toEqual([])
    expect(parseUserIds('1, 2,2,3')).toEqual([1, 2, 3])
    expect(parseUserIds('1,a')).toBeNull()
    expect(parseUserIds('0')).toBeNull()
    expect(parseUserIds('-4')).toBeNull()
  })
  it('generates 64 hex chars, different each time', () => {
    const a = generateSecretHex()
    expect(a).toMatch(/^[0-9a-f]{64}$/)
    expect(generateSecretHex()).not.toBe(a)
  })
})

describe('DppsEventsWebhookView', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    getState.mockResolvedValue(state)
    update.mockResolvedValue(state.config)
  })

  it('loads the config, shows the configured badge and never fills the secret', async () => {
    const w = await mountView()
    expect((w.get('[data-test="url"]').element as HTMLInputElement).value).toBe(state.config.url)
    expect((w.get('[data-test="secret"]').element as HTMLInputElement).value).toBe('')
    expect(w.get('[data-test="secret-badge"]').text()).toBe('admin.dppsEventsWebhook.secretConfigured')
    expect((w.get('[data-test="user-ids"]').element as HTMLInputElement).value).toBe('12, 40')
  })

  it('saves through the step-up flow with an empty secret and all types as []', async () => {
    const w = await mountView()
    await w.get('form').trigger('submit')
    await flushPromises()
    expect(stepUpRun).toHaveBeenCalledTimes(1)
    expect(update).toHaveBeenCalledWith({
      enabled: true,
      url: 'https://dpps.example/hook',
      secret: '',
      event_types: [],
      snapshot_time_utc: '03:30',
      balance_threshold: 5,
      balance_user_ids: [12, 40]
    })
  })

  it('rejects an http URL and does not call the API', async () => {
    const w = await mountView()
    await w.get('[data-test="url"]').setValue('http://insecure.example')
    await w.get('form').trigger('submit')
    await flushPromises()
    expect(w.find('[data-test="url-error"]').exists()).toBe(true)
    expect(update).not.toHaveBeenCalled()
  })

  it('rejects bad user ids and bad snapshot time', async () => {
    const w = await mountView()
    await w.get('[data-test="user-ids"]').setValue('1,x')
    await w.get('[data-test="snapshot"]').setValue('25:00')
    await w.get('form').trigger('submit')
    await flushPromises()
    expect(w.find('[data-test="user-ids-error"]').exists()).toBe(true)
    expect(w.find('[data-test="snapshot-error"]').exists()).toBe(true)
    expect(update).not.toHaveBeenCalled()
  })

  it('generate fills the secret field and shows the value once', async () => {
    const w = await mountView()
    await w.get('[data-test="generate"]').trigger('click')
    const secret = (w.get('[data-test="secret"]').element as HTMLInputElement).value
    expect(secret).toMatch(/^[0-9a-f]{64}$/)
    expect(w.get('[data-test="generated"]').text()).toContain(secret)
  })

  it('sends only the checked event types', async () => {
    const w = await mountView()
    const boxes = w.findAll('[data-test="event-type"]')
    await boxes[1].setValue(true)
    await w.get('form').trigger('submit')
    await flushPromises()
    expect(update.mock.calls[0][0].event_types).toEqual(['balance.low'])
  })
})
