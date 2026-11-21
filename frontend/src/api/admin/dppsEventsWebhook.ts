import { apiClient } from '../client'

export interface DppsEventsWebhookConfig {
  enabled: boolean
  url: string
  /** Write-only: never returned by the server. Empty on save keeps the stored secret. */
  secret?: string
  event_types: string[] // empty = all types
  snapshot_time_utc: string // "HH:MM", empty = no daily snapshot
  balance_threshold: number
  balance_user_ids: number[]
}

export interface DppsEventsWebhookState {
  config: DppsEventsWebhookConfig
  secret_configured: boolean
  all_event_types: string[]
}

export async function getDppsEventsWebhook(): Promise<DppsEventsWebhookState> {
  const { data } = await apiClient.get<DppsEventsWebhookState>('/admin/dpps/events-webhook')
  return data
}

export async function updateDppsEventsWebhook(config: DppsEventsWebhookConfig): Promise<DppsEventsWebhookConfig> {
  const { data } = await apiClient.put<DppsEventsWebhookConfig>('/admin/dpps/events-webhook', config)
  return data
}

export default { get: getDppsEventsWebhook, update: updateDppsEventsWebhook }
