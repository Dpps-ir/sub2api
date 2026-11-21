import { apiClient } from '../client'

export interface EventsWebhookConfig {
  enabled: boolean
  url: string
  /** Write-only: never returned by the server. Empty on save keeps the stored secret. */
  secret?: string
  event_types: string[] // empty = all types
  snapshot_time_utc: string // "HH:MM", empty = no daily snapshot
  balance_threshold: number
  balance_user_ids: number[]
}

export interface EventsWebhookState {
  config: EventsWebhookConfig
  secret_configured: boolean
  all_event_types: string[]
}

export async function getEventsWebhook(): Promise<EventsWebhookState> {
  const { data } = await apiClient.get<EventsWebhookState>('/admin/events-webhook')
  return data
}

export async function updateEventsWebhook(config: EventsWebhookConfig): Promise<EventsWebhookConfig> {
  const { data } = await apiClient.put<EventsWebhookConfig>('/admin/events-webhook', config)
  return data
}

export default { get: getEventsWebhook, update: updateEventsWebhook }
