<template>
  <AppLayout>
    <div class="mx-auto max-w-3xl space-y-6">
      <section class="border-b border-gray-200 pb-5 dark:border-dark-700">
        <h2 class="text-lg font-semibold text-gray-900 dark:text-white">
          {{ t('admin.eventsWebhook.title') }}
        </h2>
        <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">
          {{ t('admin.eventsWebhook.description') }}
        </p>
      </section>

      <div v-if="loading" class="flex justify-center py-12">
        <LoadingSpinner />
      </div>

      <form v-else class="card space-y-6 p-6" novalidate @submit.prevent="save">
        <div class="flex items-center justify-between gap-4">
          <div>
            <div class="text-sm font-medium text-gray-900 dark:text-white">
              {{ t('admin.eventsWebhook.enabled') }}
            </div>
            <p class="text-xs text-gray-500 dark:text-gray-400">
              {{ t('admin.eventsWebhook.enabledHint') }}
            </p>
          </div>
          <Toggle v-model="form.enabled" data-test="enabled" />
        </div>

        <div>
          <label class="mb-1 block text-xs font-medium text-gray-600 dark:text-gray-400">
            {{ t('admin.eventsWebhook.url') }}
          </label>
          <input
            v-model="form.url"
            data-test="url"
            type="url"
            class="input w-full"
            placeholder="https://"
            autocomplete="off"
          />
          <p v-if="errors.url" class="mt-1 text-xs text-red-600" data-test="url-error">
            {{ errors.url }}
          </p>
          <p v-else class="mt-1 text-xs text-gray-500 dark:text-gray-400">
            {{ t('admin.eventsWebhook.urlHint') }}
          </p>
        </div>

        <div>
          <div class="mb-1 flex items-center gap-2">
            <label class="block text-xs font-medium text-gray-600 dark:text-gray-400">
              {{ t('admin.eventsWebhook.secret') }}
            </label>
            <span
              class="rounded px-2 py-0.5 text-xs"
              :class="
                secretConfigured
                  ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300'
                  : 'bg-gray-100 text-gray-600 dark:bg-dark-700 dark:text-gray-300'
              "
              data-test="secret-badge"
            >
              {{
                secretConfigured
                  ? t('admin.eventsWebhook.secretConfigured')
                  : t('admin.eventsWebhook.secretNotConfigured')
              }}
            </span>
          </div>
          <div class="flex gap-2">
            <input
              v-model="form.secret"
              data-test="secret"
              type="password"
              class="input w-full"
              autocomplete="new-password"
              :placeholder="
                secretConfigured
                  ? t('admin.eventsWebhook.secretPlaceholderKeep')
                  : t('admin.eventsWebhook.secretPlaceholderNew')
              "
            />
            <button type="button" class="btn btn-secondary" data-test="generate" @click="generate">
              {{ t('admin.eventsWebhook.generate') }}
            </button>
          </div>
          <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">
            {{ t('admin.eventsWebhook.secretHint') }}
          </p>
          <div
            v-if="generated"
            class="mt-3 rounded border border-amber-300 bg-amber-50 p-3 dark:border-amber-700 dark:bg-amber-900/20"
            data-test="generated"
          >
            <div class="text-xs font-medium text-amber-800 dark:text-amber-200">
              {{ t('admin.eventsWebhook.generatedTitle') }}
            </div>
            <div class="mt-2 flex items-center gap-2">
              <code class="min-w-0 flex-1 break-all text-xs text-gray-900 dark:text-white">{{ generated }}</code>
              <button type="button" class="btn btn-secondary" data-test="copy" @click="copyGenerated">
                {{ t('admin.eventsWebhook.copy') }}
              </button>
            </div>
            <p class="mt-2 text-xs text-amber-800 dark:text-amber-200">
              {{ t('admin.eventsWebhook.generatedNote') }}
            </p>
          </div>
        </div>

        <div>
          <div class="mb-1 flex items-center justify-between">
            <label class="block text-xs font-medium text-gray-600 dark:text-gray-400">
              {{ t('admin.eventsWebhook.eventTypes') }}
            </label>
            <label class="flex items-center gap-2 text-xs text-gray-700 dark:text-gray-300">
              <input
                type="checkbox"
                data-test="all-types"
                :checked="allTypesChecked"
                @change="toggleAll(($event.target as HTMLInputElement).checked)"
              />
              {{ t('admin.eventsWebhook.allTypes') }}
            </label>
          </div>
          <div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
            <label
              v-for="type in allEventTypes"
              :key="type"
              class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300"
            >
              <input v-model="form.event_types" type="checkbox" :value="type" data-test="event-type" />
              <span class="font-mono text-xs">{{ type }}</span>
            </label>
          </div>
          <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">
            {{ t('admin.eventsWebhook.eventTypesHint') }}
          </p>
        </div>

        <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <div>
            <label class="mb-1 block text-xs font-medium text-gray-600 dark:text-gray-400">
              {{ t('admin.eventsWebhook.snapshotTime') }}
            </label>
            <input
              v-model="form.snapshot_time_utc"
              data-test="snapshot"
              type="text"
              class="input w-full"
              placeholder="03:30"
              maxlength="5"
            />
            <p v-if="errors.snapshot" class="mt-1 text-xs text-red-600" data-test="snapshot-error">
              {{ errors.snapshot }}
            </p>
            <p v-else class="mt-1 text-xs text-gray-500 dark:text-gray-400">
              {{ t('admin.eventsWebhook.snapshotTimeHint') }}
            </p>
          </div>
          <div>
            <label class="mb-1 block text-xs font-medium text-gray-600 dark:text-gray-400">
              {{ t('admin.eventsWebhook.balanceThreshold') }}
            </label>
            <input
              v-model.number="form.balance_threshold"
              data-test="threshold"
              type="number"
              min="0"
              step="any"
              class="input w-full"
            />
            <p v-if="errors.threshold" class="mt-1 text-xs text-red-600" data-test="threshold-error">
              {{ errors.threshold }}
            </p>
            <p v-else class="mt-1 text-xs text-gray-500 dark:text-gray-400">
              {{ t('admin.eventsWebhook.balanceThresholdHint') }}
            </p>
          </div>
        </div>

        <div>
          <label class="mb-1 block text-xs font-medium text-gray-600 dark:text-gray-400">
            {{ t('admin.eventsWebhook.balanceUserIds') }}
          </label>
          <input v-model="userIdsText" data-test="user-ids" type="text" class="input w-full" placeholder="12, 40, 97" />
          <p v-if="errors.userIds" class="mt-1 text-xs text-red-600" data-test="user-ids-error">
            {{ errors.userIds }}
          </p>
          <p v-else class="mt-1 text-xs text-gray-500 dark:text-gray-400">
            {{ t('admin.eventsWebhook.balanceUserIdsHint') }}
          </p>
        </div>

        <div class="flex justify-end">
          <button type="submit" class="btn btn-primary" data-test="save" :disabled="saving">
            {{ saving ? t('common.processing') : t('admin.eventsWebhook.save') }}
          </button>
        </div>
      </form>
    </div>
    <TotpStepUpDialog :controller="stepUp" />
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import AppLayout from '@/components/layout/AppLayout.vue'
import LoadingSpinner from '@/components/common/LoadingSpinner.vue'
import Toggle from '@/components/common/Toggle.vue'
import TotpStepUpDialog from '@/components/auth/TotpStepUpDialog.vue'
import { adminAPI } from '@/api/admin'
import { useAppStore } from '@/stores'
import { useClipboard } from '@/composables/useClipboard'
import { isStepUpBlocked, isStepUpCancelled, stepUpBlockReason, useStepUp } from '@/composables/useStepUp'
import { extractApiErrorMessage } from '@/utils/apiError'
import { generateSecretHex, isHttpsUrl, isSnapshotTime, parseUserIds } from './eventsWebhookForm'

const { t } = useI18n()
const appStore = useAppStore()
const { copyToClipboard } = useClipboard()
const stepUp = useStepUp()

const loading = ref(true)
const saving = ref(false)
const secretConfigured = ref(false)
const allEventTypes = ref<string[]>([])
const generated = ref('')
const userIdsText = ref('')
const form = reactive({
  enabled: false,
  url: '',
  secret: '',
  event_types: [] as string[],
  snapshot_time_utc: '',
  balance_threshold: 0
})
const errors = reactive({ url: '', snapshot: '', threshold: '', userIds: '' })

// Empty selection means "all types" on the server, so the toggle shows checked for both.
const allTypesChecked = computed(
  () => form.event_types.length === 0 || form.event_types.length === allEventTypes.value.length
)

function toggleAll(checked: boolean) {
  form.event_types = checked ? [] : [...allEventTypes.value.slice(0, 1)]
}

function generate() {
  const value = generateSecretHex()
  form.secret = value
  generated.value = value
}

async function copyGenerated() {
  await copyToClipboard(generated.value)
}

async function load() {
  loading.value = true
  try {
    const state = await adminAPI.eventsWebhook.get()
    const c = state.config
    allEventTypes.value = state.all_event_types ?? []
    secretConfigured.value = state.secret_configured
    form.enabled = c.enabled
    form.url = c.url ?? ''
    form.secret = ''
    form.event_types = [...(c.event_types ?? [])]
    form.snapshot_time_utc = c.snapshot_time_utc ?? ''
    form.balance_threshold = c.balance_threshold ?? 0
    userIdsText.value = (c.balance_user_ids ?? []).join(', ')
  } catch (error) {
    appStore.showError(extractApiErrorMessage(error, t('admin.eventsWebhook.loadFailed')))
  } finally {
    loading.value = false
  }
}

function validate(): number[] | null {
  errors.url = ''
  errors.snapshot = ''
  errors.threshold = ''
  errors.userIds = ''
  const ids = parseUserIds(userIdsText.value)
  // An empty URL is allowed only while the webhook is disabled.
  if (form.url.trim() !== '' || form.enabled) {
    if (!isHttpsUrl(form.url)) errors.url = t('admin.eventsWebhook.urlInvalid')
  }
  if (!isSnapshotTime(form.snapshot_time_utc.trim())) errors.snapshot = t('admin.eventsWebhook.snapshotTimeInvalid')
  const th = Number(form.balance_threshold)
  if (!Number.isFinite(th) || th < 0) errors.threshold = t('admin.eventsWebhook.balanceThresholdInvalid')
  if (ids === null) errors.userIds = t('admin.eventsWebhook.balanceUserIdsInvalid')
  return errors.url || errors.snapshot || errors.threshold || ids === null ? null : ids
}

async function save() {
  const ids = validate()
  if (ids === null) return
  saving.value = true
  const payload = {
    enabled: form.enabled,
    url: form.url.trim(),
    secret: form.secret,
    event_types: allTypesChecked.value ? [] : [...form.event_types],
    snapshot_time_utc: form.snapshot_time_utc.trim(),
    balance_threshold: Number(form.balance_threshold) || 0,
    balance_user_ids: ids
  }
  try {
    await stepUp.run(() => adminAPI.eventsWebhook.update(payload))
    appStore.showSuccess(t('admin.eventsWebhook.saved'))
    // Reload for the new "configured" state; the typed/generated secret is dropped from the form.
    await load()
  } catch (error) {
    if (isStepUpCancelled(error)) return
    if (isStepUpBlocked(error)) {
      appStore.showError(
        stepUpBlockReason(error) === 'STEP_UP_ADMIN_API_KEY_FORBIDDEN'
          ? t('stepUp.adminApiKeyForbidden')
          : t('stepUp.notEnabled')
      )
      return
    }
    appStore.showError(extractApiErrorMessage(error, t('admin.eventsWebhook.saveFailed')))
  } finally {
    saving.value = false
  }
}

onMounted(load)
</script>
