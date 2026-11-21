import type { RouteRecordRaw } from 'vue-router'

/** DPPS additions to the router (see DPPS.md). Spread into `routes` by one line in router/index.ts. */
export const dppsRoutes: RouteRecordRaw[] = [
  {
    path: '/admin/dpps/events-webhook',
    name: 'AdminDppsEventsWebhook',
    component: () => import('@/views/admin/DppsEventsWebhookView.vue'),
    meta: {
      requiresAuth: true,
      requiresAdmin: true,
      title: 'Events Webhook',
      titleKey: 'admin.dppsEventsWebhook.title',
      descriptionKey: 'admin.dppsEventsWebhook.description'
    }
  }
]
