"use client";

import type { AppSettings } from "@/lib/settings/types";
import { ApplicationStatus, QueryHistoryType } from "@/lib/settings/types";

const KOTLIN_ADMIN_SETTINGS: AppSettings = {
  auto_scroll: true,
  application_status: ApplicationStatus.ACTIVE,
  gpu_enabled: false,
  maximum_chat_retention_days: null,
  notifications: [],
  needs_reindexing: false,
  anonymous_user_enabled: false,
  invite_only_enabled: false,
  deep_research_enabled: false,
  multi_model_chat_enabled: false,
  temperature_override_enabled: false,
  reasoning_override_enabled: false,
  query_history_type: QueryHistoryType.DISABLED,
  vector_db_enabled: true,
  vectorDbEnabled: true,
  default_pruning_freq: 7 * 24 * 60 * 60,
  ee_features_enabled: false,
  enterprise: null,
  appName: "Onyx",
  logoUrl: null,
  isLoading: false,
  error: undefined,
};

/** Static settings for the intentionally unauthenticated Kotlin admin UI. */
export function useSettings(): AppSettings {
  return KOTLIN_ADMIN_SETTINGS;
}

export function useIsSearchModeAvailable(): boolean {
  return false;
}
