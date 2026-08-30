package com.onyx.foss.kotlin.api

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class SettingsController {
    @GetMapping("/settings")
    fun settings(): Map<String, Any?> = mapOf(
        "anonymous_user_enabled" to false,
        "invite_only_enabled" to false,
        "anonymous_user_path" to null,
        "maximum_chat_retention_days" to null,
        "company_name" to null,
        "company_description" to null,
        "notifications" to emptyList<Any>(),
        "needs_reindexing" to false,
        "gpu_enabled" to false,
        "application_status" to "active",
        "auto_scroll" to true,
        "temperature_override_enabled" to true,
        "reasoning_override_enabled" to true,
        "query_history_type" to "normal",
        "hide_query_history_from_admin_panel" to true,
        "deep_research_enabled" to false,
        "multi_model_chat_enabled" to false,
        "search_ui_enabled" to false,
        "auto_detect_search_filters" to false,
        "image_extraction_and_analysis_enabled" to false,
        "image_analysis_max_size_mb" to null,
        "user_knowledge_enabled" to false,
        "user_file_max_upload_size_mb" to 100,
        "file_token_count_threshold_k" to 200,
        "show_extra_connectors" to false,
        "disable_default_assistant" to true,
        "onyx_craft_enabled" to false,
        "onyx_craft_available" to false,
        "craft_default_enabled" to false,
        "craft_instructions" to null,
        "opencode_debugging_enabled" to false,
        "ee_features_enabled" to false,
        "tier" to "community",
        "seat_count" to null,
        "used_seats" to null,
        "opensearch_indexing_enabled" to true,
        "vector_db_enabled" to true,
        "hooks_enabled" to false,
        "version" to null,
        "max_allowed_upload_size_mb" to 250,
        "default_pruning_freq" to 604800,
        "default_user_file_max_upload_size_mb" to 100,
        "default_file_token_count_threshold_k" to 200,
        "is_containerized" to true,
        "posthog_key" to null,
        "posthog_host" to null,
    )
}
