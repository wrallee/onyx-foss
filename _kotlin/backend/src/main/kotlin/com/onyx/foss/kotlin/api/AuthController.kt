package com.onyx.foss.kotlin.api

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

/**
 * This backend port has no auth system. Every request is treated as the
 * single built-in admin so the frontend's login gate never blocks access.
 */
@RestController
class AuthController {
    @GetMapping("/auth/type")
    fun authType(): Map<String, Any?> = mapOf(
        "multi_tenant" to false,
        "requires_verification" to false,
        "anonymous_user_enabled" to false,
        "password_min_length" to 8,
        "password_max_length" to 128,
        "password_require_uppercase" to false,
        "password_require_lowercase" to false,
        "password_require_digit" to false,
        "password_require_special_char" to false,
        "has_users" to true,
        "oauth_enabled" to false,
        "password_auth_enabled" to false,
        "sso_providers" to emptyList<Any>(),
    )

    @GetMapping("/me")
    fun me(): Map<String, Any?> = mapOf(
        "id" to "00000000-0000-0000-0000-000000000000",
        "email" to "admin@onyx.local",
        "is_active" to true,
        "is_superuser" to true,
        "is_verified" to true,
        "account_type" to "STANDARD",
        "team_name" to null,
        "is_admin" to true,
        "admin_capabilities" to listOf("admin"),
        "effective_permissions" to listOf("admin"),
        "password_configured" to false,
        "preferences" to mapOf(
            "chosen_assistants" to null,
            "visible_assistants" to emptyList<Any>(),
            "hidden_assistants" to emptyList<Any>(),
            "default_model" to null,
            "recent_assistants" to emptyList<Any>(),
            "auto_scroll" to true,
            "shortcut_enabled" to false,
            "temperature_override_enabled" to false,
            "theme_preference" to "system",
            "language" to null,
            "chat_background" to null,
            "default_app_mode" to "CHAT",
        ),
    )
}
