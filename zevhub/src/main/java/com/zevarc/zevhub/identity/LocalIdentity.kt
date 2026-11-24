package com.zevarc.zevhub.identity

import android.content.Context
import com.zevarc.zevhub.device.DeviceId
import java.util.UUID

/**
 * The local device identity: a random 32-hex-char id generated on first
 * launch and persisted in SharedPreferences. Stable across restarts and
 * independent of any network address. The crypto identity stage will later
 * bind authentication to this id; until then it is the routing identity.
 */
internal class LocalIdentity private constructor(
    val deviceId: DeviceId
) {

    companion object {

        fun from(context: Context): LocalIdentity {
            val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            return create(
                stored = prefs.getString(KEY_DEVICE_ID, null),
                persist = { id -> prefs.edit().putString(KEY_DEVICE_ID, id).apply() }
            )
        }

        fun create(stored: String?, persist: (String) -> Unit): LocalIdentity {
            val id = stored?.takeIf { isValidDeviceIdFormat(it) }
                ?: generateDeviceId().also(persist)
            return LocalIdentity(DeviceId(id))
        }

        fun isValidDeviceIdFormat(value: String): Boolean =
            DEVICE_ID_PATTERN.matches(value)

        fun generateDeviceId(): String =
            UUID.randomUUID().toString().replace("-", "")

        private val DEVICE_ID_PATTERN = Regex("^[0-9a-f]{32}$")

        private const val PREFS_NAME = "zevhub_identity"

        private const val KEY_DEVICE_ID = "device_id"
    }
}
