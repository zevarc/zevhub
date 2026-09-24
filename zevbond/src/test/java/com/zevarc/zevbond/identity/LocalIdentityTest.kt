package com.zevarc.zevbond.identity

import com.zevarc.zevbond.device.DeviceId
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class LocalIdentityTest {

    @Test
    fun `generates a fresh 32-hex id when nothing is stored`() {
        var persisted: String? = null

        val identity = LocalIdentity.create(stored = null, persist = { persisted = it })

        assertEquals(32, identity.deviceId.value.length)
        assertTrue(persisted?.let { LocalIdentity.isValidDeviceIdFormat(it) } == true)
    }

    @Test
    fun `reuses the stored id across restarts`() {
        val identity = LocalIdentity.create(
            stored = "0123456789abcdef0123456789abcdef",
            persist = { throw IllegalStateException("must not rewrite a valid id") }
        )

        assertEquals(
            DeviceId("0123456789abcdef0123456789abcdef"),
            identity.deviceId
        )
    }

    @Test
    fun `regenerates when the stored id is malformed`() {
        var persisted: String? = null

        val identity = LocalIdentity.create(
            stored = "not-a-valid-id",
            persist = { persisted = it }
        )

        assertNotEquals("not-a-valid-id", identity.deviceId.value)
        assertEquals(identity.deviceId.value, persisted)
    }

    @Test
    fun `regenerates when the stored id has wrong length`() {
        val identity = LocalIdentity.create(
            stored = "abcdef",
            persist = { }
        )

        assertEquals(32, identity.deviceId.value.length)
    }

    @Test
    fun `format validation accepts only 32 lowercase hex chars`() {
        assertTrue(LocalIdentity.isValidDeviceIdFormat("0123456789abcdef0123456789abcdef"))
        assertFalse(LocalIdentity.isValidDeviceIdFormat("0123456789ABCDEF0123456789ABCDEF"))
        assertFalse(LocalIdentity.isValidDeviceIdFormat("0123456789abcdef0123456789abcde"))
        assertFalse(LocalIdentity.isValidDeviceIdFormat("0123456789abcdef0123456789abcdeg"))
        assertFalse(LocalIdentity.isValidDeviceIdFormat(""))
    }

    @Test
    fun `generated ids are unique`() {
        val a = LocalIdentity.generateDeviceId()
        val b = LocalIdentity.generateDeviceId()

        assertNotEquals(a, b)
    }
}
