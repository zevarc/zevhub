package com.zevarc.zevhub.discovery

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test

class DiscoveryRegistryTest {

    @Test
    fun `emits a device once uuid and mac are both known - service first`() {
        val registry = DiscoveryRegistry()

        assertNull(registry.onService("uuid-a", "Pixel", null))
        val device = registry.onService("uuid-a", "Pixel", "aa:bb:cc:dd:ee:01")

        assertNotNull(device)
        assertEquals("uuid-a", device!!.id.value)
        assertEquals("Pixel", device.name)
    }

    @Test
    fun `emits a device once uuid and mac are both known - peer first`() {
        val registry = DiscoveryRegistry()

        assertNull(registry.onPeer("aa:bb:cc:dd:ee:01", "Pixel"))
        val device = registry.onService("uuid-a", "Pixel", "aa:bb:cc:dd:ee:01")

        assertNotNull(device)
        assertEquals("uuid-a", device!!.id.value)
    }

    @Test
    fun `does not emit a peer without a uuid`() {
        val registry = DiscoveryRegistry()

        val device = registry.onPeer("aa:bb:cc:dd:ee:01", "Pixel")

        assertNull(device)
    }

    @Test
    fun `does not emit twice for the same device`() {
        val registry = DiscoveryRegistry()
        registry.onService("uuid-a", "Pixel", "aa:bb:cc:dd:ee:01")

        assertNull(registry.onService("uuid-a", "Pixel", "aa:bb:cc:dd:ee:01"))
        assertNull(registry.onPeer("aa:bb:cc:dd:ee:01", "Pixel"))
    }

    @Test
    fun `parked name is backfilled when the mac arrives`() {
        val registry = DiscoveryRegistry()

        // DNS-SD reports the UUID without the MAC first.
        assertNull(registry.onService("uuid-a", "Pixel", null))
        // The MAC then arrives through DNS-SD itself, with no name.
        val device = registry.onService("uuid-a", null, "aa:bb:cc:dd:ee:01")

        assertNotNull(device)
        assertEquals("uuid-a", device!!.id.value)
        assertEquals("Pixel", device.name)
    }

    @Test
    fun `explicit name wins over the parked one`() {
        val registry = DiscoveryRegistry()

        assertNull(registry.onService("uuid-a", "Stale", null))
        val device = registry.onService("uuid-a", "Fresh", "aa:bb:cc:dd:ee:01")

        assertEquals("Fresh", device!!.name)
    }

    @Test
    fun `macOf resolves the connect handle for a discovered uuid`() {
        val registry = DiscoveryRegistry()
        registry.onService("uuid-a", "Pixel", "aa:bb:cc:dd:ee:01")

        assertEquals("aa:bb:cc:dd:ee:01", registry.macOf("uuid-a"))
        assertNull(registry.macOf("uuid-unknown"))
    }

    @Test
    fun `two devices with different uuids on different macs stay separate`() {
        val registry = DiscoveryRegistry()

        val a = registry.onService("uuid-a", "A", "aa:aa:aa:aa:aa:01")
        val b = registry.onService("uuid-b", "B", "aa:aa:aa:aa:aa:02")

        assertEquals("uuid-a", a!!.id.value)
        assertEquals("uuid-b", b!!.id.value)
        assertEquals("aa:aa:aa:aa:aa:01", registry.macOf("uuid-a"))
        assertEquals("aa:aa:aa:aa:aa:02", registry.macOf("uuid-b"))
    }
}
