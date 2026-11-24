package com.zevarc.zevhub.protocol

internal enum class ProtocolKind(
    val value: Byte
) {
    HELLO(0x01),
    AUTH(0x02),
    READY(0x03),

    MESSAGE(0x10),

    REQUEST(0x20),
    RESPONSE(0x21),

    TRANSFER_START(0x30),
    TRANSFER_DATA(0x31),
    TRANSFER_COMPLETE(0x32),
    TRANSFER_CANCEL(0x33),

    STREAM_DATA(0x40),
    STREAM_CLOSE(0x41),

    ACK(0x70),
    ERROR(0x71),

    /**
     * Liveness probe. Consumed by the protocol layer and never surfaced to
     * the session: it carries no payload and is not acknowledged, so a slow
     * peer cannot turn probes into a feedback loop.
     */
    HEARTBEAT(0x72);

    companion object {
        fun from(value: Byte): ProtocolKind =
            entries.firstOrNull { it.value == value }
                ?: error("Unsupported protocol kind: $value")
    }
}