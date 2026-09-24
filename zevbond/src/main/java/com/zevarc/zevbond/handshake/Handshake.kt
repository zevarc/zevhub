package com.zevarc.zevbond.handshake

import com.zevarc.zevbond.device.DeviceId
import com.zevarc.zevbond.protocol.FrameIdGenerator
import com.zevarc.zevbond.protocol.Protocol
import com.zevarc.zevbond.protocol.ProtocolFrame
import com.zevarc.zevbond.protocol.ProtocolKind
import java.security.SecureRandom

/**
 * Minimal v1 handshake: HELLO, AUTH, READY in the spec order, with empty
 * signatures and no verification. Establishes peer DeviceId and protocol
 * version compatibility, and cross-checks the peer's claimed DeviceId
 * against the UUID it advertised through discovery when one is known.
 * Real authentication arrives with the identity stage.
 */
internal class Handshake(
    private val localDeviceId: DeviceId,
    private val frameIds: FrameIdGenerator,
    private val expectedPeerDeviceId: DeviceId? = null
) {

    suspend fun run(protocol: Protocol, initiator: Boolean): DeviceId {
        val random = SecureRandom()

        if (initiator) {
            sendHello(protocol, random)
            val peerHello = receiveHello(protocol)
            sendAuth(protocol)
            receiveAuth(protocol)
            sendReady(protocol)
            receiveReady(protocol)
            return peerHello.deviceId
        } else {
            val peerHello = receiveHello(protocol)
            sendHello(protocol, random)
            receiveAuth(protocol)
            sendAuth(protocol)
            receiveReady(protocol)
            sendReady(protocol)
            return peerHello.deviceId
        }
    }

    private suspend fun sendHello(protocol: Protocol, random: SecureRandom) {
        val nonce = ByteArray(HandshakeCodec.NONCE_SIZE).also(random::nextBytes)
        protocol.send(
            ProtocolFrame(
                kind = ProtocolKind.HELLO,
                frameId = frameIds.next(),
                payload = HandshakeCodec.encodeHello(localDeviceId, nonce)
            )
        )
    }

    private suspend fun receiveHello(protocol: Protocol): HandshakeCodec.HelloData {
        val frame = protocol.receiveOne()
        require(frame.kind == ProtocolKind.HELLO) {
            "Expected HELLO, got ${frame.kind}"
        }
        val hello = HandshakeCodec.decodeHello(frame.payload)
        require(hello.version == HandshakeCodec.PROTOCOL_VERSION) {
            "Unsupported protocol version: ${hello.version}"
        }
        expectedPeerDeviceId?.let { expected ->
            require(hello.deviceId == expected) {
                "Peer device id mismatch: expected ${expected.value}, " +
                    "got ${hello.deviceId.value}"
            }
        }
        return hello
    }

    private suspend fun sendAuth(protocol: Protocol) {
        protocol.send(
            ProtocolFrame(
                kind = ProtocolKind.AUTH,
                frameId = frameIds.next(),
                payload = HandshakeCodec.encodeAuth(EMPTY_SIGNATURE)
            )
        )
    }

    private suspend fun receiveAuth(protocol: Protocol) {
        val frame = protocol.receiveOne()
        require(frame.kind == ProtocolKind.AUTH) {
            "Expected AUTH, got ${frame.kind}"
        }
        // Signature verification is deferred to the identity stage.
    }

    private suspend fun sendReady(protocol: Protocol) {
        protocol.send(
            ProtocolFrame(
                kind = ProtocolKind.READY,
                frameId = frameIds.next(),
                payload = ByteArray(0)
            )
        )
   }

    private suspend fun receiveReady(protocol: Protocol) {
        val frame = protocol.receiveOne()
        require(frame.kind == ProtocolKind.READY) {
            "Expected READY, got ${frame.kind}"
        }
    }

    companion object {
        private val EMPTY_SIGNATURE = ByteArray(0)
    }
}
