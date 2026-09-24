package com.zevarc.zevbond.protocol

import com.zevarc.zevbond.transport.ByteTransport
import com.zevarc.zevbond.wire.WireCodec
import com.zevarc.zevbond.wire.WireConstants
import com.zevarc.zevbond.wire.WireDecoder
import com.zevarc.zevbond.wire.WireFrame
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch

internal class ProtocolImpl(
    private val transport: ByteTransport,
    private val scope: CoroutineScope
) : Protocol {

    private val decoder = WireDecoder()

    private val incoming = Channel<ProtocolFrame>(capacity = 64)

    private val decodeJob = scope.launch {
        try {
            transport.receive().collect { bytes ->
                for (frame in decoder.feed(bytes)) {
                    incoming.send(toProtocolFrame(frame))
                }
            }
            incoming.close()
        } catch (e: Exception) {
            incoming.close(e)
        }
    }

    override suspend fun send(frame: ProtocolFrame) {
        transport.send(
            WireCodec.encode(
                WireFrame(
                    version = WireConstants.VERSION,
                    kind = frame.kind.value,
                    frameId = frame.frameId,
                    contextId = frame.contextId,
                    correlationId = frame.correlationId,
                    sequence = frame.sequence,
                    payload = frame.payload
                )
            )
        )
    }

    override suspend fun receiveOne(): ProtocolFrame = incoming.receive()

    override fun receive(): Flow<ProtocolFrame> = incoming.consumeAsFlow()

    override suspend fun close() {
        decodeJob.cancel()
        incoming.close()
        transport.close()
    }

    private fun toProtocolFrame(frame: WireFrame): ProtocolFrame =
        ProtocolFrame(
            kind = ProtocolKind.from(frame.kind),
            frameId = frame.frameId,
            contextId = frame.contextId,
            correlationId = frame.correlationId,
            sequence = frame.sequence,
            payload = frame.payload
        )
}
