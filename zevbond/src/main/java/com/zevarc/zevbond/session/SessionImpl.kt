package com.zevarc.zevbond.session

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.message.Message
import com.zevarc.zevbond.message.MessageCodec
import com.zevarc.zevbond.protocol.FrameIdGenerator
import com.zevarc.zevbond.protocol.Protocol
import com.zevarc.zevbond.protocol.ProtocolFrame
import com.zevarc.zevbond.protocol.ProtocolKind
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch

internal class SessionImpl(
    override val device: Device,
    private val protocol: Protocol,
    private val scope: kotlinx.coroutines.CoroutineScope,
    private val frameIds: FrameIdGenerator = FrameIdGenerator()
) : Session {

    private val incoming = Channel<Message>(capacity = 64)

    private val pumpJob = scope.launch {
        try {
            protocol.receive().collect { frame ->
                if (frame.kind == ProtocolKind.MESSAGE) {
                    incoming.send(MessageCodec.decode(frame.payload))
                }
            }
            // The receive flow completing means the transport or protocol is
            // gone; the session is dead either way.
            incoming.close()
        } catch (e: Exception) {
            incoming.close(e)
        }
    }

    override fun messages(): Flow<Message> = incoming.receiveAsFlow()

    override suspend fun send(message: Message) {
        protocol.send(
            ProtocolFrame(
                kind = ProtocolKind.MESSAGE,
                frameId = frameIds.next(),
                payload = MessageCodec.encode(message)
            )
        )
    }

    override suspend fun close() {
        pumpJob.cancel()
        incoming.close()
        protocol.close()
    }
}
