package com.zevarc.zevbond.session

import com.zevarc.zevbond.device.Device
import com.zevarc.zevbond.message.Message
import com.zevarc.zevbond.message.MessageCodec
import com.zevarc.zevbond.protocol.FrameIdGenerator
import com.zevarc.zevbond.protocol.Protocol
import com.zevarc.zevbond.protocol.ProtocolFrame
import com.zevarc.zevbond.protocol.ProtocolKind
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch

internal class SessionImpl(
    override val device: Device,
    private val protocol: Protocol,
    private val scope: kotlinx.coroutines.CoroutineScope,
    private val frameIds: FrameIdGenerator = FrameIdGenerator()
) : Session {

    private val _messages = kotlinx.coroutines.flow.MutableSharedFlow<Message>(
        extraBufferCapacity = 64
    )

    private val pumpJob = scope.launch {
        protocol.receive().collect { frame ->
            when (frame.kind) {
                ProtocolKind.MESSAGE ->
                    _messages.emit(MessageCodec.decode(frame.payload))

                else -> {
                    // Unknown kinds are ignored until their consumers exist.
                }
            }
        }
    }

    override fun messages(): Flow<Message> = _messages

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
        protocol.close()
    }
}
