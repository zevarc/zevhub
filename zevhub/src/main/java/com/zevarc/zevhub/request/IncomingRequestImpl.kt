package com.zevarc.zevhub.request

import com.zevarc.zevhub.protocol.FrameIdGenerator
import com.zevarc.zevhub.protocol.Protocol
import com.zevarc.zevhub.protocol.ProtocolFrame
import com.zevarc.zevhub.protocol.ProtocolKind
import java.util.concurrent.atomic.AtomicBoolean

internal class IncomingRequestImpl(
    override val type: RequestType,
    override val data: ByteArray,
    private val correlationId: Long,
    private val frameIds: FrameIdGenerator,
    private val protocol: com.zevarc.zevhub.protocol.Protocol
) : IncomingRequest {

    private val answered = AtomicBoolean(false)

    override suspend fun respond(response: Response) {
        check(answered.compareAndSet(false, true)) {
            "Request already answered"
        }
        protocol.send(
            ProtocolFrame(
                kind = ProtocolKind.RESPONSE,
                frameId = frameIds.next(),
                correlationId = correlationId,
                payload = ResponseCodec.encode(response)
            )
        )
    }
}
