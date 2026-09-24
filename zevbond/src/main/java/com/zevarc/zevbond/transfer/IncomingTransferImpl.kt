package com.zevarc.zevbond.transfer

import com.zevarc.zevbond.protocol.FrameIdGenerator
import com.zevarc.zevbond.protocol.Protocol
import com.zevarc.zevbond.protocol.ProtocolFrame
import com.zevarc.zevbond.protocol.ProtocolKind
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A transfer offered by the peer over one context id. [accept] starts a
 * collector that routes this context's DATA frames into the destination
 * and completes on COMPLETE; [reject] tells the peer the offer is
 * refused. DATA frames for a context that was never accepted are
 * discarded by the session pump.
 */
internal class IncomingTransferImpl(
    override val metadata: TransferMetadata,
    override val totalBytes: Long?,
    private val contextId: Long,
    private val frameIds: FrameIdGenerator,
    private val scope: CoroutineScope,
    private val protocol: Protocol,
    private val frames: Channel<TransferFrame>
) : IncomingTransfer {

    private val decided = AtomicBoolean(false)

    override fun accept(destination: TransferDestination): Transfer {
        check(decided.compareAndSet(false, true)) {
            "Transfer already accepted or rejected"
        }
        return IncomingTransferJob(
            scope = scope,
            protocol = protocol,
            frameIds = frameIds,
            contextId = contextId,
            totalBytes = totalBytes,
            destination = destination,
            frames = frames
        )
    }

    override fun reject() {
        check(decided.compareAndSet(false, true)) {
            "Transfer already accepted or rejected"
        }
        // Tell the peer the offer is refused; the session pump drops any
        // in-flight DATA frames for contexts that nobody consumes.
        scope.launch {
            runCatching {
                protocol.send(
                    ProtocolFrame(
                        kind = ProtocolKind.TRANSFER_CANCEL,
                        frameId = frameIds.next(),
                        contextId = contextId
                    )
                )
            }
        }
    }
}

/**
 * The accepted receiving side of one transfer. DATA frames arrive
 * through [frames]; the session pump has already validated ordering.
 */
internal class IncomingTransferJob(
    private val scope: CoroutineScope,
    private val protocol: Protocol,
    private val frameIds: FrameIdGenerator,
    private val contextId: Long,
    private val totalBytes: Long?,
    private val destination: TransferDestination,
    private val frames: Channel<TransferFrame>
) : Transfer {

    private val progressFlow = MutableStateFlow(
        TransferProgress(bytesTransferred = 0L, totalBytes = totalBytes)
    )

    private val outcome = CompletableDeferred<TransferResult>()

    private val job = scope.launch {
        val result = runCatching {
            while (true) {
                val frame = frames.receiveCatching().getOrNull()
                    ?: throw IllegalStateException("Session closed during transfer")
                when (frame.kind) {
                    ProtocolKind.TRANSFER_DATA -> {
                        destination.write(frame.payload)
                        progressFlow.value = TransferProgress(
                            bytesTransferred = progressFlow.value.bytesTransferred +
                                frame.payload.size,
                            totalBytes = totalBytes
                        )
                    }

                    ProtocolKind.TRANSFER_COMPLETE -> {
                        destination.close()
                        return@runCatching TransferResult.Success(
                            progressFlow.value.bytesTransferred
                        )
                    }

                    ProtocolKind.TRANSFER_CANCEL -> {
                        runCatching { destination.close() }
                        return@runCatching TransferResult.Cancelled
                    }

                    else -> Unit
                }
            }
            @Suppress("UNREACHABLE_CODE")
            throw IllegalStateException("unreachable")
        }
        result.fold(
            onSuccess = { outcome.complete(it) },
            onFailure = { outcome.completeExceptionally(it) }
        )
    }

    override val progress: Flow<TransferProgress> = progressFlow

    override suspend fun await(): TransferResult = outcome.await()

    override fun cancel() {
        if (outcome.isCompleted) {
            return
        }
        scope.launch {
            runCatching {
                protocol.send(
                    ProtocolFrame(
                        kind = ProtocolKind.TRANSFER_CANCEL,
                        frameId = frameIds.next(),
                        contextId = contextId
                    )
                )
            }
        }
        job.cancel()
        scope.launch {
            runCatching { destination.close() }
        }
    }
}

/** One routed TRANSFER_* frame belonging to a single transfer context. */
internal class TransferFrame(
    val kind: ProtocolKind,
    val payload: ByteArray
)
