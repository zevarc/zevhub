package com.zevarc.zevhub.transfer

import com.zevarc.zevhub.protocol.FrameIdGenerator
import com.zevarc.zevhub.protocol.Protocol
import com.zevarc.zevhub.protocol.ProtocolFrame
import com.zevarc.zevhub.protocol.ProtocolKind
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Drives an outgoing transfer: announces it, streams source chunks as
 * DATA frames while the peer keeps up, and completes with COMPLETE or
 * CANCEL. A watcher on the context channel settles the outcome when the
 * peer cancels or the session dies, even while the sender coroutine is
 * blocked reading from the source. Reading suspends while the peer is
 * not draining DATA, which keeps memory flat on large data.
 */
internal class TransferImpl(
    private val scope: CoroutineScope,
    private val protocol: Protocol,
    private val frameIds: FrameIdGenerator,
    val contextId: Long,
    private val frames: Channel<TransferFrame>,
    private val source: TransferSource,
    private val onSettled: () -> Unit,
    private val chunkSize: Int = DEFAULT_CHUNK_SIZE
) : Transfer {

    private val progressFlow = MutableStateFlow(
        TransferProgress(bytesTransferred = 0L, totalBytes = source.totalBytes)
    )

    private val outcome = CompletableDeferred<TransferResult>()

    /** Completes on the first cancel() call; later ones are no-ops. */
    private val cancelRequested = CompletableDeferred<Unit>()

    /** Guards the one-shot end-of-transfer cleanup. */
    private val settled = AtomicBoolean(false)

    private var nextSequence = 0

    private val senderJob = scope.launch {
        val result = runCatching {
            protocol.send(
                ProtocolFrame(
                    kind = ProtocolKind.TRANSFER_START,
                    frameId = frameIds.next(),
                    contextId = contextId,
                    payload = TransferCodec.encodeStart(source.metadata, source.totalBytes)
                )
            )
            streamData()
            protocol.send(
                ProtocolFrame(
                    kind = ProtocolKind.TRANSFER_COMPLETE,
                    frameId = frameIds.next(),
                    contextId = contextId
                )
            )
            TransferResult.Success(progressFlow.value.bytesTransferred)
        }
        settle(result)
    }

    init {
        // Settles the outcome when the peer or the session ends the
        // transfer out of band of the sender coroutine.
        scope.launch {
            while (true) {
                val frame = frames.receiveCatching().getOrNull()
                if (frame == null) {
                    // The context channel closed: either a local cancel
                    // (which settles its own outcome) or session death,
                    // which must fail the wait or the caller hangs on a
                    // source that never resumes.
                    if (!outcome.isCompleted && !cancelRequested.isCompleted) {
                        senderJob.cancel()
                        markFailedBeforeResult()
                        outcome.completeExceptionally(
                            IllegalStateException("Session closed during transfer")
                        )
                    }
                    return@launch
                }
                if (frame.kind == ProtocolKind.TRANSFER_CANCEL) {
                    cancelRequested.complete(Unit)
                    frames.close()
                    senderJob.cancel()
                    markSettled(TransferResult.Cancelled)
                    outcome.complete(TransferResult.Cancelled)
                    return@launch
                }
            }
        }
    }

    override val progress: Flow<TransferProgress> = progressFlow

    override suspend fun await(): TransferResult = outcome.await()

    override fun cancel() {
        if (!cancelRequested.complete(Unit)) {
            return
        }
        frames.close()
        senderJob.cancel()
        scope.launch {
            announceCancelToPeer()
        }
        markSettled(TransferResult.Cancelled)
        outcome.complete(TransferResult.Cancelled)
    }

    /** Maps the sender coroutine's terminal state onto the outcome. */
    private fun settle(result: Result<TransferResult>) {
        val terminal = result.getOrElse { e ->
            when (e) {
                is kotlinx.coroutines.CancellationException -> return
                else -> {
                    // Transport or protocol death is not a result value.
                    markFailedBeforeResult()
                    outcome.completeExceptionally(e)
                    return
                }
            }
        }
        markSettled(terminal)
        outcome.complete(terminal)
    }

    /** Runs the one-shot end-of-transfer cleanup exactly once. */
    private fun markSettled(result: TransferResult) {
        if (settled.compareAndSet(false, true)) {
            onSettled()
            // Every terminal path funnels through here: success, local cancel and
            // peer cancel. Releasing the source anywhere else would miss one.
            scope.launch { runCatching { source.close(result) } }
        }
    }

    /**
     * Releases the source when the transfer ended without a result value:
     * session death or a source failure. The caller learns the failure from
     * [await] throwing, but the source still has to let go of its handle.
     */
    private fun markFailedBeforeResult() {
        if (settled.compareAndSet(false, true)) {
            onSettled()
            scope.launch { runCatching { source.close(TransferResult.Cancelled) } }
        }
    }

    private suspend fun streamData() {
        while (true) {
            if (cancelRequested.isCompleted) {
                return
            }
            val chunk = source.read(chunkSize) ?: return
            protocol.send(
                ProtocolFrame(
                    kind = ProtocolKind.TRANSFER_DATA,
                    frameId = frameIds.next(),
                    contextId = contextId,
                    sequence = nextSequence++,
                    payload = chunk
                )
            )
            progressFlow.value = TransferProgress(
                bytesTransferred = progressFlow.value.bytesTransferred + chunk.size,
                totalBytes = source.totalBytes
            )
        }
    }

    private suspend fun announceCancelToPeer() {
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

    companion object {
        const val DEFAULT_CHUNK_SIZE = 256 * 1024
    }
}
