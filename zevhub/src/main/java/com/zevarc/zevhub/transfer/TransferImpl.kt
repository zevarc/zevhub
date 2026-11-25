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

    /**
     * Bytes already published to [progressFlow].
     *
     * Publishing per chunk costs a CAS plus a collector wake-up per frame, which
     * for 256 KiB chunks is thousands of times per transfer. Progress is a
     * human-facing signal, so it is throttled by distance instead: see
     * [PROGRESS_STEP_BYTES].
     */
    private var publishedBytes = 0L

    /** Running total of successfully handed-off bytes. */
    private var sentBytes = 0L

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
            publishFinalProgress()
            TransferResult.Success(sentBytes)
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

    /** Publishes the exact total once, whatever the throttle did. */
    private fun publishFinalProgress() {
        if (publishedBytes != sentBytes) {
            publishedBytes = sentBytes
            progressFlow.value = TransferProgress(sentBytes, source.totalBytes)
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
            sentBytes += chunk.size
            if (sentBytes - publishedBytes >= PROGRESS_STEP_BYTES) {
                publishedBytes = sentBytes
                progressFlow.value = TransferProgress(sentBytes, source.totalBytes)
            }
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
        /**
         * Bytes handed to the wire per frame.
         *
         * 1 MiB. Chosen from device measurements, which disagreed with the
         * loopback ones in a useful way: on real Wi-Fi Direct hardware, moving
         * from 64 KiB to 1 MiB took one phone from 3.0 to 11.2 MB/s, while a
         * tablet that was link-limited stayed flat at 6-7. So the per-frame cost
         * is what the sender can control, and it dominates on the faster device.
         *
         * Loopback medians over 32 MiB: 16K 1032, 64K 1455, 256K 1778,
         * 512K 2286, 1M 2286 MB/s. Same conclusion, weaker signal.
         *
         * Capped just below the 1 MiB frame limit rather than at it: the payload
         * length prefix is checked against MAX_FRAME_PAYLOAD on the receiving
         * side, so a chunk that equals the cap leaves no room for metadata or for
         * a future field and starts rejecting frames instead of sending them.
         *
         * Costs, accepted deliberately: with four receive slots this holds about
         * 3.9 MiB awaiting a consumer, plus one encode copy in flight.
         */
        const val DEFAULT_CHUNK_SIZE = 1008 * 1024

        /**
         * Minimum byte distance between progress emissions. 256 KiB keeps a
         * multi-gigabyte transfer to a few thousand updates while still
         * animating smoothly for a human.
         */
        const val PROGRESS_STEP_BYTES = 256L * 1024L
    }
}
