package com.box.castle.core.committer.states

import akka.actor.Actor
import com.box.castle.batch.CastleMessageBatch
import com.box.castle.collections._
import com.box.castle.committer.api._
import com.box.castle.consumer.LatestOffset
import com.box.castle.core.committer._
import com.box.castle.core.committer.manager.BatchSizeManager
import com.box.castle.core.const
import com.box.castle.router.RouterRequestManager
import com.box.castle.router.messages.{CommitConsumerOffset, FetchOffset, OffsetAndMetadata}
import org.slf4s.Logging

import scala.concurrent.Future
import scala.util.{Failure, Success}


trait CommittingBatch extends CommitterActorBase
    with CommitterActorStates
    with OffsetLagTracker
    with ConsumerOffsetCommitTracker {

  self: Actor with RouterRequestManager with Logging =>

  private case class ProcessUserCommitterException(t: Throwable, endTime: Long)
  private case class ProcessUserCommitterFactoryException(t: Throwable)
  private case class ReprocessMessageBatch(castleMessageBatch: CastleMessageBatch, chunkAndCommitter: ChunkAndCommitter,
                                           metadata: Option[String])
  private case class SuccessfullyCommittedBatch(batch: CastleMessageBatch, endTime: Long, metadata: Option[String])
  private case class SuccessfullyCommittedChunk(batch: CastleMessageBatch, committer: Committer, metadata: Option[String])

  private var commitStartTime: Long = 0

  // Set of committers that have not successfully committed their message batch yet
  private var pendingCommitters = Set.empty[Committer]

  // Keeps track of reads and configures delays before next fetch
  private val batchSizeManagerOption = if(committerConfig.targetBatchSizePercent > 0)
    Some(new BatchSizeManager(committerConfig, castleConfig.bufferSizeInBytes)) else None

  /**
   * A batch will be splited evenly into chunks based on the parallelism factor
   * and be committed with metadata along side with the batch offset.
   * In this version, for metadata it only supports parallelism factor = 1,
   * in other words, if metadata = Some(String) && size(chunks) != 1,
   * this function will throw an unrecoverable exception
   * @param chunksAndCommitters
   * @param batch
   */
  private def commitChunksWithCommitters(chunksAndCommitters: IndexedSeq[ChunkAndCommitter],
                                         batch: CastleMessageBatch, metadata: Option[String]): Unit = {
    //This is a temporary check to prevent sending batch metadata to multiple committer instances
    checkMetadataAndParallelismFactor(metadata, chunksAndCommitters.size)

    val userCommitAsyncFuture = Future.traverse(chunksAndCommitters) {
      case ChunkAndCommitter(chunk, userCommitter) =>
        Future(userCommitter.commitAsync(chunk, metadata))
          .flatMap(identity)
          .map {
          case result: CommitResult => CommitterAndResult(userCommitter, result)
          case null => throw new NullPointerException(s"Committer actor: ${committerActorId} got nulll from user committer: ${committerConfig.id}")
        }
    }

    userCommitAsyncFuture onComplete {
      case Success(committerAndResults) => {
        committerAndResults foreach {
          case CommitterAndResult(committer: Committer, success: BatchCommittedSuccessfully) =>
            context.self ! SuccessfullyCommittedChunk(batch, committer, success.metadata)
          case CommitterAndResult(committer: Committer, retry: RetryBatch) =>
            val partialChunkAndCommitter = ChunkAndCommitter(retry.messageBatch, committer)
            val reprocessMessage = ReprocessMessageBatch(batch, partialChunkAndCommitter,retry.metadata)
            scheduleOnce(retry.retryAfter, reprocessMessage)
        }
      }
      case Failure(t) => {
        context.self ! ProcessUserCommitterException(t, System.nanoTime())
      }
    }
  }

  /**
   * To check if metadat is not None and parallelismFactor larger than one
   * @param metadata it's up to the caller to decide what does metadata mean in the context,
   *                 it could either mean the metadate framework passing to the user committer,
   *                 or the metadata user committer sending back
   * @param parallelismFactor it's up to the caller to decide what should be the right parallelismFactor
   *                          in the context, it could either be the size of user committer instances or
   *                          directly from committer config
   */
  private def checkMetadataAndParallelismFactor(metadata: Option[String], parallelismFactor: Int): Unit = {
    metadata match {
      case Some(data) => if (parallelismFactor != 1)
        throw new UnrecoverableCommitterActorException(committerConfig.id,
          s"Committing with metadata currently only supports parallelism factor equal to one. " +
            s"Your factor is ${parallelismFactor}")
      case None =>
    }
  }


  private def receiveSuccessfullyCommittedChunk(batch: CastleMessageBatch, committer: Committer, metadata: Option[String]) = {
    // remove committer from Set of pending committers
    pendingCommitters -= committer

    if (pendingCommitters.isEmpty) {
      // all committers are done. Send success message
      context.self ! SuccessfullyCommittedBatch(batch, System.nanoTime(), metadata)
    }
  }

  private def receiveSuccessfullyCommittedBatch(batch: CastleMessageBatch, endTime: Long, metadata: Option[String]): Unit = {
    time(const.Metrics.CommitTime, endTime - commitStartTime)
    count(const.Metrics.NumMessagesCommitted, batch.size)
    count(const.Metrics.BytesProcessed, batch.sizeInBytes)

    updateLastOffsetProcessed(batch.nextOffset)

    log.info(s"$committerActorId finished processing ${batch.offset}, next offset: ${batch.nextOffset}, " +
      s"messages: ${batch.size}, size in bytes: ${batch.sizeInBytes}")

    // We commit the consumer offset here without waiting on the result, we are assuming it will succeed most of the time
    commitConsumerOffset(batch.nextOffset, metadata)

    // If BatchSizeManager is enabled then goto Idling state with a specific non-zero delay
    batchSizeManagerOption match {
      case Some(batchSizeManager) =>
        val delay = batchSizeManager.getDelay(committerActorId)
        if(delay.getMillis > 0)
          becomeIdling(OffsetAndMetadata(batch.nextOffset, metadata), delay)
        else {
          log.info(s"$committerActorId fetching immediately because BatchSizeManager returned a delay of 0")
          becomeFetchingData(OffsetAndMetadata(batch.nextOffset, metadata))
        }
      case None =>
        becomeFetchingData(OffsetAndMetadata(batch.nextOffset, metadata))
    }
  }

  private def receiveUserCommitterException(t: Throwable, endTime: Long): Unit = {
    time(const.Metrics.CommitTime, endTime - commitStartTime)
    if (t.isInstanceOf[UnrecoverableException]) {
      log.error(s"$committerActorId encountered an unrecoverable exception thrown by the user committer", t)
      throw new UnrecoverableCommitterActorException(committerConfig.id, t.getMessage, t)
    }
    else {
      // TODO: implement try number properly for no data instead of using 0
      val delay = committerFactory.recoverableExceptionRetryStrategy.delay(0)
      log.error(s"$committerActorId encountered a recoverable exception thrown by the user committer.")
      becomeWaitingToRestart(delay, t)
    }
  }

  private def commitBatch(userCommitters: IndexedSeq[Committer], batch: CastleMessageBatch, metadata: Option[String]): Unit = {
    beginCommittingBatch(batch.nextOffset)

    val chunksAndCommitters =
      splitEvenly(batch.messageSeq, userCommitters.size, excludeZeroSizeBuckets = true)
        .zip(userCommitters)
        .map { case (chunk, committer) => ChunkAndCommitter(chunk, committer) }

    // set all committers that have a message chunk as pending
    pendingCommitters = chunksAndCommitters.map(_.committer).toSet

    commitChunksWithCommitters(chunksAndCommitters, batch, metadata)
  }

  override def becomeCommittingBatch(userCommitters: IndexedSeq[Committer], batch: CastleMessageBatch, metadata: Option[String]): Unit = {
    // We fetch the latest offset in the topic here so we can compute the offset lag
    sendRequestToRouter(FetchOffset(LatestOffset, topicAndPartition))

    // Track bytes read from Kafka if BatchSizeManager is enabled
    batchSizeManagerOption.foreach(_.track(batch.sizeInBytes, System.currentTimeMillis()))

    commitStartTime = System.nanoTime()
    context.become(committingBatch)

    commitBatch(userCommitters, batch, metadata)
  }

  override def committingBatch: Receive = {
    case ReprocessMessageBatch(batch, chunkAndCommitter, metadata) =>
      commitChunksWithCommitters(IndexedSeq(chunkAndCommitter), batch, metadata)
    case SuccessfullyCommittedChunk(batch, committer, metadata) => receiveSuccessfullyCommittedChunk(batch, committer, metadata)
    case SuccessfullyCommittedBatch(batch, endTime, metadata) => receiveSuccessfullyCommittedBatch(batch, endTime, metadata)
    case ProcessUserCommitterException(t, endTime) => receiveUserCommitterException(t, endTime)
    case result: CommitConsumerOffset.Result => receiveCommitConsumerOffsetResult(result)
    case result: FetchOffset.Result => receiveFetchOffset(result)
    case msg => receiveCommon(msg)
  }

}
