package com.box.castle.core.committer

import akka.actor.Actor
import com.box.castle.core.committer.states.CommitterActorStates
import com.box.castle.router.RouterRequestManager
import com.box.castle.router.messages.{OffsetAndMetadata, CommitConsumerOffset}
import org.slf4s.Logging

import scala.concurrent.{Future, Promise}


trait ConsumerOffsetCommitTracker extends CommitterActorBase with CommitterActorStates {
  self: Actor with RouterRequestManager with Logging =>

  /**
   * We need to track all outstanding consumer offset commits because we optimistically commit them, which means
   * at any given time we could potentially have N outstanding consumer offset commits while we continue to
   * fetch more batches and advance the offset within the committer.
   */
  private var outstandingConsumerOffsetCommitPromises: Map[Long, Promise[Any]] = Map.empty

  def commitConsumerOffset(offset: Long, metadata: Option[String]): Unit = {
    sendRequestToRouter(CommitConsumerOffset(consumerId, topicAndPartition, OffsetAndMetadata(offset, metadata)))
  }

  def beginCommittingBatch(batchOffset: Long): Unit = {
    // Starting a commit. This Promise will be completed when the batch and its offset are committed
    outstandingConsumerOffsetCommitPromises += (batchOffset -> Promise[Any]())
  }

  def receiveCommitConsumerOffsetResult(result: CommitConsumerOffset.Result): Unit = {
    checkResult(result) { response =>
      response match {
        case CommitConsumerOffset.UnknownTopicOrPartition(failedConsumerId, failedTopicAndPartition, failedOffsetAndMetadata) => {
          val msg = s"$committerActorId encountered an unknown topic or partition error " +
            s"when attempting to commit consumer offset for: $failedConsumerId, $failedTopicAndPartition, ${failedOffsetAndMetadata.offset}"
          log.error(msg)
          becomeWaitingToRestart(committerFactory.recoverableExceptionRetryStrategy.delay(0),
            new UnknownTopicOrPartitionException(msg))
        }
        case _: CommitConsumerOffset.Success    => // Success
        case _: CommitConsumerOffset.Superseded => // Superseded
      }

      // Offset committed successfully. Complete promise for the offset here
      outstandingConsumerOffsetCommitPromises(response.offsetAndMetadata.offset).success(())

      // remove the completed promise from the map to keep it from growing indefinitely
      outstandingConsumerOffsetCommitPromises -= response.offsetAndMetadata.offset
    }
  }

  def addGracefulShutdownHook(): Unit = {
    Future.sequence(outstandingConsumerOffsetCommitPromises.values.map(_.future)) onComplete (_ => {
      log.info(s"$committerActorId finished processing the last outstanding batch, stopping the actor")
      context.stop(context.self)
    })
  }
}
