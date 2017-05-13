package com.box.castle.core.committer.states

import akka.actor.Actor
import com.box.castle.consumer.{EarliestOffset, LatestOffset}
import com.box.castle.core.committer._
import com.box.castle.core.config.{CorruptMessagePolicy, OffsetOutOfRangePolicy}
import com.box.castle.core.const
import com.box.castle.router.RouterRequestManager
import com.box.castle.router.messages.{FetchData, FetchOffset, OffsetAndMetadata}
import org.slf4s.Logging


trait FetchingData extends CommitterActorBase
  with CommitterActorStates
  with OffsetLagTracker {
  self: Actor with RouterRequestManager with Logging =>

  private var fetchStartTime: Long = 0
  private[core] var consumerMetadata: Option[String] = None

  private def receiveFetchDataResult(result: FetchData.Result): Unit = {
    time(const.Metrics.FetchTime, System.nanoTime() - fetchStartTime)

    checkResult(result) {

      case success: FetchData.Success =>
        becomePreparingToCommitBatch(success, consumerMetadata)

      case noMessages: FetchData.NoMessages =>
        becomePreparingToCommitBatch(noMessages, consumerMetadata)

      case FetchData.UnknownTopicOrPartition(failedTopicAndPartition, failedOffset) => {
        val msg = s"$committerActorId encountered an unknown topic or partition error " +
          s"when attempting to fetch data for: $failedTopicAndPartition, $failedOffset"
        log.error(msg)
        becomeWaitingToRestart(committerFactory.recoverableExceptionRetryStrategy.delay(0),
          new UnknownTopicOrPartitionException(msg))
      }
      case FetchData.OffsetOutOfRange(_, offset) => {
        log.warn(s"$committerActorId encountered an offset out of range error for offset: $offset, using " +
          s"the following offset out of range policy: ${committerConfig.offsetOutOfRangePolicy}")
        committerConfig.offsetOutOfRangePolicy match {
          case OffsetOutOfRangePolicy.useOldestOffset => becomeFetchingOffset(EarliestOffset, None)
          case OffsetOutOfRangePolicy.useLatestOffset => becomeFetchingOffset(LatestOffset, None)
          case OffsetOutOfRangePolicy.fail => {
            throw new UnrecoverableCommitterActorException(committerConfig.id, s"$committerActorId encountered an " +
              s"offset out of range exception for offset $offset and is failing with an unrecoverable exception " +
              s"as specified by the OffsetOutOfRangePolicy, no further messages will be consumed from $topicAndPartition")
          }
        }
      }
      case FetchData.CorruptMessage(_, offset, nextOffset) => {
        committerConfig.corruptMessagePolicy match {
          case CorruptMessagePolicy.skip => {
            log.warn(s"$committerActorId encountered and skipped a corrupt message at offset $offset " +
              s"as specified by the CorruptMessagePolicy")
            count(const.Metrics.CorruptMessagesSkipped)
            becomeFetchingData(OffsetAndMetadata(nextOffset, consumerMetadata))
          }
          case CorruptMessagePolicy.retry => {
            log.warn(s"$committerActorId encountered a corrupt message at offset $offset " +
              s"and is fetching this offset again as specified by the CorruptMessagePolicy")
            // TODO: This should be fetched with a delay specified a strategy provided by the committer factory
            becomeFetchingData(OffsetAndMetadata(offset, consumerMetadata))
          }
          case CorruptMessagePolicy.fail => {
            throw new UnrecoverableCommitterActorException(committerConfig.id, s"$committerActorId encountered a " +
              s"corrupt message at offset $offset and is failing with an unrecoverable exception as specified by " +
              s"by the CorruptMessagePolicy, no further messages will be consumed from $topicAndPartition")
          }
        }
      }
    }
  }

  override def becomeFetchingData(offsetAndMetadata: OffsetAndMetadata): Unit = {
    fetchStartTime = System.nanoTime()
    consumerMetadata = offsetAndMetadata.metadata

    context.become(fetchingData)
    sendRequestToRouter(FetchData(topicAndPartition, offsetAndMetadata.offset))
  }

  override def fetchingData: Receive = {
    case result: FetchData.Result => receiveFetchDataResult(result)
    case result: FetchOffset.Result => receiveFetchOffset(result)
    case msg => receiveCommon(msg)
  }

}
