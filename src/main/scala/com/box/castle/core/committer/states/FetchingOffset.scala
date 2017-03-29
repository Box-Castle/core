package com.box.castle.core.committer.states

import akka.actor.Actor
import com.box.castle.consumer.OffsetType
import com.box.castle.core.committer.{UnknownTopicOrPartitionException, CommitterActorBase}
import com.box.castle.router.RouterRequestManager
import com.box.castle.router.messages.{OffsetAndMetadata, FetchOffset}
import org.slf4s.Logging


trait FetchingOffset extends CommitterActorBase with CommitterActorStates {
  self: Actor with RouterRequestManager with Logging =>

  override def becomeFetchingOffset(offsetType: OffsetType, metadata: Option[String]): Unit = {
    context.become(fetchingOffset)
    sendRequestToRouter(FetchOffset(offsetType, topicAndPartition))
  }

  override def fetchingOffset: Receive = {
    case result: FetchOffset.Result => {
      checkResult(result) {
        case success: FetchOffset.Success => {
          log.info(s"$committerActorId successfully fetched the ${success.offsetType} offset: ${success.offset}")
          becomeFetchingData(OffsetAndMetadata(success.offset, None))
        }
        case FetchOffset.UnknownTopicOrPartition(failedOffsetType, failedTopicAndPartition) => {
          val msg = s"$committerActorId encountered an unknown topic or partition error " +
            s"when attempting to fetch the initial offset for: $failedTopicAndPartition, $failedOffsetType"
          log.error(msg)
          becomeWaitingToRestart(committerFactory.recoverableExceptionRetryStrategy.delay(0),
            new UnknownTopicOrPartitionException(msg))
        }
      }
    }
    case msg => receiveCommon(msg)
  }
}
