package com.box.castle.core.committer.states

import akka.actor.Actor
import com.box.castle.core.committer.CommitterActorBase
import com.box.castle.router.RouterRequestManager
import com.box.castle.router.messages.FetchConsumerOffset
import org.slf4s.Logging


trait FetchingConsumerOffset extends CommitterActorBase with CommitterActorStates {
  self: Actor with RouterRequestManager with Logging =>

  override def becomeFetchingConsumerOffset(): Unit = {
    log.info(s"$committerActorId fetching the last known successfully committed offset")
    context.become(fetchingConsumerOffset)
    sendRequestToRouter(FetchConsumerOffset(consumerId, topicAndPartition))
  }

  override def fetchingConsumerOffset: Receive = {
    case result: FetchConsumerOffset.Result => {
      checkResult(result) {
        case success: FetchConsumerOffset.Success => {
          log.info(s"$committerActorId successfully fetched the last known successfully committed offset of ${success.offsetAndMetadata.offset}")
          becomeFetchingData(success.offsetAndMetadata)
        }
        case _: FetchConsumerOffset.NotFound => {
          log.info(s"$committerActorId could not find a last known successfully committed offset, fetching " +
            s"the ${committerConfig.initialOffset} offset within the topic and partition")
          becomeFetchingOffset(committerConfig.initialOffset, None)
        }
      }
    }
    case msg => receiveCommon(msg)
  }
}
