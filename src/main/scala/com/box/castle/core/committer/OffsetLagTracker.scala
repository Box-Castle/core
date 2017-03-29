package com.box.castle.core.committer

import akka.actor.Actor
import com.box.castle.core.committer.states.CommitterActorStates
import com.box.castle.core.const
import com.box.castle.router.RouterRequestManager
import com.box.castle.router.messages.FetchOffset
import org.slf4s.Logging


trait OffsetLagTracker extends CommitterActorBase with CommitterActorStates {
  self: Actor with RouterRequestManager with Logging =>

  private var lastOffsetProcessed: Long = 0

  final def updateLastOffsetProcessed(offset: Long): Unit = {
    lastOffsetProcessed = offset
  }

  def receiveFetchOffset(result: FetchOffset.Result): Unit = {
    checkResult(result) {
      case success: FetchOffset.Success => {
        count(const.Metrics.OffsetLag, math.max(0, success.offset - lastOffsetProcessed))
      }
      case FetchOffset.UnknownTopicOrPartition(failedOffsetType, failedTopicAndPartition) => {
        val msg = s"$committerActorId encountered an unknown topic or partition error " +
          s"when attempting to fetch the latest offset for: $failedTopicAndPartition, $failedOffsetType"
        log.error(msg)
        becomeWaitingToRestart(committerFactory.recoverableExceptionRetryStrategy.delay(0),
          new UnknownTopicOrPartitionException(msg))
      }
    }
  }
}
