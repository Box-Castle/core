package com.box.castle.core.committer.states

import akka.actor.Actor
import com.box.castle.batch.CastleMessageBatch
import com.box.castle.committer.api.Committer
import com.box.castle.consumer.OffsetType
import com.box.castle.router.messages.OffsetAndMetadata
import org.joda.time.Duration

import scala.concurrent.duration.FiniteDuration

/**
  *
  * This trait encapsulates all the possible states that a CommitterActor can be in
  */
trait CommitterActorStates {
  self: Actor =>

  def becomeFetchingConsumerOffset(): Unit

  def fetchingConsumerOffset: Receive

  def becomeFetchingOffset(offsetType: OffsetType, metadata: Option[String]): Unit

  def fetchingOffset: Receive

  def becomeWaitingToRestart(delay: FiniteDuration, t: Throwable): Unit

  def waitingToRestart: Receive

  def becomePreparingToCommitBatch(batch: CastleMessageBatch, metadata: Option[String]): Unit

  def preparingToCommitBatch: Receive

  def becomeCommittingBatch(userCommitters: IndexedSeq[Committer], batch: CastleMessageBatch, metadata: Option[String]): Unit

  def committingBatch: Receive

  def becomeFetchingData(offsetAndMetadata: OffsetAndMetadata): Unit

  def fetchingData: Receive

  def becomeShuttingDown(): Unit

  def shuttingDown: Receive

  def becomeIdling(offsetAndMetadata: OffsetAndMetadata, delayOption: Option[Duration] = None): Unit

  def idling: Receive
}
