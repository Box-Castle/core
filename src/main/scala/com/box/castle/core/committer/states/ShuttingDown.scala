package com.box.castle.core.committer.states

import akka.actor.Actor
import com.box.castle.core.committer.{ConsumerOffsetCommitTracker, CommitterActorBase}
import com.box.castle.router.RouterRequestManager
import com.box.castle.router.messages.CommitConsumerOffset
import org.slf4s.Logging


trait ShuttingDown extends CommitterActorBase
    with CommitterActorStates
    with ConsumerOffsetCommitTracker {

  self: Actor with RouterRequestManager with Logging =>

  override def becomeShuttingDown(): Unit = {
    context.become(shuttingDown)
    addGracefulShutdownHook()
  }

  override def shuttingDown: Receive = {
    case result: CommitConsumerOffset.Result => receiveCommitConsumerOffsetResult(result)
    case _ => log.info(s"$committerActorId is currently shutting down, ignoring all messages...")
  }
}
