package com.box.castle.core.committer.states

import akka.actor.Actor
import com.box.castle.core.committer.CommitterActorBase
import com.box.castle.core.const
import com.box.castle.router.RouterRequestManager
import org.slf4s.Logging

import scala.concurrent.duration.FiniteDuration


trait WaitingToRestart extends CommitterActorBase with CommitterActorStates {
  self: Actor with RouterRequestManager with Logging =>

  private case class RestartCommitterActor(t: Throwable)

  override def becomeWaitingToRestart(delay: FiniteDuration, t: Throwable): Unit = {
    log.info(s"$committerActorId restarting itself in $delay")
    context.become(waitingToRestart)
    scheduleOnce(delay, RestartCommitterActor(t))
  }

  override def waitingToRestart: Receive = {
    case RestartCommitterActor(t) => {
      log.info(s"$committerActorId is restarting itself due to ${t.getMessage}")
      count(const.Metrics.RecoverableFailures)
      throw t
    }
    case msg => receiveCommon(msg)
  }
}
