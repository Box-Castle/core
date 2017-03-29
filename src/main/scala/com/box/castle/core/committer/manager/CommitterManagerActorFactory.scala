package com.box.castle.core.committer.manager

import akka.actor.{ActorRef, Props}
import com.box.castle.core.committer.CommitterActorFactory
import com.box.castle.core.config.CommitterConfig
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterRef
import com.box.castle.core.worker.tasks.Task

// $COVERAGE-OFF$

class CommitterManagerActorFactory(committerActorFactory: CommitterActorFactory,
                          committerConfigs: Iterable[CommitterConfig], metricsLogger: MetricsLogger) {

  def props(assignedTask: Task, router: RouterRef): Props = {
    Props(new CommitterManagerActor(assignedTask,
      router,
      committerConfigs,
      committerActorFactory,
      metricsLogger))
  }
}
// $COVERAGE-ON$