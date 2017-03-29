package com.box.castle.core.worker

import akka.actor.Props
import com.box.castle.core.committer.manager.CommitterManagerActorFactory
import com.box.castle.core.config.CommitterConfig
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterRef

// $COVERAGE-OFF$



class WorkerActorFactory(val committerManagerActorFactory: CommitterManagerActorFactory,
                         val workerFactory: WorkerFactory,
                         val committerConfigs: Iterable[CommitterConfig],
                         val metricsLogger: MetricsLogger) {

  def props(router: RouterRef): Props =
    Props(new WorkerActor(workerFactory, router, committerManagerActorFactory, committerConfigs, metricsLogger))
}

// $COVERAGE-ON$