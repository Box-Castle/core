package com.box.castle.core.supervisor

import akka.actor.Props
import com.box.castle.committer.api.CommitterFactory
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterFactory
import com.box.castle.core.leader.LeaderActorFactory
import com.box.castle.core.worker.WorkerActorFactory

// $COVERAGE-OFF$

/**
 * This is the top level supervisor for the Castle service that has
 * the Worker, Leader, and Dispatcher actors as children.
 */
class CastleSupervisorFactory(val workerActorFactory: WorkerActorFactory,
                              val leaderActorFactory: LeaderActorFactory,
                              val dispatcherActorFactory: RouterFactory,
                              val committerFactoryMap: Map[String, CommitterFactory],
                              val metricsLogger: MetricsLogger) {
  def props(): Props = {
    Props(new CastleSupervisor(workerActorFactory,
                               leaderActorFactory,
                               dispatcherActorFactory,
                               committerFactoryMap,
                               metricsLogger))
  }
}

// $COVERAGE-ON$