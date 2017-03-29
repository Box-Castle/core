package com.box.castle.core.leader

import akka.actor.Props
import com.box.castle.core.config.LeaderConfig
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterRef



class LeaderActorFactory(leaderFactory: LeaderFactory,
                         leaderConfig: LeaderConfig,
                         metricsLogger: MetricsLogger) {

  def props(router: RouterRef): Props = {
    Props(new LeaderActor(leaderFactory, leaderConfig, router, metricsLogger))
  }
}
