package com.box.castle.core.leader

import com.box.castle.core.CuratorFactory
import com.box.castle.core.config.{CommitterConfig, LeaderConfig}
import com.box.castle.metrics.MetricsLogger
import com.box.castle.consumer.ClientId



class LeaderFactory(leaderConfig: LeaderConfig,
                    clientId: ClientId,
                    curatorFactory: CuratorFactory,
                    taskManager: TaskManager,
                    metricsLogger: MetricsLogger) {

  def create(leaderActor: LeaderActorRef) =
    new Leader(leaderActor, leaderConfig, clientId, curatorFactory, taskManager, metricsLogger)
}
