package com.box.castle.core.committer

import akka.actor.Props
import com.box.castle.committer.api.CommitterFactory
import com.box.castle.core.common.CastleFatalException
import com.box.castle.core.config.{CastleConfig, CommitterConfig}
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterRef

// $COVERAGE-OFF$

class CommitterActorFactory(castleConfig: CastleConfig,
                            committerFactoryMap: Map[String, CommitterFactory],
                            metricsLogger: MetricsLogger) {

  def props(topic: String, partition: Int, committerConfig: CommitterConfig, router: RouterRef): Props = {
    if (committerFactoryMap.contains(committerConfig.id)) {
      val committerFactory = committerFactoryMap(committerConfig.id)
      Props(new CommitterActor(topic, partition, castleConfig, committerConfig, committerFactory, router, metricsLogger))
    }
    else {
      throw new CastleFatalException(s"No committer factory found for committer id: ${committerConfig.id}")
    }
  }
}

// $COVERAGE-ON$