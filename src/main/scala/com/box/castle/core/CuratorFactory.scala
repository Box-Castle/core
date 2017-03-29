package com.box.castle.core

import com.box.castle.core.common.{CastleFatalException}
import com.box.castle.core.config.CastleConfig
import org.slf4s.Logging
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}

import scala.concurrent.duration._

// $COVERAGE-OFF$

class CuratorFactory(castleConfig: CastleConfig) extends Logging {

  def create(): CuratorFramework = {
    log.info("Begin creating Curator...")
    val curator: CuratorFramework = CuratorFrameworkFactory.builder
      .namespace(util.join(castleConfig.castleZookeeperConfig.root, castleConfig.namespace))
      .connectString(castleConfig.castleZookeeperConfig.connect)
      .sessionTimeoutMs(castleConfig.castleZookeeperConfig.sessionTimeout.toMillis.toInt)
      .connectionTimeoutMs(castleConfig.castleZookeeperConfig.connectionTimeout.toMillis.toInt)
      .retryPolicy(castleConfig.castleZookeeperConfig.retryPolicy)
      .build

    curator.start()
    curator.blockUntilConnected(castleConfig.castleZookeeperConfig.initialConnectTimeout.toMillis.toInt, MILLISECONDS)
    if (curator.getState != CuratorFrameworkState.STARTED) {
      throw new CastleFatalException(s"Could not connect to ZK within the " +
        s"configured timeout of ${castleConfig.castleZookeeperConfig.initialConnectTimeout}")
    }

    log.info("Successfully created a CuratorFramework instance")
    curator
  }
}

// $COVERAGE-ON$