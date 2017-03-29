package com.box.castle.core.config

import java.util.concurrent.TimeUnit

import CastleConfig.{DefaultCacheSizeInBytes, DefaultBufferSizeInBytes, DefaultBrokerTimeout, DefaultGracefulShutdownTimeout}
import com.box.castle.router.RouterConfig
import com.box.kafka.Broker

import scala.concurrent.duration.FiniteDuration


case class CastleConfig(private val namespaceRaw: String,
                        brokers: Set[Broker],
                        leaderConfig: LeaderConfig,
                        committerConfigs: Iterable[CommitterConfig],
                        castleZookeeperConfig: CastleZookeeperConfig,
                        routerConfig: RouterConfig,
                        brokerTimeout: FiniteDuration = DefaultBrokerTimeout,
                        bufferSizeInBytes: Int = DefaultBufferSizeInBytes,
                        cacheSizeInBytes: Long = DefaultCacheSizeInBytes,
                        gracefulShutdownTimeout: FiniteDuration = DefaultGracefulShutdownTimeout) {
  require(bufferSizeInBytes > 0, "bufferSizeInBytes must be positive")
  require(brokers.nonEmpty, "must specify at least one broker")

  require(committerConfigs.nonEmpty, "Must specify at least one committer config")
  require(committerConfigs.map(cfg => cfg.id).toSet.size == committerConfigs.size, "Committer ids must be unique")

  val namespace = namespaceRaw.trim()
  require(namespace.replaceAll("[^A-Za-z0-9-_]", "_") == namespace, "Castle namespace must consist of alphanumeric characters, dashes (-), and underscores (_)")
}

object CastleConfig {
  val DefaultBufferSizeInBytes: Int = (1024 * 1024 * 4) - 128 // 4 MB minus overhead
  val DefaultCacheSizeInBytes: Long = 1024 * 1024 * 1024 // 1 GB
  val DefaultBrokerTimeout = FiniteDuration(60, TimeUnit.SECONDS)
  val DefaultGracefulShutdownTimeout = FiniteDuration(10, TimeUnit.SECONDS)
}