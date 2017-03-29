package com.box.castle.core.config

import java.util.concurrent.TimeUnit

import com.box.castle.common.Require

import scala.concurrent.duration.FiniteDuration

import LeaderConfig.{DefaultKafkaPollInterval, DefaultAvailableWorkersPollInterval, DefaultLeadershipAcquisitionTimeout}


case class LeaderConfig(kafkaTopicsPollInterval: FiniteDuration = DefaultKafkaPollInterval,
                        availableWorkersPollInterval: FiniteDuration = DefaultAvailableWorkersPollInterval,
                        leadershipAcquisitionTimeout: FiniteDuration = DefaultLeadershipAcquisitionTimeout) {
  Require.positiveDuration(kafkaTopicsPollInterval, "kafkaTopicsPollInterval")
  Require.positiveDuration(availableWorkersPollInterval, "availableWorkersPollInterval")
  Require.positiveDuration(availableWorkersPollInterval, "leadershipAcquisitionTimeout")
}

object LeaderConfig {
  val DefaultKafkaPollInterval = FiniteDuration(10, TimeUnit.SECONDS)
  val DefaultAvailableWorkersPollInterval = FiniteDuration(5, TimeUnit.SECONDS)
  val DefaultLeadershipAcquisitionTimeout = FiniteDuration(60, TimeUnit.SECONDS)
}
