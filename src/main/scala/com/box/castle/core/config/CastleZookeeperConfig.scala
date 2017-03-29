package com.box.castle.core.config

import java.util.concurrent.TimeUnit.SECONDS
import com.box.castle.core.config.CastleZookeeperConfig.{DefaultConnectionTimeout,
  DefaultInitialConnectTimeout, DefaultRetryPolicy, DefaultRoot, DefaultSessionTimeout}
import com.box.castle.common.Require
import org.apache.curator.RetryPolicy
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.concurrent.duration.{Duration, FiniteDuration}


case class CastleZookeeperConfig(connect: String,
                                 root: String = DefaultRoot,
                                 sessionTimeout: Duration = DefaultSessionTimeout,
                                 connectionTimeout: Duration = DefaultConnectionTimeout,
                                 initialConnectTimeout: Duration = DefaultInitialConnectTimeout,
                                 retryPolicy: RetryPolicy = DefaultRetryPolicy) {
  Require.positiveDuration(sessionTimeout, "sessionTimeout")
  Require.positiveDuration(connectionTimeout, "connectionTimeout")
  Require.positiveDuration(initialConnectTimeout, "initialConnectTimeout")
}

object CastleZookeeperConfig {

  val DefaultRoot = "castle"
  val DefaultSessionTimeout = FiniteDuration(60, SECONDS)
  val DefaultConnectionTimeout = FiniteDuration(15, SECONDS)
  val DefaultInitialConnectTimeout = FiniteDuration(30, SECONDS)

  val DefaultExponentialBackoffBaseSleepTime = FiniteDuration(1, SECONDS)
  val DefaultExponentialBackoffMaxRetries: Int = 10
  val DefaultExponentialBackoffMaxSleepTime = FiniteDuration(60, SECONDS)

  val DefaultRetryPolicyType = "exponentialBackoff"

  val DefaultRetryPolicy = new ExponentialBackoffRetry(
    DefaultExponentialBackoffBaseSleepTime.toMillis.toInt,
    DefaultExponentialBackoffMaxRetries,
    DefaultExponentialBackoffMaxSleepTime.toMillis.toInt)
}