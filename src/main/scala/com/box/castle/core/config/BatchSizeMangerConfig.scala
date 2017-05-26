package com.box.castle.core.config

import org.joda.time.Duration
import BatchSizeMangerConfig._

case class BatchSizeMangerConfig(samplingSlots: Int = DefaultSamplingSlots,
                                 samplingInterval: Duration = DefaultSamplingInterval,
                                 maxWaitTime: Duration = DefaultMaxWaitTime,
                                 discountFactor: Double = DefaultDiscountFactor,
                                 fullBufferThresholdCount: Int = DefaultFullBufferThresholdCount,
                                 emptyBufferThresholdCount: Int = DefaultEmptyBufferThresholdCount,
                                 targetBatchSizePercent: Double = DefaultTargetBatchSizePercent) {

  require(samplingSlots > 1, "Sampling Slots should be greater than 1")

  require(discountFactor > 0 && discountFactor < 1, "Discount Factor should range between 0 to 1")

}


object BatchSizeMangerConfig {
  val DefaultTargetBatchSizePercent = 0
  val DefaultSamplingSlots = 20
  val DefaultSamplingInterval = new Duration(60000) // 1 minute
  val DefaultMaxWaitTime = new Duration(300000) // 5 minutes
  val DefaultDiscountFactor = 0.8
  val DefaultFullBufferThresholdCount = 3
  val DefaultEmptyBufferThresholdCount = 3
}
