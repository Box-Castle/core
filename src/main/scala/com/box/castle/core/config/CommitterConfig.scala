package com.box.castle.core.config

import java.util.concurrent.Executors

import com.box.castle.core.config.CommitterConfig._
import com.box.castle.core.config.CorruptMessagePolicy.CorruptMessagePolicy
import com.box.castle.core.config.InitialOffset.InitialOffset
import com.box.castle.core.config.OffsetOutOfRangePolicy.OffsetOutOfRangePolicy
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4s.Logging

import org.joda.time.Duration

import scala.concurrent.ExecutionContext
import scala.util.matching.Regex


case class CommitterConfig(private val idRaw: String,
                           factoryClassName: String,
                           customConfig: Map[String, String],
                           private val topicsRegexRaw: Option[String],
                           topicsSet: Option[Set[String]],
                           heartbeatCadenceInMillis: Option[Duration],
                           numThreads: Int = DefaultNumThreads,
                           initialOffset: InitialOffset = DefaultInitialOffset,
                           offsetOutOfRangePolicy: OffsetOutOfRangePolicy = DefaultOffsetOutOfRangePolicy,
                           parallelismFactor: Int = DefaultParallelismFactor,
                           parallelismFactorByTopic: Map[String, Int] = DefaultParallelismFactorByTopic,
                           corruptMessagePolicy: CorruptMessagePolicy = DefaultCorruptMessagePolicy,
                           useKafkaMetadataManager: Boolean = DefaultUseKafkaMetadataManager,
                           samplingSlots: Int = DefaultSamplingSlots,
                           samplingInterval: Duration = DefaultSamplingInterval,
                           maxWaitTime: Duration = DefaultMaxWaitTime,
                           discountFactor: Double = DefaultDiscountFactor,
                           fullBufferThresholdCount: Int = DefaultFullBufferThresholdCount,
                           targetBatchSizePercent: Double = DefaultTargetBatchSizePercent) {
  val id = idRaw.trim()
  require(id.nonEmpty, "Committer id must have at least one character")
  require(id.replaceAll("[^A-Za-z0-9-_]", "_") == id, "Committer id must consist of alphanumeric characters, dashes (-), and underscores (_)")

  require(topicsRegexRaw.isDefined || topicsSet.isDefined, "Must specify either a topics set or regex")

  require(parallelismFactor >= 1, "Parallelism factor must be 1 or greater")

  require(samplingSlots > 1, "Sampling Slots should be greater than 1")

  require(discountFactor > 0 && discountFactor < 1, "Discount Factor should range between 0 to 1")

  parallelismFactorByTopic.foreach {
    case (topic, factor) => {
      require(topic.nonEmpty, "All topics specified in parallelismFactorByTopic must not be empty")
      require(factor >= 1, s"Parallelism factor for topic $topic must be 1 or greater")
    }
  }

  val topicsRegex: Option[Regex] = topicsRegexRaw.map(rawRegex => rawRegex.trim.r)

  // We do this to avoid having to import scala.language.reflectiveCalls
  class CommitterExecutionContext extends ExecutionContext with Logging {
    val threadPool = Executors.newFixedThreadPool(numThreads, new ThreadFactoryBuilder()
      .setNameFormat(s"pool-$id-%d").build())

    def execute(runnable: Runnable): Unit = {
      threadPool.submit(runnable)
    }

    def reportFailure(t: Throwable): Unit = {
      log.error(s"Committer $id threw unhandled exception", t)
    }

    def shutdown(): Unit = {
      log.info(s"Shutting down Committer threadpool for: $id")
      threadPool.shutdown()
    }
  }

  val executionContext = new CommitterExecutionContext()
}

object CommitterConfig {
  val DefaultOffsetOutOfRangePolicy = OffsetOutOfRangePolicy.useOldestOffset
  val DefaultInitialOffset = InitialOffset.oldest
  val DefaultNumThreads = 32
  val DefaultParallelismFactor = 1
  val DefaultParallelismFactorByTopic = Map.empty[String, Int]
  val DefaultCorruptMessagePolicy = CorruptMessagePolicy.skip
  val DefaultUseKafkaMetadataManager = true
  val DefaultTargetBatchSizePercent = 0
  val DefaultSamplingSlots = 20
  val DefaultSamplingInterval = new Duration(60000) // 1 minute
  val DefaultMaxWaitTime = new Duration(300000) // 5 minutes
  val DefaultDiscountFactor = 0.8
  val DefaultFullBufferThresholdCount = 3
}