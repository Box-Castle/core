package com.box.castle.core.committer.manager

import com.box.castle.core.config.CommitterConfig
import org.joda.time.Duration
import org.specs2.mutable.Specification

/**
  * Unit Tests for BatchSizeManager
  */
class BatchSizeManagerTest extends Specification {

  val samplingSlots = 3
  val samplingInterval = new Duration(60000) // 1 minute
  val maxWaitTime = new Duration(300000) // 5 minutes
  val discountFactor = 0.9
  val threshold = 3
  val committerConfig = CommitterConfig("dummyId","dummyClass",Map.empty,None, Some(Set("dummyTopic")), None, useBatchSizeManager = true,
    samplingSlots = samplingSlots, samplingInterval = samplingInterval, maxWaitTime = maxWaitTime,
    discountFactor = discountFactor, fullBufferThresholdCount = threshold)

  "track" should {
    "track all samples initially when queue is not full" in {
      // Setup

      val bufferSize = 1200 //bytes
      val bytesRead = 40
      val batchSizeManager = new BatchSizeManager("dummyCommitter", committerConfig, bufferSize)

      // Execute
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      batchSizeManager.track(bytesRead, System.currentTimeMillis())

      // Validate
      // All samples should be tracked since queue is initially empty
      batchSizeManager.size shouldEqual samplingSlots
      batchSizeManager.getLastSample.bytesRead shouldEqual 120

    }

    "only keep samples occurring after specified sample interval after queue is full" in {
      // Setup
      val bufferSize = 1200 //bytes
      val bytesRead = 40
      val batchSizeManager = new BatchSizeManager("dummyCommitter", committerConfig, bufferSize)

      // Execute
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      val lastTimestamp = System.currentTimeMillis()
      batchSizeManager.track(bytesRead, lastTimestamp)
      // This should be discarded as sampling interval is 1 min
      batchSizeManager.track(bytesRead, lastTimestamp + 1000)

      // Validate
      batchSizeManager.size shouldEqual samplingSlots
      batchSizeManager.getLastSample.bytesRead shouldEqual 120

      // This sample should be added
      batchSizeManager.track(bytesRead, lastTimestamp + 70000)
      batchSizeManager.size shouldEqual samplingSlots
      batchSizeManager.getLastSample.bytesRead shouldEqual 160

    }
  }

  "getDelay" should {
    "compute delay based on tracked samples" in {
      // Setup
      val bufferSize = 100 //bytes
      val bytesRead = 10
      val batchSizeManager = new BatchSizeManager("dummyCommitter", committerConfig, bufferSize)

      // Execute
      val ts = System.currentTimeMillis()
      batchSizeManager.track(bytesRead, ts)
      batchSizeManager.track(bytesRead, ts + 100)
      batchSizeManager.track(bytesRead, ts + 200)

      // Validate
      val expectedResult = (bufferSize / 0.1) * discountFactor
      batchSizeManager.getDelay.getMillis shouldEqual expectedResult.toLong
    }

    "return zero delay when there is zero or one sample" in {
      // Setup
      val bufferSize = 100 //bytes
      val bytesRead = 10
      val batchSizeManager = new BatchSizeManager("dummyCommitter", committerConfig, bufferSize)

      // Validate
      batchSizeManager.getDelay.getMillis shouldEqual 0

      // Add one sample
      batchSizeManager.track(bytesRead, System.currentTimeMillis())

      batchSizeManager.getDelay.getMillis shouldEqual 0

    }

    "return zero delay if we have seen fixed number of consecutive full buffer reads" in {
      // Setup
      val bufferSize = 100 //bytes
      val bytesRead = 10
      val batchSizeManager = new BatchSizeManager("dummyCommitter", committerConfig, bufferSize)

      // Execute
      batchSizeManager.track(bytesRead,System.currentTimeMillis())
      batchSizeManager.track(bytesRead,System.currentTimeMillis())
      batchSizeManager.track(bufferSize,System.currentTimeMillis())
      batchSizeManager.track(bufferSize,System.currentTimeMillis())
      batchSizeManager.track(bufferSize,System.currentTimeMillis())
      batchSizeManager.track(bufferSize,System.currentTimeMillis())

      // Validate
      batchSizeManager.getDelay.getMillis shouldEqual 0
      batchSizeManager.size shouldEqual 0

    }

  }

}
