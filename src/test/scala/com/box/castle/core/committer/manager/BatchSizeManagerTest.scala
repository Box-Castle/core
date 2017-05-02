package com.box.castle.core.committer.manager

import org.specs2.mutable.Specification

/**
  * Unit Tests for BatchSizeManager
  */
class BatchSizeManagerTest extends Specification {


  "track" should {
    "track fixed number of samples only" in {
      // Setup
      val samplingSlots = 3
      val samplingInterval = 100 // milliseconds
      val bufferSize = 100 //bytes
      val bytesRead = 10
      val batchSizeManager = new BatchSizeManager(samplingSlots, samplingInterval, bufferSize)

      // Execute
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      Thread.sleep(samplingInterval)
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      Thread.sleep(samplingInterval)
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      Thread.sleep(samplingInterval)
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      Thread.sleep(samplingInterval)
      batchSizeManager.track(bytesRead, System.currentTimeMillis())

      // Validate
      batchSizeManager.size shouldEqual samplingSlots

    }

    "only keep samples occurring after specified sample interval" in {
      // Setup
      val samplingSlots = 3
      val samplingInterval = 1000 // milliseconds
      val bufferSize = 100 //bytes
      val bytesRead = 10
      val batchSizeManager = new BatchSizeManager(samplingSlots, samplingInterval, bufferSize)

      // Execute
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      batchSizeManager.track(bytesRead, System.currentTimeMillis()) // This one should be discarded

      // Validate
      batchSizeManager.size shouldEqual 1

    }
  }

  "getDelay" should {
    "compute delay based on tracked samples" in {
      // Setup
      val samplingSlots = 3
      val samplingInterval = 100 // milliseconds
      val bufferSize = 100 //bytes
      val bytesRead = 10
      val batchSizeManager = new BatchSizeManager(samplingSlots, samplingInterval, bufferSize)

      // Execute
      val ts1 = System.currentTimeMillis()
      batchSizeManager.track(bytesRead, ts1)
      Thread.sleep(samplingInterval)
      batchSizeManager.track(bytesRead, System.currentTimeMillis())
      Thread.sleep(samplingInterval)
      val ts2 = System.currentTimeMillis()
      batchSizeManager.track(bytesRead, ts2)

      // Validate
      val expectedResult = 100 * 0.9 / (20 / (ts2 - ts1))
      batchSizeManager.getDelay.getMillis shouldEqual expectedResult.toLong
    }

    "return zero delay when there is zero or one sample" in {
      // Setup
      val samplingSlots = 3
      val samplingInterval = 100 // milliseconds
      val bufferSize = 100 //bytes
      val bytesRead = 10
      val batchSizeManager = new BatchSizeManager(samplingSlots, samplingInterval, bufferSize)

      // Validate
      batchSizeManager.getDelay.getMillis shouldEqual 0

      // Add one sample
      batchSizeManager.track(bytesRead, System.currentTimeMillis())

      batchSizeManager.getDelay.getMillis shouldEqual 0

    }

    "return zero delay if we have seen fixed number of consecutive full buffer reads" in {
      // Setup
      val samplingSlots = 3
      val samplingInterval = 1000 // milliseconds
      val bufferSize = 100 //bytes
      val bytesRead = 10
      val batchSizeManager = new BatchSizeManager(samplingSlots, samplingInterval, bufferSize)

      // Execute
      batchSizeManager.track(bytesRead,System.currentTimeMillis())
      Thread.sleep(samplingInterval)
      batchSizeManager.track(bytesRead,System.currentTimeMillis())
      Thread.sleep(samplingInterval)
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
