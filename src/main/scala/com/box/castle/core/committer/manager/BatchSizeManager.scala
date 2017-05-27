package com.box.castle.core.committer.manager

import com.box.castle.core.common.{BoundedQueue, ReadSample}
import com.box.castle.core.config.BatchSizeManagerConfig
import org.joda.time.Duration
import org.slf4s.Logging


/**
  * Implements logic to track reads and compute delay for following reads so as to ensure we always read close to
  * bufferSize of data from Kafka.
  *
  */
class BatchSizeManager(batchSizeManagerConfig: BatchSizeManagerConfig,
                       bufferSize: Int) extends Logging {

  private val samples = new BoundedQueue[ReadSample](batchSizeManagerConfig.samplingSlots)
  private var consecutiveFullBuffers = 0
  private var consecutiveEmptyBuffers = 0
  private val zeroByteSample = ReadSample(0, 0)

  /**
    * Tracks bytes read from kafka and the corresponding timestamp based on specified sampling rate
    * Also keeps track of consecutive full buffer reads seen.
    *
    * @param bytesRead
    * @param timestamp
    */
  def track(bytesRead: Option[Int], timestamp: Long) = {
    bytesRead match {
      case Some(bytes) =>
        if(bytes >= bufferSize) {
          // Update number of times we have seen consecutive full buffer reads
          consecutiveFullBuffers += 1
        }
        else {
          // Reset counter if no more consecutive full buffer reads seen
          consecutiveFullBuffers = 0
        }
        // Reset empty buffer counter if we got non-zero bytes
        consecutiveEmptyBuffers = 0
      case None =>
        // Keep track of consecutive empty buffers seen
        consecutiveEmptyBuffers += 1
    }

    // If queue is full then we add a new sample only after sampling interval
    if (!samples.isFull || timestamp - samples.last.timestamp >= batchSizeManagerConfig.samplingInterval.getMillis) {

      // The bytes read is cumulative here so the new sample adds on to the last samples bytesRead
      // This makes computation of rate more efficient during getDelay
      val sample = ReadSample(samples.lastOption.getOrElse(zeroByteSample).bytesRead + bytesRead.getOrElse(0), timestamp)
      samples.enqueue(sample)
    }

    if (consecutiveFullBuffers > batchSizeManagerConfig.fullBufferThresholdCount || consecutiveEmptyBuffers > batchSizeManagerConfig.emptyBufferThresholdCount) {
      // We are most likely in catchup mode if we have seen consecutive full buffers
      // Flush all samples so that delay falls to 0

      // If we have seen consecutive empty buffers then also we need to flush all samples
      // Otherwise next computed delay will be unreasonably large
      samples.clear()
    }

  }

  /**
    * Computes the delay before next read from kafka based on tracked samples of historical reads and
    * consecutive full buffer reads seen.
    *
    * @return
    */
  def getDelay(committerActorId: String): Duration = {

    if (samples.size > 1) {
      // We need atleast 2 samples to figure out the rate of data generation and hence the delay
      val dataRead: Double = samples.last.bytesRead - samples.front.bytesRead
      val elapsedTime: Double = samples.last.timestamp - samples.front.timestamp
      val rate: Double = dataRead / elapsedTime
      val delay = (bufferSize * batchSizeManagerConfig.targetBatchSizePercent) / rate

      // The discount factor reduces delay based on how many consecutive full buffers we have seen.
      val discount = Math.pow(batchSizeManagerConfig.discountFactor, consecutiveFullBuffers)
      val computedDelay = Math.min(batchSizeManagerConfig.maxWaitTime.getMillis, (delay * discount).toLong)

      log.info(s"$committerActorId idling for ${computedDelay / 1000} seconds after calculating a rate of ${"%.3f".format(rate / 1048.576)} MiB/sec with discountFactor of ${"%.3f".format(discount)}")
      new Duration(computedDelay)
    }
    else {
      // Not enough data to compute delay
      Duration.ZERO
    }
  }

  def size: Int = samples.size

  def getLastSample: ReadSample = samples.last

}

