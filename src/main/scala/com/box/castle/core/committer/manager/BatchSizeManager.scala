package com.box.castle.core.committer.manager

import com.box.castle.core.common.{BoundedQueue, ReadSample}
import com.box.castle.core.config.CommitterConfig
import org.joda.time.Duration
import org.slf4s.Logging


/**
  * Implements logic to track reads and compute delay for following reads so as to ensure we always read close to
  * bufferSize of data from Kafka.
  *
  */
class BatchSizeManager(committerConfig: CommitterConfig,
                       bufferSize: Int) extends Logging {

  private val samples = new BoundedQueue[ReadSample](committerConfig.samplingSlots)
  private var consecutiveFullBuffers = 0

  /**
    * Tracks bytes read from kafka and the corresponding timestamp based on specified sampling rate
    * Also keeps track of consecutive full buffer reads seen.
    *
    * @param bytesRead
    * @param timestamp
    */
  def track(bytesRead: Int, timestamp: Long) = {
    if (bytesRead >= bufferSize) {
      // Update number of times we have seen consecutive full buffer reads
      consecutiveFullBuffers += 1
    }
    else {
      // Reset counter if no more consecutive full buffer reads seen
      consecutiveFullBuffers = 0
    }

    if (!samples.isFull || timestamp - samples.last.timestamp >= committerConfig.samplingInterval.getMillis) {
      // If queue is full then we add a new sample only after sampling interval
      // The bytes read is cumulative here so the new sample adds on to the last samples bytesRead
      // This makes computation of rate more efficient
      if (samples.isEmpty)
        samples.enqueue(ReadSample(bytesRead, timestamp))
      else
        samples.enqueue(ReadSample(samples.last.bytesRead + bytesRead, timestamp))
    }

    if (consecutiveFullBuffers > committerConfig.fullBufferThresholdCount) {
      // We are most likely in catchup mode
      // Flush all samples so that delay falls to 0
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
      val delay = bufferSize / rate

      // The discount factor reduces delay based on how many consecutive full buffers we have seen.
      val discount = Math.pow(committerConfig.discountFactor, consecutiveFullBuffers + 1)
      val computedDelay = Math.min(committerConfig.maxWaitTime.getMillis, (delay * discount).toLong)

      log.info(s"$committerActorId: Idling for ${computedDelay / 1000} seconds after calculating a rate of ${"%.3f".format(rate)} KiB/sec with discountFactor of ${"%.3f".format(discount)}")
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

