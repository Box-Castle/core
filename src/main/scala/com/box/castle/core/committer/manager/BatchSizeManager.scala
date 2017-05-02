package com.box.castle.core.committer.manager

import com.box.castle.core.committer.manager.BatchSizeManager._
import com.box.castle.core.common.{BoundedQueue, ReadSample}
import org.joda.time.Duration


/**
  * Implements logic to track reads and compute delay for following reads so as to ensure we always read close to
  * bufferSize of data from Kafka.
  *
  */
class BatchSizeManager(samplingSlots: Int, samplingInterval: Long, bufferSize: Int) {

  private val samples = new BoundedQueue[ReadSample](samplingSlots)
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
    else if (consecutiveFullBuffers > 0) {
      // Reset counter if no more consecutive full buffer reads seen
      consecutiveFullBuffers = 0
    }

    if (samples.isEmpty || timestamp - samples.last.timestamp > samplingInterval) {
      // Add sample to the queue if sampling interval amount of time has passed else we discard it
      samples.enqueue(ReadSample(bytesRead, timestamp))
    }

    if (consecutiveFullBuffers > FullBufferMaxCount) {
      // We are mostly in catchup mode
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
  def getDelay: Duration = {

    if (samples.size > 1) {
      // We need atleast 2 samples to figure out the rate of data generation and hence the delay
      val dataRead = samples.take(samples.size - 1).map(_.bytesRead).sum
      val rate = dataRead / (samples.last.timestamp - samples.front.timestamp)
      // The discount factor reduces delay based on how many consecutive full buffers we have seen.
      new Duration((bufferSize * Math.pow(DiscountFactor, consecutiveFullBuffers + 1) / rate).toLong)
    }
    else {
      // Not enough data to compute delay
      Duration.ZERO
    }
  }

  def size: Int = samples.size

}

object BatchSizeManager {
  val DiscountFactor = 0.9
  val FullBufferMaxCount = 3
}
