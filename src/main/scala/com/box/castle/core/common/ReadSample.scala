package com.box.castle.core.common

/**
  * Encapsulates a kafka fetch data sample
  */
case class ReadSample(bytesRead: Int, timestamp: Long)
