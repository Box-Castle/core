package com.box.castle.core.config

import com.box.castle.consumer.{LatestOffset, EarliestOffset, OffsetType}

import scala.language.implicitConversions


object InitialOffset extends Enumeration {
  type InitialOffset = Value
  val oldest, latest = Value

  // Converts to the offset type class that BoxSimpleConsumer takes as input
  implicit def convertToOffsetType(initialOffset: InitialOffset): OffsetType = {
    if (initialOffset == oldest)
      EarliestOffset
    else
      LatestOffset
  }
}
