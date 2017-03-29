package com.box.castle.core.config


object OffsetOutOfRangePolicy extends Enumeration {
  type OffsetOutOfRangePolicy = Value
  val useOldestOffset, useLatestOffset, fail = Value
}
