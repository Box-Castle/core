package com.box.castle.core.config


object CorruptMessagePolicy extends Enumeration {
  type CorruptMessagePolicy = Value
  val skip, retry, fail = Value
}