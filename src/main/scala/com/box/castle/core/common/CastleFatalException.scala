package com.box.castle.core.common

/**
 * Exceptions of this type are fatal to the healthy operation of the Castle framework itself.
 * @param msg
 * @param t
 */
private[core] class CastleFatalException(msg: String, t: Throwable = null) extends RuntimeException(msg, t)