package com.box.castle.core.committer


private[committer] class UnrecoverableCommitterActorException(val committerId: String, msg: String, t: Throwable = null) extends RuntimeException(msg, t)
