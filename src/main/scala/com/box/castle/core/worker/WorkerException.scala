package com.box.castle.core.worker



class WorkerException(val msg: String, e: Throwable = null) extends RuntimeException(msg, e)
