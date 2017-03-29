package com.box.castle.core.worker.tasks

import com.box.castle.core.common.CastleFatalException




class MalformedAssignedTasksException(msg: String, e: Throwable = null) extends CastleFatalException(msg, e)
