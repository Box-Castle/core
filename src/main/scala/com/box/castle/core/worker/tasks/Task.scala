package com.box.castle.core.worker.tasks

case class Task(topic: String, partition: Int, committerIds: Set[String])
