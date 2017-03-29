package com.box.castle.core.worker.messages

import org.apache.zookeeper.Watcher.Event.{KeeperState, EventType}



sealed abstract class WorkerMessage {
  def workerActorId: Long
}

case class AssignedTasksChanged(workerActorId: Long, assignedTasksWatcherId: Long) extends WorkerMessage

case class UnexpectedWatcherType(workerActorId: Long, eventType: EventType) extends WorkerMessage

case class NoneWatcherType(workerActorId: Long, keeperState: KeeperState) extends WorkerMessage

case class AssignedTasksNodeDeleted(workerActorId: Long) extends WorkerMessage
