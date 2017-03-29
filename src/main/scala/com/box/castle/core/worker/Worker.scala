package com.box.castle.core.worker

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import com.box.castle.consumer.ClientId
import com.box.castle.core.CuratorFactory
import com.box.castle.core.const.CastleZkPaths
import com.box.castle.core.worker.Worker.DefaultNodeCreationTimeout
import com.box.castle.core.worker.messages.{NoneWatcherType, AssignedTasksChanged, AssignedTasksNodeDeleted, UnexpectedWatcherType}
import com.box.castle.core.worker.tasks.AssignedTasks
import org.slf4s.Logging
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL_SEQUENTIAL
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.Exception

/**
 * The relevant ZooKeeper paths are:
 * /<namespace>/workers/ids/<workerId>
 *    The unique worker ids are generated in this path by creating ephemeral sequential nodes
 *
 * /<namespace>/workers/available/<workerId>
 *    These are all ephemeral nodes that are created by workers when they advertise themselves as available
 *    In steady state there will be one ephemeral node per worker in the Castle Cluster.
 *
 * /<namespace>/workers/tasks/<workerId>
 *    These are persistent nodes that contain the worker configuration in JSON format which lists all the
 *    topics and partitions this particular worker is responsible for called "tasks".  The worker monitors its
 *    own node for changes that might be written to it by the cluster leader, and will adjust which tasks it is
 *    currently responsible for in an efficient manner.
 **/
class Worker(workerActor: WorkerActorRef,
             workerActorId: Long,
             clientId: ClientId,
             curatorFactory: CuratorFactory,
             persistentEphemeralNodeFactory: PersistentEphemeralNodeFactory = new PersistentEphemeralNodeFactory())
  extends Logging {

  val nodeData: Array[Byte]  = s"""
      {
        "hostname": "${java.net.InetAddress.getLocalHost.getHostName}",
        "clientId": "$clientId"
      }
      """.stripMargin.getBytes(Charset.forName("UTF-8"))

  private[worker] lazy val castleCurator = curatorFactory.create()

  lazy val workerIdNode = {
    log.info(s"WorkerActor-$workerActorId creating ephemeral sequential worker id node...")
    val node = persistentEphemeralNodeFactory.create(castleCurator, PROTECTED_EPHEMERAL_SEQUENTIAL,
      CastleZkPaths.WorkerIds + "/worker-", nodeData)
    node.start()
    if (!node.waitForInitialCreate(DefaultNodeCreationTimeout.toSeconds, TimeUnit.SECONDS)) {
      throw new WorkerException(s"WorkerActor-$workerActorId failed to create a worker ID node in ZooKeeper")
    }
    log.info(s"WorkerActor-$workerActorId successfully created worker id node: ${fq(node.getActualPath)}")
    node
  }

  var assignedTasksWatcherId: Long = 0

  lazy val workerId = ZKPaths.getNodeFromPath(workerIdNode.getActualPath)
  private lazy val assignedTasksPath = CastleZkPaths.WorkerTasks + "/" + workerId
  private lazy val availabilityNodePath = CastleZkPaths.WorkerAvailability + "/" + workerId

  def fq(path: String) = s"/${castleCurator.getNamespace}$path"

  def createIfNotExistsAssignedTasksNode() = {
    try {
      log.info(s"WorkerActor-$workerActorId creating assigned tasks node")

      val zeroTasks: Array[Byte] = AssignedTasks.ZeroTasks.toJsonBytes()
      castleCurator.create().creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT).forPath(assignedTasksPath, zeroTasks)

      log.info(s"WorkerActor-$workerActorId finished creating assigned tasks node: " +
        s"${fq(assignedTasksPath)}, verifying its existence and content")

      val assignedTasks: Array[Byte] = castleCurator.getData.forPath(assignedTasksPath)

      if (assignedTasks.sameElements(zeroTasks)) {
        log.info(s"WorkerActor-$workerActorId successfully created assigned " +
          s"tasks node: ${fq(assignedTasksPath)} with zero tasks")
      }
      else {
        throw new WorkerException(s"WorkerActor-$workerActorId expected zero tasks in " +
          s"/${fq(assignedTasksPath)}, but got this instead: ${AssignedTasks.fromJson(assignedTasks)}")
      }
    }
    catch {
      case e: NodeExistsException => log.info(s"WorkerActor-$workerActorId assigned tasks node already " +
        s"exists at: ${fq(assignedTasksPath)}")
    }
  }

  def createIfNotExistsAvailabilityNode() = {
    try {
      log.info(s"WorkerActor-$workerActorId creating ephemeral availability node...")
      castleCurator.create().creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL).forPath(availabilityNodePath, nodeData)

      log.info(s"WorkerActor-$workerActorId successfully created a " +
        s"worker availability node: ${fq(assignedTasksPath)})")
    }
    catch {
      case e: NodeExistsException => log.info(s"WorkerActor-$workerActorId availability node already " +
        s"exists at: ${fq(availabilityNodePath)}")
    }
  }

  def getAssignedTasks: AssignedTasks = {
    createIfNotExistsAssignedTasksNode()
    createIfNotExistsAvailabilityNode()

    val watcher = createAssignedTasksWatcher()
    try {
      val rawAssignedTasksJson: Array[Byte] = castleCurator.getData.usingWatcher(watcher).forPath(assignedTasksPath)
      AssignedTasks.fromJson(rawAssignedTasksJson)
    }
    catch {
      case e: NoNodeException =>
        throw new WorkerException(s"WorkerActor-$workerActorId did not find assigned tasks path $assignedTasksPath")
    }
  }

  private def createAssignedTasksWatcher() = {
    assignedTasksWatcherId += 1
    val currentCuratorWatcherId = assignedTasksWatcherId
    new CuratorWatcher() {
      override def process(event: WatchedEvent): Unit = {
        log.info(s"WorkerActor-$workerActorId assigned tasks watcher $currentCuratorWatcherId triggered " +
          s"on path: ${event.getPath}, state: ${event.getState}, type: ${event.getType}")

        event.getType match {
          case Watcher.Event.EventType.NodeDataChanged =>
            workerActor ! AssignedTasksChanged(workerActorId, currentCuratorWatcherId)
          case Watcher.Event.EventType.NodeDeleted => workerActor ! AssignedTasksNodeDeleted(workerActorId)
          case Watcher.Event.EventType.None => workerActor ! NoneWatcherType(workerActorId, event.getState)
          case unexpectedEventType => workerActor ! UnexpectedWatcherType(workerActorId, unexpectedEventType)
        }
      }
    }
  }

  def removeAvailability() = {
    log.info(s"WorkerActor-$workerActorId removing ephemeral availability node: $availabilityNodePath")
    Exception.ignoring(classOf[Throwable])(castleCurator.delete().guaranteed().forPath(availabilityNodePath))

    log.info(s"WorkerActor-$workerActorId removing protected ephemeral workerId node: ${workerIdNode.getActualPath}")
    Exception.ignoring(classOf[Throwable])(workerIdNode.close())

    log.info(s"WorkerActor-$workerActorId deleting assigned tasks node: $assignedTasksPath")
    Exception.ignoring(classOf[Throwable])(castleCurator.delete().guaranteed().forPath(assignedTasksPath))
  }

  def close() = {
    Exception.ignoring(classOf[Throwable])(castleCurator.close())
  }
}

object Worker {
  val DefaultNodeCreationTimeout = FiniteDuration(30, TimeUnit.SECONDS)
}
