package com.box.castle.core.leader

import java.nio.charset.Charset
import java.util.concurrent.{Executors, TimeUnit}

import com.box.castle.core.CuratorFactory
import com.box.castle.core.config.{CommitterConfig, LeaderConfig}
import com.box.castle.core.const.CastleZkPaths
import com.box.castle.core.leader.messages._
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterException
import com.box.castle.router.kafkadispatcher.messages.{KafkaBrokerUnreachable, UnexpectedFailure}
import com.box.castle.router.messages.UnexpectedFailure
import com.box.castle.core.util._
import com.box.castle.core.worker.tasks.{AssignedTasks, Task}
import com.box.castle.consumer.ClientId
import org.slf4s.Logging
import kafka.common.TopicAndPartition
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap
import scala.concurrent.{Future, ExecutionContext}
import scala.util.control.{Exception, NonFatal}
import scala.util.{Failure, Success}

private[leader]
class Leader(leaderActor: LeaderActorRef,
             leaderConfig: LeaderConfig,
             clientId: ClientId,
             curatorFactory: CuratorFactory,
             taskManager: TaskManager,
             metricsLogger: MetricsLogger)
  extends Logging {

  // We do this to avoid having to import scala.language.reflectiveCalls
  private class LeaderExecutionContext extends ExecutionContext {
    val threadPool = Executors.newSingleThreadExecutor()

    def execute(runnable: Runnable): Unit = threadPool.submit(runnable)

    def reportFailure(t: Throwable): Unit = {
      log.error("LeaderExecutionContext encountered an unexpected failure", t)
      leaderActor ! FailedToAcquireLeadership
    }

    def shutdown(): Unit = threadPool.shutdown()
  }

  private implicit val ec = new LeaderExecutionContext()

  var currentTasks = Set.empty[Task]
  var currentWorkers = Map.empty[String, Set[Task]]

  val leadershipAcquisitionTimeout = leaderConfig.leadershipAcquisitionTimeout

  private var processingAvailableWorkersChange = false

  lazy val castleCurator = curatorFactory.create()

  lazy val leaderLatch = {
    val leaderLatch = new LeaderLatch(castleCurator, "/leader_election", clientId.value)
    leaderLatch.start()
    leaderLatch
  }

  def waitForLeadership(): Unit = {
    Future({
      log.info(s"Client $clientId is waiting to acquire leadership")
      leaderLatch.await(leadershipAcquisitionTimeout.toSeconds, TimeUnit.SECONDS)
    }) onComplete {
      case Success(isLeadershipAcquired) => {
        if (isLeadershipAcquired) {
          log.info(s"Client $clientId acquired leadership")
          leaderActor ! BecomeLeader
        }
        else {
          log.info(s"$clientId could not obtain leadership after $leadershipAcquisitionTimeout")
          leaderActor ! FailedToAcquireLeadership
        }
      }
      case Failure(t) => {
        log.error(s"$clientId could not obtain leadership due to an error", t)
        leaderActor ! FailedToAcquireLeadership
      }
    }
  }

  def pollAvailableWorkers(): Unit = {
    withLeadershipCheck {
      getAvailableWorkers match {
        case Some(availableWorkers) => {
          if (currentWorkers != availableWorkers) {
            log.info(s"Detected an available workers change." +
              s"\nCurrent workers = $currentWorkers" +
              s"\nNew workers = $availableWorkers")
            processingAvailableWorkersChange = true
            leaderActor ! ProcessAvailableWorkersChange
          }
          else {
            log.info(s"No change in available workers. ClientId=$clientId")
          }
        }
        case None => // Ignore this, we will get the correct list the next time we poll
      }
    }
  }

  private def assignTasksToAvailableWorkers(tasksToAssign: Set[Task], availableWorkers: Map[String, Set[Task]]) = {
    if (availableWorkers.isEmpty) {
      log.warn(s"THERE ARE NO AVAILABLE WORKERS!! UNABLE TO ASSIGN: ${tasksToAssign}")
      currentTasks = tasksToAssign
      currentWorkers = availableWorkers
      processingAvailableWorkersChange = false
    }
    else {
      val newWorkerTaskAssignments = taskManager.assignTasks(availableWorkers, tasksToAssign)
      try {
        newWorkerTaskAssignments.foreach { case (workerId, tasks) =>
          val workerTasksPath = CastleZkPaths.WorkerTasks + "/" + workerId
          val assignedTasks = AssignedTasks(tasks)
          val assignedTasksJson = assignedTasks.toJson().getBytes(Charset.forName("UTF-8"))
          castleCurator.setData().forPath(workerTasksPath, assignedTasksJson)
        }
        // We only update our current local state if we were able to successfully do all the
        // operations required to get the tasks and assign them
        currentTasks = tasksToAssign
        currentWorkers = newWorkerTaskAssignments
        processingAvailableWorkersChange = false

        writeClusterInfo(newWorkerTaskAssignments)
      }
      catch {
        case e: NoNodeException => {
          // This node no longer exists because that worker exited and cleaned up the node, we ignore this since
          // our watch on the workers will trigger in that case and we'll re-process the tasks
          log.warn("Ignored NoNodeException while attempting to assign tasks")
        }
      }
    }
  }

  private def writeClusterInfo(taskAssignments: Map[String, Set[Task]]): Unit = {
    val (byWorker,
          byTopic,
          byCommitterByTopic,
          byCommitterByWorker,
          totalTasks,
          summary) = prettyPrint(taskAssignments)

    val fullSummary = s"\n\nLast task assignment: ${new java.util.Date}\n" +
      s"Current leader: $clientId\n" +
      s"Currently processing: ${totalTasks} tasks\n" +
      s"Assigned tasks by worker:\n" +
      s"$summary\n\n"

    writePathData(CastleZkPaths.ClusterInfo + "/by_worker", byWorker)
    writePathData(CastleZkPaths.ClusterInfo + "/by_topic", byTopic)
    writePathData(CastleZkPaths.ClusterInfo + "/by_committer_by_topic", byCommitterByTopic)
    writePathData(CastleZkPaths.ClusterInfo + "/by_committer_by_worker", byCommitterByWorker)
    writePathData(CastleZkPaths.ClusterInfo + "/summary", fullSummary)

  }

  private def writePathData(path: String, data: String): Unit = {
    try {
      if (castleCurator.checkExists().forPath(path) == null)
        castleCurator.create().creatingParentsIfNeeded().forPath(path, data.getBytes(Charset.forName("UTF-8")))
      else
        castleCurator.setData().forPath(path, data.getBytes(Charset.forName("UTF-8")))
    }
    catch {
      case NonFatal(_) => // Ignore
    }

  }

  private def withLeadershipCheck(f: => Unit): Unit =
    if (leaderLatch.hasLeadership)
      f
    else
      leaderActor ! WaitForLeadership

  def processLatestTopics(latestTopics: Iterable[TopicAndPartition]): Unit = {
    withLeadershipCheck {
      val latestTasks = taskManager.generateTasksToAssign(Leader.toKafkaTopics(latestTopics))
      if (currentTasks != latestTasks || processingAvailableWorkersChange) {
        log.info(s"Detected a Kafka Topics change or available workers change. Getting available workers." +
          s"\nCurrent tasks = $currentTasks" +
          s"\nLatest tasks = $latestTasks")
        getAvailableWorkers match {
          case Some(availableWorkers) => {
            log.info(s"Got latest available workers." +
              s"\nCurrent workers = $currentWorkers" +
              s"\nLatest available workers = $availableWorkers")
            assignTasksToAvailableWorkers(latestTasks, availableWorkers)
          }
          case None => {
            // Ignore this, we will get the correct list the next time we poll
          }
        }
      }
      else {
        log.info(s"No change in Kafka topics. ClientId=$clientId")
      }
    }
  }

  private def getAvailableWorkers: Option[Map[String, Set[Task]]] = {
    try {
      val workers = castleCurator.getChildren.forPath(CastleZkPaths.WorkerAvailability).asScala.toList
      val availableWorkers = workers.map(workerId => {
        val workerTasksPath = CastleZkPaths.WorkerTasks + "/" + workerId
        val data = castleCurator.getData.forPath(workerTasksPath)
        val assignedTasks = AssignedTasks.fromJson(data)
        workerId -> assignedTasks.tasks
      })
      Some(availableWorkers.toMap)
    }
    catch {
      case e: NoNodeException => {
        // We eat this exception because we will get the correct workers set the next time we poll
        log.info(s"Worker assigned tasks node no longer exists, this can happen if the worker goes offline. ClientId=$clientId")
        None
      }
    }
  }

  def relinquishLeadership(): Unit = {
    Exception.ignoring(classOf[Throwable])(leaderLatch.close())
  }

  def close(): Unit = {
    Exception.ignoring(classOf[Throwable])(castleCurator.close())
    Exception.ignoring(classOf[Throwable])(ec.shutdown())
  }

  /**
   * This is a place holder until we can expose something better via REST
   */
  private def prettyPrint(taskAssignments: Map[String, Set[Task]]): (String, String, String, String, Int, String) = {
    case class TaskWithWorkerId(workerId: String, topic: String, partition: Int, committerIds: Set[String])
    implicit val taskWithWorkerIdOrdering = new Ordering[TaskWithWorkerId] {
      override def compare(a: TaskWithWorkerId, b: TaskWithWorkerId): Int = {
        if (a.topic < b.topic || (a.topic == b.topic && a.partition < b.partition))
          -1
        else if (a.topic == b.topic && a.partition == b.partition)
          0
        else
          1
      }
    }

    val allTasks = taskAssignments.flatMap {
      case (workerId, tasks) => tasks.map(t => TaskWithWorkerId(workerId, t.topic, t.partition, t.committerIds))
    }.toSeq

    val workerIdToClientIdMap = getWorkerIdClientIdMap()

    def toClientId(workerId: String): String = workerIdToClientIdMap.getOrElse(workerId, workerId)

    val totalTasks = taskAssignments.map(_._2.size).sum

    val summary = taskAssignments.map {
      case (workerId, tasks) => {
        s"${toClientId(workerId)}: ${tasks.size} tasks"
      }
    }

    val byWorker = taskAssignments.flatMap {
      case (workerId, tasks) => {
        val sortedTasks = tasks.toSeq.sortWith((a, b) => a.topic < b.topic || (a.topic == b.topic && a.partition < b.partition))
        TreeMap(sortedTasks.groupBy(x => (x.topic, x.committerIds.mkString(", "))).toArray:_*).flatMap {
          case ((topic, committerIds), innerTasks) => {
            List(s"\n${toClientId(workerId)} - $topic ($committerIds)\n" +
              s"    ${ innerTasks.map(_.partition).mkString(", ")}")
          }
        }
      }.toList
    }

    val groupedByTopic = TreeMap(allTasks
      .sortWith((a, b) => a.topic < b.topic || (a.topic == b.topic && a.partition < b.partition))
      .groupBy(_.topic)
      .toArray:_*)

    val byTopic = groupedByTopic.flatMap {
      case (topic, innerTasks) => {
        innerTasks.headOption match {
          case Some(task) =>
            s"\n$topic (${task.committerIds.mkString(", ")})" +:
              innerTasks.map(t => s"${"%8d".format(t.partition)}: ${toClientId(t.workerId)}")
          case None =>
            List.empty[String]
        }
      }
    }

    val expandedCommitters = taskAssignments.flatMap {
      case (workerId, tasks) => {
        tasks.flatMap(t => {
          t.committerIds.map (committerId =>
            (committerId, t.topic, t.partition, workerId)
          )
        })
      }
    }.toSeq

    val sortedByCommitter = TreeMap(allTasks
      .flatMap(task => task.committerIds.map(committerId => (committerId, task)))
      .sortBy(_._1)
      .groupBy(_._1)
      .toArray:_*)

    val groupedByCommitter = sortedByCommitter.map {
      case (committerId, committerIdAndTaskTuples) => {
        committerId -> committerIdAndTaskTuples.map { case (_, task) => task }.sorted
      }
    }

    val byCommitterByTopic = groupedByCommitter.flatMap {
      case (committerId, innerTasks) => {
        val byTopic = TreeMap(innerTasks.groupBy(_.topic).toArray:_*)
        byTopic.flatMap {
          case (topic, tasks) => {
            s"\n$committerId - $topic" +:
              tasks.map(t => s"${"%8d".format(t.partition)}: ${toClientId(t.workerId)}").toList
          }
        }.toList
      }
    }


    val byCommitterByWorker = groupedByCommitter.flatMap {
      case (committerId, innerTasks) => {
        val byWorkerId = innerTasks.groupBy(_.workerId)
        byWorkerId.flatMap {
          case (workerId, workerTasks) => {
            val byTopic = TreeMap(workerTasks.groupBy(_.topic).toArray:_*)
            s"\n$committerId - $workerId" +:
              byTopic.flatMap {
                case (topic, tasks) => {
                  List(s"        $topic: ${  tasks.map(t => t.partition).mkString(", ")  }")
                }
              }.toList
          }
        }.toList
      }
    }

    (byWorker.mkString("\n"),
      byTopic.mkString("\n"),
      byCommitterByTopic.mkString("\n"),
      byCommitterByWorker.mkString("\n"),
      totalTasks,
      summary.mkString("\n"))
  }

  def getWorkerIdClientIdMap(): Map[String, String] = {
    try {
      val workers = castleCurator.getChildren.forPath(CastleZkPaths.WorkerAvailability).asScala.toList
      workers.map(workerId => {
        val workerAvailabilityPath = CastleZkPaths.WorkerAvailability + "/" + workerId
        val rawData = castleCurator.getData.forPath(workerAvailabilityPath)
        val data = new String(rawData, Charset.forName("UTF-8"))
        workerId -> data.split("\"")(3)
      }).toMap
    }
    catch {
      case NonFatal(_) => Map.empty
    }
  }
}


object Leader {

  def toKafkaTopics(topicsFromKafka: Iterable[TopicAndPartition]): Set[KafkaTopic] = {
    topicsFromKafka.groupBy(_.topic).map {
      case (topicName, topicAndPartitions) => KafkaTopic(topicName, topicAndPartitions.map(_.partition).toSet)
    }.toSet
  }
}