package com.box.castle.core.leader

import com.box.castle.committer.api.TopicFilter
import com.box.castle.core.config.{CommitterConfig, LeaderConfig}
import com.box.castle.core.worker.tasks.Task
import org.slf4s.Logging

import scala.collection.immutable.{Seq, TreeMap}
import scala.util.Random
import scala.util.matching.Regex
import scala.collection.mutable

object TaskManager {
  def apply(
             leaderConfig: LeaderConfig,
             committerConfigs: Iterable[CommitterConfig],
             topicFilterMap: Map[String, TopicFilter]) =
    new TaskManager(leaderConfig, committerConfigs, topicFilterMap)
}



class TaskManager(
                   val leaderConfig: LeaderConfig,
                   val committerConfigs: Iterable[CommitterConfig],
                   topicFilterMap: Map[String, TopicFilter]) extends Logging {

  /**
   * Given a set of kafka topics and their partitions, this method will generate a set of tasks that match
   * the requirements imposed by the various committer configurations passed into the constructor of this class
   * @param kafkaTopics
   * @return
   */
  def generateTasksToAssign(kafkaTopics: Set[KafkaTopic]): Set[Task] = {
    kafkaTopics.foldLeft[Set[Task]](Set())(foldTopic)
  }

  /**
   * Given a map of workers to their currently assigned tasks, and a list of tasks that should be assigned
   * to the workers, return a new map of workers to their assigned tasks.
   *
   * This method has several guarantees:
   *    1. When reassigning tasks, it will attempt to maintain as many of the old tasks assigned to a worker as possible
   *    2. It will balance the tasks to assign between the available workers as evenly as possible while
   *
   *
   * @param workers
   * @param tasksToAssign
   * @return
   */
  def assignTasks(workers: Map[String, Set[Task]], tasksToAssign: Set[Task]) = {
    new Workers(workers, tasksToAssign).removeStaleTasks().removeExtraTasks().assignTasks()
  }

  private def foldTopic(tasks: Set[Task], kafkaTopic: KafkaTopic): Set[Task] = {
    val committerIds = committerConfigs.foldLeft[Set[String]](Set()) { (committerIds, committerConfig) =>
      committerConfig.topicsRegex match {
        case Some(topicsRegex) => matchTopicsRegex(topicsRegex, kafkaTopic, committerConfig.id, committerIds)
        case None => matchTopicsSet(committerConfig.topicsSet.get, kafkaTopic, committerConfig.id, committerIds)
      }
    }

    if (committerIds.nonEmpty)
      tasks ++ kafkaTopic.partitions.map(partition => Task(kafkaTopic.name, partition, committerIds))
    else
      tasks
  }

  protected[leader] def matchTopicsRegex(topicsRegex: Regex, kafkaTopic: KafkaTopic, committerId: String, committerIds: Set[String]) = {
      kafkaTopic.name match {
        case topicsRegex(_*) => if(matchCommitterTopicFilter(kafkaTopic,committerId)) committerIds + committerId else committerIds
        case _ => committerIds
      }
  }

  protected[leader] def matchTopicsSet(topicsSet: Set[String], kafkaTopic: KafkaTopic, committerId: String, committerIds: Set[String]) = {
    if (topicsSet.contains(kafkaTopic.name) && matchCommitterTopicFilter(kafkaTopic, committerId))
      committerIds + committerId
    else
      committerIds
  }

  protected[leader] def matchCommitterTopicFilter(kafkaTopic: KafkaTopic, committerId: String): Boolean = {
    val topicFilter = topicFilterMap(committerId)
    topicFilter.matches(kafkaTopic.name)
  }

  private class Workers(var workers: Map[String, Set[Task]], val tasksToAssign: Set[Task]) {
    val workerTargetNumTasks: Map[String, Int] = {
      val baseNumTasks = tasksToAssign.size / workers.size
      val extraTasks = tasksToAssign.size % workers.size

      // We sort the workers by the number of tasks they use so that we re-assign the minimum amount
      // of tasks possible in cases where we would shuffle tasks from the end of the array to the beginning
      val sortedWorkers = workers.toIndexedSeq.sortBy { case (workerId, tasks) =>
        // Including workerId gives us a stable order for testing purposes
        (tasks.size, workerId)
      }.reverse

      sortedWorkers.zipWithIndex.map { case ((workerId, _), idx) =>
        val additionalTask = if (idx < extraTasks) 1 else 0
        (workerId, baseNumTasks + additionalTask)
      }.toMap
    }

    val tasksToReassign = mutable.Set[Task]()

    /**
     * Removes stale tasks that should no longer be assigned to any worker
     */
    def removeStaleTasks() = {
      this.workers = workers.map { case (workerId, tasks) =>
        val tasksToRemove = tasks -- tasksToAssign
        (workerId, tasks -- tasksToRemove)
      }
      this
    }

    /**
     * Removes extra tasks that workers are working on that should be reassigned to some other worker
     * @return
     */
    def removeExtraTasks() = {
      this.workers = workers.map { case (workerId, tasks) =>
        val targetNumTasks = workerTargetNumTasks(workerId)
        if (targetNumTasks < tasks.size) {
          val reassign = tasks.take(tasks.size - targetNumTasks)
          tasksToReassign ++= reassign
          (workerId, tasks -- reassign)
        }
        else {
          (workerId, tasks)
        }
      }
      this
    }

    def assignTasks() = {
      val assignedTasks = workers.values.foldLeft(Set[Task]()) { (assignedTasks, tasks) => assignedTasks ++ tasks}
      var unassignedTasks = Random.shuffle(tasksToAssign -- assignedTasks ++ tasksToReassign)

      workers.map { case (workerId, tasks) =>
        val targetNumTasks = workerTargetNumTasks(workerId)
        if (unassignedTasks.size > 0 && targetNumTasks > tasks.size) {
          val tasksToAssign = unassignedTasks.take(targetNumTasks - tasks.size)
          unassignedTasks = unassignedTasks -- tasksToAssign
          (workerId, tasks ++ tasksToAssign)
        }
        else {
          (workerId, tasks)
        }
      }
    }
  }

}
