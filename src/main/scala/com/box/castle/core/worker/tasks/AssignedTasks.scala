package com.box.castle.core.worker.tasks

import java.nio.charset.Charset

import com.box.castle.core.config.{CommitterConfig, InitialOffset}
import org.json4s._
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.util.control.NonFatal



object AssignedTasks {
  implicit val formats = DefaultFormats + new EnumNameSerializer(InitialOffset)

  def fromJson(rawJsonBytes: Array[Byte]): AssignedTasks = {
    val rawJsonString: String = new String(rawJsonBytes, Charset.forName("UTF-8"))
    fromJson(rawJsonString)
  }

  /**
   * Returns an AssignedTasks object given a raw json string
   * @param json
   * @return
   */
  def fromJson(json: String): AssignedTasks = {
    try {
      val parsed = parse(json)
      val tasks = (parsed \ "tasks").extract[Set[Task]]
      validateTasks(tasks)
      new AssignedTasks(tasks)
    }
    catch {
      case e: MalformedAssignedTasksException => throw e
      case NonFatal(e) => {
        throw new MalformedAssignedTasksException("Malformed json", e)
      }
    }
  }

  def validateTasks(tasks: Set[Task]) = {
    val seenTopicPartitions = scala.collection.mutable.Set[(String, Int)]()

    tasks.foreach(task => {
      if (task.committerIds.size < 1)
        throw new MalformedAssignedTasksException(s"No committer ids associated with topic: ${task.topic}, partition: ${task.partition}")
      if (seenTopicPartitions.contains((task.topic, task.partition))) {
        throw new MalformedAssignedTasksException(s"Assigned tasks contain a duplicate topic: ${task.topic}, partition: ${task.partition}")
      }
      else {
        seenTopicPartitions.add((task.topic, task.partition))
      }
    })
  }

  val ZeroTasks = new AssignedTasks(Set.empty)
}

case class AssignedTasks(tasks: Set[Task]) {
  implicit val formats = DefaultFormats + new EnumNameSerializer(InitialOffset)

  def toJson(): String = {
    Serialization.write(this)
  }

  def toJsonBytes(): Array[Byte] = {
    this.toJson().getBytes(Charset.forName("UTF-8"))
  }
}
