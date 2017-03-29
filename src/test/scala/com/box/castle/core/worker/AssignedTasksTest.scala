package com.box.castle.core.worker

import com.box.castle.core.config.InitialOffset
import com.box.castle.core.mock.MockTools
import com.box.castle.core.worker.tasks.{MalformedAssignedTasksException, Task, AssignedTasks}
import org.slf4s.Logging
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification




class AssignedTasksTest extends Specification with Mockito with Logging with MockTools {

  val tasks =
    """
{
  "tasks": [
    {
      "topic": "performance",
      "partition": 2,
      "committerIds": [
        "kafkaCommiter_SV1",
        "kafkaCommiter_SFO"
      ]
    },
    {
      "topic": "login",
      "partition": 7,
      "committerIds": [
        "kafkaCommiter_SV1",
        "kafkaCommiter_SFO",
        "elasticSearch"
      ]
    }
  ]
}
    """

  "AssignedTasks" should {
    "correctly parse the WorkerConfig" in {

      val assignedTasks = AssignedTasks.fromJson(tasks)

      val expectedTasks = Set(
        Task("performance", 2, Set("kafkaCommiter_SV1", "kafkaCommiter_SFO")),
        Task("login", 7, Set("kafkaCommiter_SV1", "kafkaCommiter_SFO", "elasticSearch")))

      assignedTasks.tasks must_== expectedTasks

    }
  }


  "AssignedTasks" should {
    "correctly parse json with zero tasks" in {
      val assignedTasks = AssignedTasks.fromJson(AssignedTasks.ZeroTasks.toJson())

      assignedTasks.tasks must_== Map()
    }
  }

  "AssignedTasks" should {
    "produce correct json for ZERO_TASKS" in {
      val expected ="""{"tasks":[]}"""
      AssignedTasks.ZeroTasks.toJson() must_== expected
    }
  }

  "AssignedTasks" should {
    "raise a MalformedAssignedTasks on malformed data" in {
      AssignedTasks.fromJson("{") must throwA[MalformedAssignedTasksException]("Malformed json")
    }
  }


  "AssignedTasks" should {
    "raise a MalformedAssignedTasks when there are no committer ids specified" in {
      AssignedTasks.fromJson("""
                                { "tasks": [
                                    {
                                      "topic": "performance",
                                      "partition": 2,
                                      "committerIds": []
                                    }
                                  ]
                                }
                                """) must
        throwA[MalformedAssignedTasksException](s"No committer ids associated with topic: performance, partition: 2")
    }
  }

  "fromJson" should {
    "raise a MalformedAssignedTasks when there duplicate topic partitions" in {
      AssignedTasks.fromJson("""
                              {
                                "tasks": [
                                  {
                                    "topic": "perf",
                                    "partition": 2,
                                    "committerIds": ["elasticSearch"]
                                  },
                                  {
                                    "topic": "perf",
                                    "partition": 2,
                                    "committerIds": ["elasticSearch", "kafkaCommiter_SFO"]
                                  }
                                ]
                              }
                                """) must
        throwA[MalformedAssignedTasksException](s"Assigned tasks contain a duplicate topic: perf, partition: 2")
    }
  }



  "toJson" should {
    "serialize to json properly" in {
      val task1 = Task("perf", 1, Set("id1", "id2"))
      val task2 = Task("login", 1, Set("id1", "id2", "id3"))

      val assignedTasks = AssignedTasks(Set(task1, task2))

      val expected =
        """\s*""".r.replaceAllIn(
          """
          {
          "tasks": [
            {
              "topic": "perf",
              "partition": 1,
              "committerIds": [
                "id1",
                "id2"
              ]
            },
            {
              "topic": "login",
              "partition": 1,
              "committerIds": [
                "id1",
                "id2",
                "id3"
              ]
            }
          ]
        }
          """, "")

      assignedTasks.toJson() must_== expected
    }
  }

}

