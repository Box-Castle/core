package com.box.castle.core.leader

import java.util.concurrent.TimeUnit

import com.box.castle.core.config.{InitialOffset, LeaderConfig, CommitterConfig}
import com.box.castle.core.mock.MockTools
import com.box.castle.core.worker.tasks.Task
import org.slf4s.Logging
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.collection.immutable.IndexedSeq
import scala.concurrent.duration.FiniteDuration



class TaskManagerTest extends Specification with Mockito with Logging with MockTools {

  def getTasksManager(committerConfigs: Iterable[CommitterConfig]) =
    new TaskManager(LeaderConfig(FiniteDuration(1000, TimeUnit.MILLISECONDS), FiniteDuration(500, TimeUnit.MILLISECONDS)), committerConfigs)

  "generateTasksToAssign" should {
    "use the tasks regex to match topic names if it is provided" in {
      val kafkaTopics = Set(
        KafkaTopic("performance", Set(0, 1)),
        KafkaTopic("login", Set(8, 9)))

      val committerConfigs = List(
        createCommitterConfig(InitialOffset.latest, "kafkaCommitter", topicsRegexRaw=Some(".*")),
        createCommitterConfig(InitialOffset.latest, "esCommitter", topicsRegexRaw=Some("perf.*")),
        createCommitterConfig(InitialOffset.latest, "xCommitter", topicsRegexRaw=Some("log.*")))

      val tasks = getTasksManager(committerConfigs).generateTasksToAssign(kafkaTopics)
      val expectedTasks = Set(
        Task("performance", 0, Set("kafkaCommitter", "esCommitter")),
        Task("performance", 1, Set("kafkaCommitter", "esCommitter")),
        Task("login", 8, Set("kafkaCommitter", "xCommitter")),
        Task("login", 9, Set("kafkaCommitter", "xCommitter")))

      tasks must_== expectedTasks
    }
  }

  "generateTasksToAssign" should {
    "use the tasks set to match topic names if it is provided" in {
      val kafkaTopics = Set(KafkaTopic("performance", Set(0, 1)),
        KafkaTopic("login", Set(8, 9)))

      val committerConfigs = List(
        createCommitterConfig(InitialOffset.latest, "kafkaCommitter", topicsRegexRaw=None, topicsSet=Some(Set("performance", "login"))),
        createCommitterConfig(InitialOffset.latest, "esCommitter", topicsRegexRaw=None, topicsSet=Some(Set("performance"))),
        createCommitterConfig(InitialOffset.latest, "xCommitter", topicsRegexRaw=None, topicsSet=Some(Set("login"))))

      val tasks = getTasksManager(committerConfigs).generateTasksToAssign(kafkaTopics)
      log.info(s"#################### $tasks ################ $committerConfigs")
      val expectedTasks = Set(
        Task("performance", 0, Set("kafkaCommitter", "esCommitter")),
        Task("performance", 1, Set("kafkaCommitter", "esCommitter")),
        Task("login", 8, Set("kafkaCommitter", "xCommitter")),
        Task("login", 9, Set("kafkaCommitter", "xCommitter")))

      tasks must_== expectedTasks
    }
  }

  "generateTasksToAssign" should {
    "not include any topics that are not matched by any of the committer requirements" in {
      val kafkaTopics = Set(
        KafkaTopic("performance", Set(0, 1)),
        KafkaTopic("login", Set(8, 9)))

      val committerConfigs = List(
        createCommitterConfig(InitialOffset.latest, "kafkaCommitter", topicsRegexRaw=None, topicsSet=Some(Set("performance"))),
        createCommitterConfig(InitialOffset.latest, "esCommitter", topicsRegexRaw=None, topicsSet=Some(Set("performance"))),
        createCommitterConfig(InitialOffset.latest, "xCommitter", topicsRegexRaw=None, topicsSet=Some(Set("performance"))))

      val tasks = getTasksManager(committerConfigs).generateTasksToAssign(kafkaTopics)
      val expectedTasks = Set(
        Task("performance", 0, Set("kafkaCommitter", "esCommitter", "xCommitter")),
        Task("performance", 1, Set("kafkaCommitter", "esCommitter", "xCommitter")))

      tasks must_== expectedTasks
    }
  }

  "generateTasksToAssign" should {
    "prefer the regex if both the regex and the topic set is specified" in {
      val kafkaTopics = Set(
        KafkaTopic("performance", Set(0, 1)),
        KafkaTopic("login", Set(8, 9)))

      val committerConfigs = List(
        createCommitterConfig(InitialOffset.latest, "kafkaCommitter", topicsRegexRaw=Some(".*"), topicsSet=Some(Set("zzz"))),
        createCommitterConfig(InitialOffset.latest, "esCommitter", topicsRegexRaw=Some("perf.*"), topicsSet=Some(Set("zzz"))),
        createCommitterConfig(InitialOffset.latest, "xCommitter", topicsRegexRaw=Some("log.*"), topicsSet=Some(Set("zzz"))))

      val tasks = getTasksManager(committerConfigs).generateTasksToAssign(kafkaTopics)
      val expectedTasks = Set(
        Task("performance", 0, Set("kafkaCommitter", "esCommitter")),
        Task("performance", 1, Set("kafkaCommitter", "esCommitter")),
        Task("login", 8, Set("kafkaCommitter", "xCommitter")),
        Task("login", 9, Set("kafkaCommitter", "xCommitter")))

      tasks must_== expectedTasks
    }
  }

  "generateTasksToAssign" should {
    "support mixing regexes and topic sets between different committers" in {
      val kafkaTopics = Set(
        KafkaTopic("performance", Set(0, 1)),
        KafkaTopic("login", Set(8, 9)))

      val committerConfigs = List(
        createCommitterConfig(InitialOffset.latest, "kafkaCommitter", topicsRegexRaw=Some(".*"), topicsSet=Some(Set("zzz"))),
        createCommitterConfig(InitialOffset.latest, "esCommitter", topicsRegexRaw=Some("perf.*"), topicsSet=Some(Set("zzz"))),
        createCommitterConfig(InitialOffset.latest, "xCommitter", topicsRegexRaw=None, topicsSet=Some(Set("login"))))

      val tasks = getTasksManager(committerConfigs).generateTasksToAssign(kafkaTopics)
      val expectedTasks = Set(
        Task("performance", 0, Set("kafkaCommitter", "esCommitter")),
        Task("performance", 1, Set("kafkaCommitter", "esCommitter")),
        Task("login", 8, Set("kafkaCommitter", "xCommitter")),
        Task("login", 9, Set("kafkaCommitter", "xCommitter")))

      tasks must_== expectedTasks
    }
  }

  "assignTasks" should {
    "handle the 0 initial tasks case properly" in {
      val workers = Map("workerA" -> Set[Task]())
      val tasksToAssign = Set(
        Task("performance", 0, Set("kafkaCommitter", "esCommitter")),
        Task("performance", 1, Set("kafkaCommitter", "esCommitter")),
        Task("login", 8, Set("kafkaCommitter", "xCommitter")),
        Task("login", 9, Set("kafkaCommitter", "xCommitter")))

      val newWorkers = getTasksManager(None).assignTasks(workers, tasksToAssign)
      val expectedWorkers = Map("workerA" -> Set(
        Task("performance", 0, Set("kafkaCommitter", "esCommitter")),
        Task("performance", 1, Set("kafkaCommitter", "esCommitter")),
        Task("login", 8, Set("kafkaCommitter", "xCommitter")),
        Task("login", 9, Set("kafkaCommitter", "xCommitter"))))

      newWorkers must_== expectedWorkers
    }
  }

  "assignTasks" should {
    "not move around tasks if there is just 1 task when rebalancing" in {
      val workers = Map(
        "workerA" -> Set(Task("performance", 0, Set("kafkaCommitter", "esCommitter"))),
        "workerB" -> Set[Task]())

      val tasksToAssign = Set(Task("performance", 0, Set("kafkaCommitter", "esCommitter")))

      val newWorkers = getTasksManager(None).assignTasks(workers, tasksToAssign)
      val expectedWorkers = Map(
        "workerA" -> Set(Task("performance", 0, Set("kafkaCommitter", "esCommitter"))),
        "workerB" -> Set[Task]())

      newWorkers must_== expectedWorkers
    }
  }

  "assignTasks" should {
    "rebalance tasks when adding a new worker for any number of initial tasks" in {
      (1 to 10).foreach(numTasks => {
        val tasksToAssign = (1 to numTasks).map(i => Task("perf", i, Set("kafkaCommitter"))).toSet
        val workers = Map(
          "workerA" -> tasksToAssign,
          "workerB" -> Set[Task]())

        val newWorkers = getTasksManager(None).assignTasks(workers, tasksToAssign)
        val expectedATaskSetSize = Math.ceil(numTasks.toDouble / 2.0).toInt
        val expectedBTaskSetSize = numTasks - expectedATaskSetSize
        assert(newWorkers("workerA").size == expectedATaskSetSize,
          s"workerA.size=${newWorkers("workerA").size}, expected: $expectedATaskSetSize, numTasks=$numTasks")
        assert(newWorkers("workerB").size == expectedBTaskSetSize,
          s"workerB.size=${newWorkers("workerB").size}, expected: $expectedBTaskSetSize, numTasks=$numTasks")

        newWorkers("workerA") ++ newWorkers("workerB") must_== tasksToAssign
      })
      ok
    }
  }

  "assignTasks" should {
    "rebalance tasks when adding a 2 new workers for any number of initial tasks" in {
      val expectedSizes = IndexedSeq(
        IndexedSeq(1, 0, 0),
        IndexedSeq(1, 1, 0),
        IndexedSeq(1, 1, 1),
        IndexedSeq(2, 1, 1),
        IndexedSeq(2, 2, 1),
        IndexedSeq(2, 2, 2),
        IndexedSeq(3, 2, 2),
        IndexedSeq(3, 3, 2),
        IndexedSeq(3, 3, 3),
        IndexedSeq(4, 3, 3)
      )
      (1 to 10).foreach(numTasks => {
        val tasksToAssign = (1 to numTasks).map(i => Task("perf", i, Set("kafkaCommitter"))).toSet
        val workers = Map(
          "worker3A" -> tasksToAssign,
          "worker2B" -> Set[Task](),
          "worker1C" -> Set[Task]())

        val newWorkers = getTasksManager(None).assignTasks(workers, tasksToAssign)
        val expectedATaskSetSize = expectedSizes(numTasks - 1)(0)
        val expectedBTaskSetSize = expectedSizes(numTasks - 1)(1)
        val expectedCTaskSetSize = expectedSizes(numTasks - 1)(2)
        assert(newWorkers("worker3A").size == expectedATaskSetSize,
          s"workerA.size=${newWorkers("worker3A").size}, expected: $expectedATaskSetSize, numTasks=$numTasks")
        assert(newWorkers("worker2B").size == expectedBTaskSetSize,
          s"workerB.size=${newWorkers("worker2B").size}, expected: $expectedBTaskSetSize, numTasks=$numTasks")
        assert(newWorkers("worker1C").size == expectedCTaskSetSize,
          s"workerB.size=${newWorkers("worker1C").size}, expected: $expectedCTaskSetSize, numTasks=$numTasks")

        newWorkers("worker3A") ++ newWorkers("worker2B") ++ newWorkers("worker1C") must_== tasksToAssign
      })
      ok
    }
  }

  "assignTasks" should {
    "rebalance tasks when removing an existing worker" in {
      val rebalanceCombinations = List(
      //   From     Expected to
      //   A,B,C    A, B
      //  Start with 0
        ( (0,0,0), (0, 0) ),

      //  Start with 1 task
        ( (0,0,1), (0, 1) ),
        ( (0,1,0), (0, 1) ),
        ( (1,0,0), (1, 0) ),

      //  Start with 2 tasks
        ( (0,0,2), (1, 1) ),
        ( (0,1,1), (1, 1) ),
        ( (0,2,0), (1, 1) ),
        ( (1,0,1), (1, 1) ),
        ( (1,1,0), (1, 1) ),
        ( (2,0,0), (1, 1) ),

      //   Start with 3 tasks
        ( (0,0,3), (1, 2) ),
        ( (0,1,2), (1, 2) ),
        ( (0,2,1), (1, 2) ),
        ( (0,3,0), (1, 2) ),
        ( (1,0,2), (2, 1) ),
        ( (1,1,1), (1, 2) ),
        ( (1,2,0), (1, 2) ),
        ( (2,0,1), (2, 1) ),
        ( (2,1,0), (2, 1) ),
        ( (3,0,0), (2, 1) ),

      // Start with 4 tasks
        ( (0,0,4), (2, 2) ),
        ( (0,1,3), (2, 2) ),
        ( (0,2,2), (2, 2) ),
        ( (0,3,1), (2, 2) ),
        ( (0,4,0), (2, 2) ),
        ( (1,0,3), (2, 2) ),
        ( (1,1,2), (2, 2) ),
        ( (1,2,1), (2, 2) ),
        ( (1,3,0), (2, 2) ),
        ( (2,0,2), (2, 2) ),
        ( (2,1,1), (2, 2) ),
        ( (2,2,0), (2, 2) ),
        ( (3,0,1), (2, 2) ),
        ( (3,1,0), (2, 2) ),
        ( (4,0,0), (2, 2) ),

      // Start with 5 tasks, a lot of these are (2, 3) because when sorting on equal task sizes, the worker id is used, and then
      // the result is reversed, so (n, workerA), (n, workerB) always sorts as (n, workerB), (n, workerA)
        ( (0,0,5), (2, 3) ),
        ( (0,1,4), (2, 3) ),
        ( (0,2,3), (2, 3) ),
        ( (0,3,2), (2, 3) ),
        ( (0,4,1), (2, 3) ),
        ( (0,5,0), (2, 3) ),
        ( (1,0,4), (3, 2) ),
        ( (1,1,3), (2, 3) ),
        ( (1,2,2), (2, 3) ),
        ( (1,3,1), (2, 3) ),
        ( (1,4,0), (2, 3) ),
        ( (2,0,3), (3, 2) ),
        ( (2,1,2), (3, 2) ),
        ( (2,2,1), (2, 3) ),
        ( (2,3,0), (2, 3) ),
        ( (3,0,2), (3, 2) ),
        ( (3,1,1), (3, 2) ),
        ( (3,2,0), (3, 2) ),
        ( (4,0,1), (3, 2) ),
        ( (4,1,0), (3, 2) ),
        ( (5,0,0), (3, 2) )
      )

      rebalanceCombinations.foreach { case ((a, b, c), (expectedA, expectedB)) =>
        val numTasks = a + b + c
        val tasksToAssignSeq: IndexedSeq[Task] = (1 to numTasks).map(i => Task("perf", i, Set("kafkaCommitter")))
        val tasksToAssign = tasksToAssignSeq.toSet

        val initialAtasks = tasksToAssignSeq.slice(0, a).toSet
        val initialBtasks = tasksToAssignSeq.slice(a, a + b).toSet
        val workers = Map(
          "workerA" -> initialAtasks,
          "workerB" -> initialBtasks)

        val newWorkers = getTasksManager(None).assignTasks(workers, tasksToAssign)

        assert(newWorkers("workerA").size == expectedA,
          s"workerA.size=${newWorkers("workerA").size}, expected: $expectedA, numTasks=$numTasks, a=$a, b=$b, c=$c, workers=$workers")
        assert(newWorkers("workerB").size == expectedB,
          s"workerB.size=${newWorkers("workerB").size}, expected: $expectedB, numTasks=$numTasks")

        // Checking the intersection here ensures that we did not move around the actual tasks themselves unless we absolutely had to
        assert(initialAtasks.intersect(newWorkers("workerA")).size == Math.min(a, expectedA),
          s"A: numTasks=$numTasks, a=$a, b=$b, c=$c,")
        assert(initialBtasks.intersect(newWorkers("workerB")).size == Math.min(b, expectedB),
          s"B: numTasks=$numTasks, a=$a, b=$b, c=$c,")

        newWorkers("workerA") ++ newWorkers("workerB") must_== tasksToAssign
      }
      ok
    }
  }

}
