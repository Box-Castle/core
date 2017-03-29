package com.box.castle.core.worker

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import akka.actor.Props
import akka.testkit.{TestProbe, TestActorRef}

import com.box.castle.core.committer.manager.{CommitterManagerActor, CommitterManagerActorFactory}
import com.box.castle.core.config.InitialOffset
import com.box.castle.core.const
import com.box.castle.core.const.CastleZkPaths
import com.box.castle.core.mock.MockTools
import com.box.castle.core.worker.messages.{NoneWatcherType, AssignedTasksChanged}
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterRef
import com.box.castle.core.worker.tasks.{AssignedTasks, Task}
import com.box.castle.consumer.ClientId
import com.box.castle.router.mock.MockMetricsLogger
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.duration.FiniteDuration



class WorkerActorTest extends Specification with Mockito with MockTools {
  val timeout = FiniteDuration(10, TimeUnit.SECONDS)

  val committerConfig = createCommitterConfig(id = "test", initialOffset = InitialOffset.oldest,
    parallelismFactor = 1)

  "WorkerActor" should {

    "KeeperState changes are handled properly" in new actorSystemAndCurator {
      val probe = TestProbe()(system)
      val clientId = ClientId("mockClientId")
      val workerFactory = new WorkerFactory(clientId, curatorFactory)

      val router = mock[RouterRef]
      val committerManagerActorFactory = mock[CommitterManagerActorFactory]
      committerManagerActorFactory.props(any, any) returns Props(mock[CommitterManagerActor])

      val metricsLogger = new MockMetricsLogger()
      val workerActorFactory = new WorkerActorFactory(committerManagerActorFactory,
        workerFactory, Seq(committerConfig), metricsLogger)
      val workerActorRef = TestActorRef(workerActorFactory.props(router), "WorkerActor")(system)


      val workerActor = workerActorRef.underlyingActor.asInstanceOf[WorkerActor]

      val worker = workerActor.worker

      // Initial assigned tasks were zero tasks
      mustEventuallyBeTrue({
        val rawAssignedTasks: Array[Byte] = worker.castleCurator.getData.forPath(CastleZkPaths.WorkerTasks + "/" + worker.workerId)
        AssignedTasks.fromJson(rawAssignedTasks) == AssignedTasks.ZeroTasks
      }, 10, TimeUnit.SECONDS)

      // Availability node was created
      worker.castleCurator.checkExists.forPath(CastleZkPaths.WorkerAvailability + "/" + worker.workerId)

      worker.castleCurator.getState must_== CuratorFrameworkState.STARTED

      workerActor.isKeeperStateConnected must_== true
      workerActorRef ! NoneWatcherType(workerActor.workerActorId, KeeperState.Disconnected)
      workerActor.isKeeperStateConnected must_== false
      metricsLogger.getCountFor(const.Metrics.WatcherEvent) must_== 1

      workerActorRef ! NoneWatcherType(workerActor.workerActorId, KeeperState.SyncConnected)
      workerActor.isKeeperStateConnected must_== true
      metricsLogger.getCountFor(const.Metrics.WatcherEvent) must_== 2

      workerActorRef ! NoneWatcherType(workerActor.workerActorId, KeeperState.Disconnected)
      workerActor.isKeeperStateConnected must_== false
      metricsLogger.getCountFor(const.Metrics.WatcherEvent) must_== 3

      metricsLogger.getCountFor(const.Metrics.ZkReconnectFailed) must_== 0

      // Make sure that we properly restart the worker if it fails to reconnect to ZK after the configured timeout
      mustEventuallyBeTrue({
        metricsLogger.getCountFor(const.Metrics.ZkReconnectFailed) == 1
      }, 30, TimeUnit.SECONDS)

    }

    "get assigned tasks upon startup" in new actorSystemAndCurator {
      val clientId = ClientId("mockClientId")
      val workerFactory = new WorkerFactory(clientId, curatorFactory)

      val router = mock[RouterRef]
      val committerManagerActorFactory = mock[CommitterManagerActorFactory]
      committerManagerActorFactory.props(any, any) returns Props(mock[CommitterManagerActor])

      val workerActorFactory = new WorkerActorFactory(committerManagerActorFactory,
        workerFactory, Seq(committerConfig), mock[MetricsLogger])
      val workerActorRef = TestActorRef(workerActorFactory.props(router), "WorkerActor")(system)

      val workerActor = workerActorRef.underlyingActor.asInstanceOf[WorkerActor]

      val worker = workerActor.worker

      // Initial assigned tasks were zero tasks
      mustEventuallyBeTrue({
        val rawAssignedTasks: Array[Byte] = worker.castleCurator.getData.forPath(CastleZkPaths.WorkerTasks + "/" + worker.workerId)
        AssignedTasks.fromJson(rawAssignedTasks) == AssignedTasks.ZeroTasks
      }, 10, TimeUnit.SECONDS)

      // Availability node was created
      worker.castleCurator.checkExists.forPath(CastleZkPaths.WorkerAvailability + "/" + worker.workerId)
    }

    "handle task changes correctly" in new actorSystemAndCurator {
      val clientId = ClientId("mockClientId")
      val workerFactory = new WorkerFactory(clientId, curatorFactory)
      val mockMetricsLogger = new MockMetricsLogger()

      val router = mock[RouterRef]
      val committerManagerActorFactory = mock[CommitterManagerActorFactory]
      committerManagerActorFactory.props(any, any) returns Props(mock[CommitterManagerActor])

      val workerActorFactory = new WorkerActorFactory(committerManagerActorFactory,
        workerFactory, Seq(committerConfig), mockMetricsLogger)
      val workerActorRef = TestActorRef(workerActorFactory.props(router), "WorkerActor")(system)

      val workerActor = workerActorRef.underlyingActor.asInstanceOf[WorkerActor]
      workerActor.worker.assignedTasksWatcherId must_== 1

      workerActor.currentCommitterManagers.size must_== 0
      curator.setData().forPath(CastleZkPaths.WorkerTasks + "/" + workerActor.worker.workerId,
        """
        { "tasks": [
                 { "topic": "perf", "partition": 2, "committerIds": ["kafkaCommitter"] },
                 { "topic": "perf", "partition": 3, "committerIds": ["kafkaCommitter"] }
          ]
        } """.getBytes(Charset.forName("UTF-8")))

      // Wait for currentFetchers size to change
      mustEventuallyBeTrue(workerActor.currentCommitterManagers.size == 2)
      workerActor.worker.assignedTasksWatcherId must_== 2

      there was one(committerManagerActorFactory).props(Task("perf", 2, Set("kafkaCommitter")), router)
      there was one(committerManagerActorFactory).props(Task("perf", 3, Set("kafkaCommitter")), router)

      workerActor.currentCommitterManagers.size must_== 2

      workerActor.currentCommitterManagers.get(Task("perf", 3, Set("kafkaCommitter"))) must_!= None

      curator.setData().forPath(CastleZkPaths.WorkerTasks + "/" + workerActor.worker.workerId,
        """
        {
          "tasks": [
                 { "topic": "perf", "partition": 2, "committerIds": ["kafkaCommitter"] },
                 { "topic": "perf", "partition": 3, "committerIds": ["kafkaCommitter", "anotherCommitter"] }
          ]
        } """.getBytes(Charset.forName("UTF-8")))

      // Wait for the old fetcher to be removed
      mustEventuallyBeTrue(workerActor.currentCommitterManagers.get(Task("perf", 3, Set("kafkaCommitter"))) == None)
      workerActor.worker.assignedTasksWatcherId must_== 3

      // Since we changed the committer ids associated with perf 3, the worker actor will stop that committer manager
      // and then restart that committer manager with the new committer ids

      there was one(committerManagerActorFactory).props(Task("perf", 3, Set("kafkaCommitter")), router)

      workerActor.currentCommitterManagers.size must_== 2
      workerActor.currentCommitterManagers.get(Task("perf", 3, Set("kafkaCommitter", "anotherCommitter"))) must_!= None

      // Before we start, establish that there were only 3 calls to the committer manager factory
      there were three(committerManagerActorFactory).props(any, any)

      // Remove one of the tasks
      curator.setData().forPath(CastleZkPaths.WorkerTasks + "/" + workerActor.worker.workerId,
        """
        {
          "tasks": [
                 { "topic": "perf", "partition": 3, "committerIds": ["kafkaCommitter", "anotherCommitter"] }
          ]
        } """.getBytes(Charset.forName("UTF-8")))

      // Wait for currentFetchers size to change
      mustEventuallyBeTrue(workerActor.currentCommitterManagers.size == 1)
      workerActor.worker.assignedTasksWatcherId must_== 4

      // The committer should be removed, without any further calls to the committer manager factory, so there should still
      // be only three calls to it
      there were three(committerManagerActorFactory).props(any, any)

      // The worker actor now has only 1 fetcher
      workerActor.currentCommitterManagers.size must_== 1

      mockMetricsLogger.getCountFor(const.Metrics.Failures) must_== 0

      // The worker actor will ignore assigned task changes that don't match the current watcher id
      workerActorRef ! AssignedTasksChanged(workerActor.workerActorId, 3)

      mockMetricsLogger.getCountFor(const.Metrics.Failures) must_== 1
    }

  }

}
