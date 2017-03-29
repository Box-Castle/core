package com.box.castle.core.worker

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import akka.testkit.TestProbe
import com.box.castle.core.config.CommitterConfig
import com.box.castle.core.const.CastleZkPaths
import com.box.castle.core.mock.MockTools
import com.box.castle.core.worker.messages.AssignedTasksChanged
import com.box.castle.core.worker.tasks.{AssignedTasks, Task}
import com.box.castle.consumer.ClientId
import org.slf4s.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.specs2.mutable.Specification




class WorkerTest extends Specification with MockTools with org.specs2.mock.Mockito with Logging {
  val clientId = ClientId("mockClientId")

  class MockPersistentEphemeralNodeFactory(mockWorkerIdNode: Boolean, mockAvailabilityNode: Boolean) extends PersistentEphemeralNodeFactory {
    override def create(client: CuratorFramework,
               mode: PersistentEphemeralNode.Mode,
               basePath: String,
               data: Array[Byte]): PersistentEphemeralNode = {

      if (basePath.startsWith(CastleZkPaths.WorkerIds)) {
        if (mockWorkerIdNode) {
          val persistentEphemeralNode = mock[PersistentEphemeralNode]
          persistentEphemeralNode.waitForInitialCreate(
            Worker.DefaultNodeCreationTimeout.toSeconds, TimeUnit.SECONDS) returns false
        }
        else {
          new PersistentEphemeralNode(client, mode, basePath, data)
        }
      }
      else {
        assert(basePath.startsWith(CastleZkPaths.WorkerAvailability))
        // This is the availability node
        if (mockAvailabilityNode) {
          val persistentEphemeralNode = mock[PersistentEphemeralNode]
          persistentEphemeralNode.waitForInitialCreate(
            Worker.DefaultNodeCreationTimeout.toSeconds, TimeUnit.SECONDS) returns false
        }
        else {
          new PersistentEphemeralNode(client, mode, basePath, data)
        }
      }
    }
  }

  "Worker" should {
    "throw a WorkerException if it cannot create a worker id node in ZK within the allotted time" in new actorSystemAndCurator {
      val workerActor = mock[WorkerActorRef]
      val committerConfigs = mock[Iterable[CommitterConfig]]

      val ephemeralNodeFactory = new MockPersistentEphemeralNodeFactory(true, true)
      val workerActorId = 99
      val worker = new Worker(workerActor, workerActorId, clientId, curatorFactory, ephemeralNodeFactory)

      worker.createIfNotExistsAvailabilityNode() must throwA(
        new WorkerException(s"WorkerActor-$workerActorId failed to create a worker ID node in ZooKeeper"))
    }

    "have idempotent advertise availability functionality" in new actorSystemAndCurator {
      val workerFactory = new WorkerFactory(clientId, curatorFactory)
      val probe = TestProbe()(system)
      val workerActorId = 99
      val worker = workerFactory.create(WorkerActorRef(probe.ref), workerActorId)

      worker.createIfNotExistsAvailabilityNode()
      worker.createIfNotExistsAvailabilityNode()
      worker.createIfNotExistsAvailabilityNode()

      worker.castleCurator.checkExists().forPath(CastleZkPaths.WorkerAvailability + "/" + worker.workerId) must_!= null
    }

    "have idempotent create assigned tasks node functionality" in new actorSystemAndCurator {
      val workerFactory = new WorkerFactory(clientId, curatorFactory)
      val probe = TestProbe()(system)
      val workerActorId = 99
      val worker = workerFactory.create(WorkerActorRef(probe.ref), workerActorId)

      worker.createIfNotExistsAssignedTasksNode()
      worker.createIfNotExistsAssignedTasksNode()
      worker.createIfNotExistsAssignedTasksNode()

      worker.castleCurator.checkExists().forPath(CastleZkPaths.WorkerTasks + "/" + worker.workerId) must_!= null
    }

    "properly advertise and clean up after itself" in new actorSystemAndCurator {
      val workerFactory = new WorkerFactory(clientId, curatorFactory)
      val probe = TestProbe()(system)
      val workerActorId = 99
      val worker = workerFactory.create(WorkerActorRef(probe.ref), workerActorId)

      worker.getAssignedTasks

      // Worker id node must be created
      worker.workerId.endsWith("-worker-0000000000") must_== true

      val workerIdNode: Array[Byte] = curator.getData.forPath(CastleZkPaths.WorkerIds + "/" + worker.workerId)
      workerIdNode must_== worker.nodeData

      // Initial tasks node with zero tasks is created
      val workerTasksNode = new String(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/" + worker.workerId), Charset.forName("UTF-8"))
      workerTasksNode must_== AssignedTasks.ZeroTasks.toJson()

      // Availability node is created
      val availabilityNode: Array[Byte] = curator.getData.forPath(CastleZkPaths.WorkerAvailability + "/" + worker.workerId)
      availabilityNode must_== worker.nodeData

      worker.removeAvailability()
      curator.checkExists().forPath(CastleZkPaths.WorkerIds + "/" + worker.workerId) must_== null
      curator.checkExists().forPath(CastleZkPaths.WorkerTasks + "/" + worker.workerId) must_== null
      curator.checkExists().forPath(CastleZkPaths.WorkerAvailability + "/" + worker.workerId) must_== null
    }

    "the task watcher sends a process tasks change message to the worker actor if the tasks node changes" in new actorSystemAndCurator {
      val workerFactory = new WorkerFactory(clientId, curatorFactory)
      val probe = TestProbe()(system)
      val workerActorId = 99
      val worker = workerFactory.create(WorkerActorRef(probe.ref), workerActorId)

      worker.assignedTasksWatcherId must_== 0
      // This call should cause the watch to be setup on worker tasks node in ZK
      worker.getAssignedTasks must_== AssignedTasks(Set.empty[Task])
      worker.assignedTasksWatcherId must_== 1

      // This will trigger the watch and the worker actor should get another message sent to it
      worker.workerId.endsWith("-worker-0000000000") must_== true
      curator.setData().forPath(CastleZkPaths.WorkerTasks + "/" + worker.workerId, "NEW ASSIGNED TASKS DATA".getBytes(Charset.forName("UTF-8")))
      probe.expectMsg(AssignedTasksChanged(99,1))
    }
  }
}
