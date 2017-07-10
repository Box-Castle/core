package com.box.castle.core.leader

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestProbe}
import com.box.castle.committer.api.TopicFilter
import com.box.castle.core.CuratorFactory
import com.box.castle.core.config.{CommitterConfig, InitialOffset, LeaderConfig}
import com.box.castle.core.const.CastleZkPaths
import com.box.castle.core.leader.messages.{PollAvailableKafkaTopics, PollAvailableWorkers}
import com.box.castle.core.mock.MockTools
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.kafkadispatcher.KafkaDispatcherFactory
import com.box.castle.router.mock.{MockCastleSimpleConsumer, MockCastleSimpleConsumerFactory, MockMetricsLogger}
import com.box.castle.router.proxy.KafkaDispatcherProxyPoolFactory
import com.box.castle.router.{Router, RouterFactory, RouterRef}
import com.box.castle.core.worker.tasks.{AssignedTasks, Task}
import com.box.kafka.Broker
import com.box.castle.consumer.{CastleSimpleConsumerFactory, ClientId}
import org.slf4s.Logging
import kafka.api.{PartitionMetadata, TopicMetadata, TopicMetadataResponse}
import kafka.cluster.KafkaConversions
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.duration.FiniteDuration

class LeaderTest extends Specification with MockTools with Mockito with Logging {

  val clientId = ClientId("clientId-B")

  def getLeaderFactory(curatorFactory: CuratorFactory,
                       clientId: ClientId,
                       committerConfigs: Iterable[CommitterConfig],
                       metricsLogger: MetricsLogger = MetricsLogger.defaultLogger) = {
    val leaderConfig = LeaderConfig()
    val filterMap = Map ("kafkaCommitterId" -> new TopicFilter())
    val taskManager = new TaskManager(leaderConfig, committerConfigs, filterMap)
    new LeaderFactory(leaderConfig,
      clientId, curatorFactory, taskManager, metricsLogger) {
    }
  }

  val timeout = FiniteDuration(10, TimeUnit.SECONDS)

  val broker = Broker(0, "mock.dev.box.net", 8000)
  val kafkaBroker = KafkaConversions.brokerToKafkaBroker(broker)

  val topicAndPartitionMetadata = TopicMetadata("preview",
    Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq.empty),
      PartitionMetadata(1, Some(kafkaBroker), replicas = Seq.empty)))

  val topicAndPartition2Metadata = TopicMetadata("login",
    Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq(kafkaBroker))))

  val defaultMockTopicMetadata: Map[Int, TopicMetadataResponse] =
    Map(1 -> TopicMetadataResponse(Seq(topicAndPartitionMetadata, topicAndPartition2Metadata), 1),
      2 -> TopicMetadataResponse(Seq(topicAndPartitionMetadata, topicAndPartition2Metadata), 2),
      3 -> TopicMetadataResponse(Seq(topicAndPartitionMetadata, topicAndPartition2Metadata), 3))



  def createRouterActor[T <: akka.actor.Actor](mockMetricsLogger: MetricsLogger,
                                               mockBoxSimpleConsumer: MockCastleSimpleConsumer)
                                              (implicit system: ActorSystem): TestActorRef[T] = {
    val broker = mockBoxSimpleConsumer.broker
    val boxSimpleConsumerFactory = new MockCastleSimpleConsumerFactory(Map(broker -> mockBoxSimpleConsumer))
    createRouterActor[T](boxSimpleConsumerFactory, mockMetricsLogger, Set(broker))
  }

  def createRouterActor[T <: akka.actor.Actor](boxSimpleConsumerFactory: CastleSimpleConsumerFactory,
                                               mockMetricsLogger: MetricsLogger,
                                               brokers: Set[Broker])(implicit system: ActorSystem): TestActorRef[T] = {
    val kafkaDispatcherFactory = new KafkaDispatcherFactory(boxSimpleConsumerFactory, mockMetricsLogger)
    val kafkaDispatcherProxyPoolFactory = new KafkaDispatcherProxyPoolFactory(kafkaDispatcherFactory, 1024 * 1024, mockMetricsLogger)
    val routerActorFactory = new RouterFactory(kafkaDispatcherProxyPoolFactory, brokers, mockMetricsLogger)

    val requester = TestProbe()
    TestActorRef[T](routerActorFactory.props(), requester.ref, "router")
  }


  "Leader" should {
    "correctly process the case when there is no worker availability path in ZK" in new actorSystemAndCurator {
      val mockMetricsLogger = new MockMetricsLogger()

      val committerConfig = createCommitterConfig(InitialOffset.latest, "kafkaCommitterId")

      val leaderFactory = getLeaderFactory(curatorFactory, clientId, List(committerConfig), mockMetricsLogger)
      val leaderActorFactory = new LeaderActorFactory(leaderFactory, LeaderConfig(), mockMetricsLogger)

      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout, broker, defaultMockTopicMetadata)
      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      val leaderActor = TestActorRef[LeaderActor](leaderActorFactory.props(RouterRef(routerActor)))

      mustEventuallyBeTrue(mockBoxSimpleConsumer.getTopicMetadataCalls.get == 2)

      leaderActor.underlyingActor.leader.currentTasks must_== Set.empty
      leaderActor.underlyingActor.leader.currentWorkers must_== Map[String, Set[Task]]()

      mockBoxSimpleConsumer.getNumErrors must_== 0
      mockBoxSimpleConsumer.getTopicMetadataCalls.get must_== 2
    }

    "correctly process the case when there is a worker availability path in ZK, but 0 workers in it" in new actorSystemAndCurator {
      val mockMetricsLogger = new MockMetricsLogger()
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerAvailability)

      val committerConfig = createCommitterConfig(InitialOffset.latest, "kafkaCommitterId")

      val leaderFactory = getLeaderFactory(curatorFactory, clientId, List(committerConfig), mockMetricsLogger)
      val leaderActorFactory = new LeaderActorFactory(leaderFactory, LeaderConfig(), mockMetricsLogger)

      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout, broker, defaultMockTopicMetadata)
      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      val leaderActor = TestActorRef[LeaderActor](leaderActorFactory.props(RouterRef(routerActor)))

      mustEventuallyBeTrue({
        (leaderActor.underlyingActor.leader.currentTasks == Set(Task("preview", 0, Set(committerConfig.id)),
                                                               Task("preview", 1, Set(committerConfig.id)),
                                                               Task("login", 0, Set(committerConfig.id)))
        &&
        leaderActor.underlyingActor.leader.currentWorkers == Map.empty)
      })

      mockBoxSimpleConsumer.getNumErrors must_== 0
      mockBoxSimpleConsumer.getTopicMetadataCalls.get must_== 2
    }

    "assign tasks when it becomes the leader" in new actorSystemAndCurator {
      val mockMetricsLogger = new MockMetricsLogger()
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerAvailability + "/worker-001")
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerTasks + "/worker-001", AssignedTasks.ZeroTasks.toJsonBytes())

      val initialTasks = new String(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"), Charset.forName("UTF-8"))

      val committerConfig = createCommitterConfig(InitialOffset.latest, "kafkaCommitterId")

      val leaderFactory = getLeaderFactory(curatorFactory, clientId, List(committerConfig), mockMetricsLogger)
      val leaderActorFactory = new LeaderActorFactory(leaderFactory, LeaderConfig(), mockMetricsLogger)

      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout, broker, defaultMockTopicMetadata)
      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      val leaderActor = TestActorRef[LeaderActor](leaderActorFactory.props(RouterRef(routerActor)))

      mustEventuallyBeTrue({
        val newTasks = new String(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"), Charset.forName("UTF-8"))
        initialTasks != newTasks
      })
      val rawJsonBytes = curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001")
      val rawJsonString: String = new String(rawJsonBytes, Charset.forName("UTF-8"))

      // The leader should have assigned the kafka topic tasks to the single available worker
      val assignedTasks = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))

      assignedTasks.tasks must_== Set(
        Task("preview", 0, Set(committerConfig.id)),
        Task("preview", 1, Set(committerConfig.id)),
        Task("login", 0, Set(committerConfig.id)))

      mockBoxSimpleConsumer.getNumErrors must_== 0
    }

    "properly process kafka topics changes" in new actorSystemAndCurator {
      val mockMetricsLogger = new MockMetricsLogger()
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerAvailability + "/worker-001")
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerTasks + "/worker-001", AssignedTasks.ZeroTasks.toJsonBytes())
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerAvailability + "/worker-002")
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerTasks + "/worker-002", AssignedTasks.ZeroTasks.toJsonBytes())

      val committerConfig = createCommitterConfig(InitialOffset.latest, "kafkaCommitterId")

      val leaderFactory = getLeaderFactory(curatorFactory, clientId, List(committerConfig), mockMetricsLogger)
      // Here we set the timeouts to be incredibly long so we can manually send these messages ourselves
      val leaderActorFactory = new LeaderActorFactory(leaderFactory, LeaderConfig(
        kafkaTopicsPollInterval = FiniteDuration(10, TimeUnit.DAYS),
        availableWorkersPollInterval = FiniteDuration(10, TimeUnit.DAYS)
      ), mockMetricsLogger)

      val previewWithOnePartitionTopicMetadata = TopicMetadata("preview",
        Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq.empty)))

      val previewWithTwoPartitionsTopicMetadata = TopicMetadata("preview",
        Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq.empty),
          PartitionMetadata(1, Some(kafkaBroker), replicas = Seq.empty)))

      val loginWithOnePartitionTopicMetadata = TopicMetadata("login",
        Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq(kafkaBroker))))

      val loginWithTwoPartitionsTopicMetadata = TopicMetadata("login",
        Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq(kafkaBroker)),
          PartitionMetadata(1, Some(kafkaBroker), replicas = Seq(kafkaBroker))))

      val mockTopicMetadata: Map[Int, TopicMetadataResponse] = Map.empty
      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout, broker, mockTopicMetadata)
      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      val leaderActor = TestActorRef[LeaderActor](leaderActorFactory.props(RouterRef(routerActor)))

      mockBoxSimpleConsumer.topicMetadataForAllCorrelationIds = Some(Seq(previewWithOnePartitionTopicMetadata))

      var assignedTasks002 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-002"))

      mustEventuallyBeTrue({
        assignedTasks002 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-002"))
        // When the leader starts up it fetches topic metadata twice, once for the workers changing, and once for the
        // initial get of the topics
        assignedTasks002.tasks == Set(Task("preview", 0, Set(committerConfig.id)))
      })
      println("topic change picked up preview, 0")

      mockBoxSimpleConsumer.topicMetadataForAllCorrelationIds = Some(Seq(previewWithTwoPartitionsTopicMetadata))

      var assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
      assignedTasks001.tasks must_== Set.empty

      // Should pick up preview, 1
      leaderActor ! PollAvailableKafkaTopics

      mustEventuallyBeTrue({
        assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
        assignedTasks001.tasks == Set(Task("preview", 1, Set(committerConfig.id)))
      })
      println("topic change picked up preview, 1")

      assignedTasks002 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-002"))
      assignedTasks002.tasks must_== Set(Task("preview", 0, Set(committerConfig.id)))

      mockBoxSimpleConsumer.topicMetadataForAllCorrelationIds =
        Some(Seq(previewWithTwoPartitionsTopicMetadata, loginWithOnePartitionTopicMetadata))

      // Should pick up login, 0
      leaderActor ! PollAvailableKafkaTopics

      mustEventuallyBeTrue({
        assignedTasks002 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-002"))
        assignedTasks002.tasks == Set(Task("preview", 0, Set(committerConfig.id)),
          Task("login", 0, Set(committerConfig.id)))
      })
      println("topic change picked up login, 0")

      assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
      assignedTasks001.tasks must_== Set(Task("preview", 1, Set(committerConfig.id)))

      mockBoxSimpleConsumer.topicMetadataForAllCorrelationIds =
        Some(Seq(previewWithTwoPartitionsTopicMetadata, loginWithTwoPartitionsTopicMetadata))
      // Should pick up login, 1
      leaderActor ! PollAvailableKafkaTopics

      mustEventuallyBeTrue({
        assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
        assignedTasks001.tasks == Set(Task("preview", 1, Set(committerConfig.id)),
          Task("login", 1, Set(committerConfig.id)))
      })
      println("topic change picked up login, 1")

      assignedTasks002 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-002"))
      assignedTasks002.tasks must_== Set(Task("preview", 0, Set(committerConfig.id)),
        Task("login", 0, Set(committerConfig.id)))

      // Make sure the available workers watch still works properly after processing all these topics changes
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerAvailability + "/worker-003")
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerTasks + "/worker-003", AssignedTasks.ZeroTasks.toJsonBytes())

      leaderActor ! PollAvailableWorkers

      mustEventuallyBeTrue({
        val assignedTasks003 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-003"))
        assignedTasks003.tasks == Set(Task("preview", 1, Set(committerConfig.id)))
      })
      println("picked up available worker change after adding worker-003")

      assignedTasks002 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-002"))
      assignedTasks002.tasks must_== Set(Task("preview", 0, Set(committerConfig.id)),
        Task("login", 0, Set(committerConfig.id)))

      assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
      assignedTasks001.tasks must_== Set(Task("login", 1, Set(committerConfig.id)))

      mockBoxSimpleConsumer.getNumErrors must_== 0
    }

    "properly process available worker changes" in new actorSystemAndCurator {
      val mockMetricsLogger = new MockMetricsLogger()
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerAvailability + "/worker-001")
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerTasks + "/worker-001", AssignedTasks.ZeroTasks.toJsonBytes())

      val committerConfig = createCommitterConfig(InitialOffset.latest, "kafkaCommitterId")

      val leaderFactory = getLeaderFactory(curatorFactory, clientId, List(committerConfig), mockMetricsLogger)
      val leaderActorFactory = new LeaderActorFactory(leaderFactory, LeaderConfig(
        kafkaTopicsPollInterval = FiniteDuration(120, TimeUnit.SECONDS),
        availableWorkersPollInterval = FiniteDuration(120, TimeUnit.SECONDS)
      ), mockMetricsLogger)

      val previewTopicMetadata = TopicMetadata("preview",
        Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq.empty),
          PartitionMetadata(1, Some(kafkaBroker), replicas = Seq.empty)))

      val loginTopicMetadata = TopicMetadata("login",
        Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq(kafkaBroker)),
          PartitionMetadata(1, Some(kafkaBroker), replicas = Seq(kafkaBroker))))

      val mockTopicMetadata: Map[Int, TopicMetadataResponse] =
        Map(1 -> TopicMetadataResponse(Seq(previewTopicMetadata, loginTopicMetadata), 1),
          2 -> TopicMetadataResponse(Seq(previewTopicMetadata, loginTopicMetadata), 2),
          3 -> TopicMetadataResponse(Seq(previewTopicMetadata, loginTopicMetadata), 3),
          4 -> TopicMetadataResponse(Seq(previewTopicMetadata, loginTopicMetadata), 4),
          5 -> TopicMetadataResponse(Seq(previewTopicMetadata, loginTopicMetadata), 5),
          6 -> TopicMetadataResponse(Seq(previewTopicMetadata, loginTopicMetadata), 6),
          7 -> TopicMetadataResponse(Seq(previewTopicMetadata, loginTopicMetadata), 7),
          8 -> TopicMetadataResponse(Seq(previewTopicMetadata, loginTopicMetadata), 8),
          9 -> TopicMetadataResponse(Seq(previewTopicMetadata, loginTopicMetadata), 9))

      val tasks = Set(Task("preview", 0, Set(committerConfig.id)),
        Task("preview", 1, Set(committerConfig.id)),
        Task("login", 0, Set(committerConfig.id)),
        Task("login", 1, Set(committerConfig.id)))

      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout, broker, mockTopicMetadata)
      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      val leaderActor = TestActorRef[LeaderActor](leaderActorFactory.props(RouterRef(routerActor)))

      var assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))

      mustEventuallyBeTrue({
        assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
        assignedTasks001.tasks == tasks
      })

      // Add a new worker
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerAvailability + "/worker-002")
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerTasks + "/worker-002", AssignedTasks.ZeroTasks.toJsonBytes())

      leaderActor ! PollAvailableWorkers

      mustEventuallyBeTrue({
        assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
        assignedTasks001.tasks.size == 2
      })

      var assignedTasks002 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-002"))
      assignedTasks002.tasks.size must_== 2

      assignedTasks001.tasks ++ assignedTasks002.tasks must_== tasks

      // Add a new worker
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerAvailability + "/worker-003")
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerTasks + "/worker-003", AssignedTasks.ZeroTasks.toJsonBytes())

      leaderActor ! PollAvailableWorkers
      mustEventuallyBeTrue({
        assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
        assignedTasks001.tasks.size == 1
      })

      assignedTasks002 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-002"))
      assignedTasks002.tasks.size must_== 2

      var assignedTasks003 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-003"))
      assignedTasks003.tasks.size must_== 1

      assignedTasks001.tasks ++ assignedTasks002.tasks ++ assignedTasks003.tasks must_== tasks

      // Add a new worker
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerAvailability + "/worker-004")
      curator.create().creatingParentsIfNeeded().forPath(CastleZkPaths.WorkerTasks + "/worker-004", AssignedTasks.ZeroTasks.toJsonBytes())

      leaderActor ! PollAvailableWorkers

      mustEventuallyBeTrue({
        assignedTasks002 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-002"))
        assignedTasks002.tasks.size == 1
      })

      assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
      assignedTasks001.tasks.size must_== 1

      assignedTasks003 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-003"))
      assignedTasks003.tasks.size must_== 1

      var assignedTasks004 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-004"))
      assignedTasks004.tasks.size must_== 1

      assignedTasks001.tasks ++ assignedTasks002.tasks ++ assignedTasks003.tasks ++ assignedTasks004.tasks must_== tasks

      // Remove a worker
      curator.delete().forPath(CastleZkPaths.WorkerAvailability + "/worker-002")
      leaderActor ! PollAvailableWorkers

      mustEventuallyBeTrue({
        assignedTasks004 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-004"))
        assignedTasks004.tasks.size == 2
      })
      assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
      assignedTasks001.tasks.size must_== 1

      assignedTasks003 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-003"))
      assignedTasks003.tasks.size must_== 1

      assignedTasks001.tasks ++ assignedTasks003.tasks ++ assignedTasks004.tasks must_== tasks

      // Remove a worker
      curator.delete().forPath(CastleZkPaths.WorkerAvailability + "/worker-003")
      leaderActor ! PollAvailableWorkers

      mustEventuallyBeTrue({
        assignedTasks001 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-001"))
        assignedTasks001.tasks.size == 2
      })

      assignedTasks004 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-004"))
      assignedTasks004.tasks.size must_== 2

      assignedTasks001.tasks ++ assignedTasks004.tasks must_== tasks

      // Remove a worker
      curator.delete().forPath(CastleZkPaths.WorkerAvailability + "/worker-001")
      leaderActor ! PollAvailableWorkers

      mustEventuallyBeTrue({
        assignedTasks004 = AssignedTasks.fromJson(curator.getData.forPath(CastleZkPaths.WorkerTasks + "/worker-004"))
        assignedTasks004.tasks.size == 4
      })

      assignedTasks004.tasks must_== tasks

      mockBoxSimpleConsumer.getNumErrors must_== 0
      mockBoxSimpleConsumer.getTopicMetadataCalls.get >= 8 must_== true
    }
  }
}
