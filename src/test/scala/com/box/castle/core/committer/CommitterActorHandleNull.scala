package com.box.castle.core.committer

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestActorRef}
import com.box.castle.committer.api.{CommitterFactory, BatchCommittedSuccessfully, CommitResult, Committer}
import com.box.castle.consumer._
import com.box.castle.core.config.{InitialOffset, CastleConfig, CommitterConfig}
import com.box.castle.core.const
import com.box.castle.core.mock.MockTools
import com.box.castle.metrics.MetricsLogger
import com.box.castle.retry.strategies.RandomMinMaxStrategy
import com.box.castle.router.{RouterRef, Router, RouterFactory}
import com.box.castle.router.kafkadispatcher.KafkaDispatcherFactory
import com.box.castle.router.mock.{MockCastleSimpleConsumerFactory, MockCastleSimpleConsumer, MockMetricsLogger}
import com.box.castle.router.proxy.KafkaDispatcherProxyPoolFactory
import com.box.kafka.Broker
import kafka.api._
import kafka.cluster.KafkaConversions
import kafka.common.{OffsetMetadataAndError, TopicAndPartition, ErrorMapping}
import kafka.message.{MessageSet, Message}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.time.NoTimeConversions

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future, ExecutionContext}

/**
 * Created by yyin on 1/23/17.
 */
class CommitterActorHandleNull extends Specification
with Mockito with MockTools with NoTimeConversions {

  val offset = 23842L
  val mockNamespace = "mock_namespace"
  val committerId = "my_mock_committer_id"

  val consumerId = ConsumerId(mockNamespace + "_" + committerId)
  val err = ErrorMapping

  val timeout = FiniteDuration(10, TimeUnit.SECONDS)

  val broker = Broker(0, "mock.dev.box.net", 8000)
  val kafkaBroker = KafkaConversions.brokerToKafkaBroker(broker)

  val topicAndPartition = TopicAndPartition("preview", 1)
  val topicAndPartitionMetadata = TopicMetadata(topicAndPartition.topic,
    Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq.empty),
      PartitionMetadata(topicAndPartition.partition, Some(kafkaBroker), replicas = Seq.empty)))

  val topicAndPartition2Metadata = TopicMetadata("login",
    Seq(PartitionMetadata(0, Some(kafkaBroker), replicas = Seq(kafkaBroker))))

  val defaultMockTopicMetadata: Map[Int, TopicMetadataResponse] =
    Map(1 -> TopicMetadataResponse(Seq(topicAndPartitionMetadata, topicAndPartition2Metadata), 1),
      2 -> TopicMetadataResponse(Seq(topicAndPartitionMetadata, topicAndPartition2Metadata), 2),
      3 -> TopicMetadataResponse(Seq(topicAndPartitionMetadata, topicAndPartition2Metadata), 3))

  var nullCommitterCalled = 0
  val nullCommitterLock = new Object()

  //commitAsync returns a Future(null)
  class MockNullCommitter() extends Committer {
    override def commit(messageBatch: IndexedSeq[Message], metadata: Option[String] = None): CommitResult = {
      BatchCommittedSuccessfully()
    }

    override def commitAsync(messageBatch: IndexedSeq[Message], metadata: Option[String] = None)(implicit execctx: ExecutionContext): Future[CommitResult] = {
      nullCommitterLock synchronized {
        nullCommitterCalled += 1
        if (nullCommitterCalled < 2) {
          Future(null)(execctx)
        } else {
          Future(commit(messageBatch))(execctx)
        }
      }
    }

    override def close(): Unit = {}

    override def heartbeat(metadata: Option[String] = None): Option[String] = metadata
  }

  var asyncNullCommitterCalled = 0
  val asyncNullCommitterLock = new Object()

  //commitAsync retuens a null
  class MockNullAsyncCommitter() extends Committer {
    override def commit(messageBatch: IndexedSeq[Message], metadata: Option[String] = None): CommitResult = {
      BatchCommittedSuccessfully()
    }

    override def commitAsync(messageBatch: IndexedSeq[Message], metadata: Option[String] = None)(implicit execctx: ExecutionContext): Future[CommitResult] = {
      asyncNullCommitterLock synchronized {
        asyncNullCommitterCalled += 1
        if (asyncNullCommitterCalled < 2) {
          null
        } else {
          Future(commit(messageBatch))(execctx)
        }
      }
    }

    override def close(): Unit = {}

    override def heartbeat(metadata: Option[String] = None): Option[String] = metadata
  }


  class MockNullCommitterFactory(asyncNull: Boolean) extends CommitterFactory {
    override def create(topic: String, partition: Int, id: Int): Committer = {
      if (asyncNull)
        new MockNullAsyncCommitter
      else
        new MockNullCommitter
    }

    override def close(): Unit = {}

    override def noDataBackoffStrategy = RandomMinMaxStrategy(FiniteDuration(5, TimeUnit.MILLISECONDS),
      FiniteDuration(10, TimeUnit.MILLISECONDS))

    override def recoverableExceptionRetryStrategy = RandomMinMaxStrategy(FiniteDuration(5, TimeUnit.MILLISECONDS),
      FiniteDuration(10, TimeUnit.MILLISECONDS))
  }

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

  def createActorRef(mockFetchData: Map[Int, Map[TopicAndPartition, Map[Long, FetchResponsePartitionData]]],
                     mockMetricsLogger: MockMetricsLogger,
                     parallelismFactor: Int = 1,
                     hasSynchronousErrors: Boolean = false,
                     hasAsyncErrors: Boolean = false,
                     asyncNullCommitter: Boolean = false,
                     mockCommitterActorFactory: Option[CommitterActorFactory] = None,
                     mockCommitterConfig: Option[CommitterConfig] = None)
                    (implicit system: ActorSystem):
  (MockCastleSimpleConsumer, TestActorRef[CommitterActor]) = {

    val oldestOffsetResponse = PartitionOffsetsResponse(err.NoError, Seq(offset))
    val latestOffsetResponse = PartitionOffsetsResponse(err.NoError, Seq(offset))

    val castleConfig = mock[CastleConfig]
    castleConfig.namespace returns mockNamespace
    val committerConfig = mockCommitterConfig.getOrElse(createCommitterConfig(id = committerId, initialOffset = InitialOffset.oldest,
      parallelismFactor = parallelismFactor))
    val committerFactory = new MockNullCommitterFactory(asyncNullCommitter)

    val committerActorFactory = mockCommitterActorFactory.getOrElse(
      new CommitterActorFactory(castleConfig, Map(committerId -> committerFactory), mockMetricsLogger))

    val mockFetchConsumerOffsetData =
      Map(1 -> Map(consumerId ->
        Map(topicAndPartition -> OffsetMetadataAndError(11111L, error = err.UnknownTopicOrPartitionCode))),
        2 -> Map(consumerId ->
          Map(topicAndPartition -> OffsetMetadataAndError(11111L, error = err.UnknownTopicOrPartitionCode))),
        3 -> Map(consumerId ->
          Map(topicAndPartition -> OffsetMetadataAndError(11111L, error = err.UnknownTopicOrPartitionCode))),
        4 -> Map(consumerId ->
          Map(topicAndPartition -> OffsetMetadataAndError(11111L, error = err.UnknownTopicOrPartitionCode))),
        5 -> Map(consumerId ->
          Map(topicAndPartition -> OffsetMetadataAndError(11111L, error = err.UnknownTopicOrPartitionCode))))

    val mockOffsetData: Map[Int, Map[TopicAndPartition, Map[OffsetType, PartitionOffsetsResponse]]] =
      Map(1 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        2 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        3 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        4 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        5 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        6 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        7 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        8 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        9 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        10 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        11 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)),
        12 -> Map(topicAndPartition -> Map(EarliestOffset -> oldestOffsetResponse, LatestOffset -> latestOffsetResponse)))


    val mockCommitConsumerOffsetData =
      Map(1 -> Map(consumerId -> Map(topicAndPartition -> err.NoError)),
        2 -> Map(consumerId -> Map(topicAndPartition -> err.NoError)),
        3 -> Map(consumerId -> Map(topicAndPartition -> err.NoError))
      )

    val mockBoxSimpleConsumer: MockCastleSimpleConsumer = new MockCastleSimpleConsumer(timeout, broker, defaultMockTopicMetadata,
      fetchMockData = mockFetchData,
      fetchOffsetMockData = mockOffsetData,
      fetchConsumerOffsetMockData = mockFetchConsumerOffsetData,
      commitConsumerOffsetMockData = mockCommitConsumerOffsetData)

    val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)

    (mockBoxSimpleConsumer,
      TestActorRef[CommitterActor](committerActorFactory.props(topicAndPartition.topic, topicAndPartition.partition,
        committerConfig, RouterRef(routerActor)))
      )
  }

  "CommitterActor" should {
    "handle Future of null as a return value from asyncCommit" in new actorSystemAndCurator {
      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)

      val firstMessageSet: MessageSet = makeMockMessageSet(List(a, b, c))
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      val (mockBoxSimpleConsumer, committerActor) =
        createActorRef(mockFetchData, mockMetricsLogger)

      // this is a way for us to "block" here until the actor is restarted and the final committer is created
      // if we don't do this here and simply use Await.result, we will get a hold of a committer instance
      // that gets thrown away when the actor is restarted
      mustEventuallyBeTrue({
        println(s"CHECKING number of nullCommitter called ==> $nullCommitterCalled")
        nullCommitterCalled >= 2
      })

      val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

      // By default there should be only one mockCommitter
      mockCommitters.size must_== 1

      val mockCommitter = mockCommitters.head

      mockMetricsLogger.getCountFor(const.Metrics.NumMessagesCommitted) must_== 3

      // Make sure we count them in our metrics
      mockMetricsLogger.getCountFor(const.Metrics.RecoverableFailures) must_== 1
    }
  }
  "CommitterActor" should {
    "handle null as a return value from asyncCommit" in new actorSystemAndCurator {
      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)

      val firstMessageSet: MessageSet = makeMockMessageSet(List(a, b, c))
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      val (mockBoxSimpleConsumer, committerActor) =
        createActorRef(mockFetchData, mockMetricsLogger, asyncNullCommitter = true)

      // this is a way for us to "block" here until the actor is restarted and the final committer is created
      // if we don't do this here and simply use Await.result, we will get a hold of a committer instance
      // that gets thrown away when the actor is restarted
      mustEventuallyBeTrue({
        println(s"CHECKING number of asyncNullCommitter called ==> $asyncNullCommitterCalled")
        asyncNullCommitterCalled >= 2
      })

      val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

      // By default there should be only one mockCommitter
      mockCommitters.size must_== 1

      val mockCommitter = mockCommitters.head

      mockMetricsLogger.getCountFor(const.Metrics.NumMessagesCommitted) must_== 3

      // Make sure we count them in our metrics
      mockMetricsLogger.getCountFor(const.Metrics.RecoverableFailures) must_== 1
    }
  }
}
