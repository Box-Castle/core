package com.box.castle.core.committer

import com.box.castle.core.common.CastleFatalException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, Props, ActorSystem}
import akka.testkit.{TestProbe, TestActorRef, TestKit}
import com.box.castle.committer.api._
import com.box.castle.core.config.{CommitterConfig, CastleConfig, InitialOffset}
import com.box.castle.core.const
import com.box.castle.core.mock.MockTools
import com.box.castle.core.committer.states.{FetchingData, Idling}
import com.box.castle.core.committer.states.Idling._
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.kafkadispatcher.KafkaDispatcherFactory
import com.box.castle.router.messages.OffsetAndMetadata
import com.box.castle.router.mock.{MockCastleSimpleConsumerFactory, MockMetricsLogger, MockCastleSimpleConsumer}
import com.box.castle.router.proxy.KafkaDispatcherProxyPoolFactory
import com.box.castle.router.{RouterFactory, Router, RouterRef}
import com.box.castle.retry.strategies.RandomMinMaxStrategy
import com.box.kafka.Broker
import com.box.castle.consumer._
import kafka.api._
import kafka.cluster.KafkaConversions
import kafka.common.{ErrorMapping, OffsetMetadataAndError, TopicAndPartition}
import kafka.message.{MessageSet, Message}
import org.joda.time.{DateTime, Duration, Period}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.time.NoTimeConversions

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}



class CommitterActorTest extends Specification
with Mockito with MockTools with NoTimeConversions {

  val lock = new Object()
  var numSynchronousCommitCallsWithErrorEnabled = 0
  var numAsyncCommitCallsWithErrorEnabled = 0

  val TESTHEARTBEATCADENCEINMILLIS = 3000
  val TESTFETCHDELAYINMILLIS = 5000


  class MockCommitter(hasSynchronousErrors: Boolean, hasAsyncErrors: Boolean, val id: Int) extends Committer {
    var committedMessageBatches = Queue.empty[IndexedSeq[Message]]

    override def commit(messageBatch: IndexedSeq[Message], metadata: Option[String] = None): CommitResult = {
      committedMessageBatches = committedMessageBatches.enqueue(messageBatch)
      BatchCommittedSuccessfully()
    }

    override def commitAsync(messageBatch: IndexedSeq[Message], metadata: Option[String] = None)(implicit execctx: ExecutionContext): Future[CommitResult] = {
      lock synchronized {
        if (hasSynchronousErrors) {
          if (numSynchronousCommitCallsWithErrorEnabled < 2) {
            numSynchronousCommitCallsWithErrorEnabled += 1
            throw new Exception("synchronous exception")
          }
          numSynchronousCommitCallsWithErrorEnabled += 1
        }

        if (hasAsyncErrors) {
          if (numAsyncCommitCallsWithErrorEnabled < 3) {
            numAsyncCommitCallsWithErrorEnabled += 1
            throw new Exception("async exception")
          }
          numAsyncCommitCallsWithErrorEnabled += 1
        }
      }
      Future(commit(messageBatch))(execctx)
    }

    override def close(): Unit = {}

    var lastheartbeatTime = DateTime.now()
    var heartbeatCount = 0

    def mockSleepDuringHeartbeat(): Int = 5000

    override def heartbeat(metadata: Option[String] = None): Option[String] = {
      lastheartbeatTime = DateTime.now()
      Thread.sleep(mockSleepDuringHeartbeat)
      heartbeatCount += 1
      metadata
    }
  }

  class MockCommitterFactory(hasSynchronousErrors: Boolean, hasAsyncErrors: Boolean) extends CommitterFactory {

    override def create(topic: String, partition: Int, id: Int): Committer = new MockCommitter(hasSynchronousErrors, hasAsyncErrors, id)

    override def close(): Unit = {}

    override def noDataBackoffStrategy = RandomMinMaxStrategy(FiniteDuration(5, TimeUnit.MILLISECONDS),
      FiniteDuration(10, TimeUnit.MILLISECONDS))

    override def recoverableExceptionRetryStrategy = RandomMinMaxStrategy(FiniteDuration(5, TimeUnit.MILLISECONDS),
      FiniteDuration(10, TimeUnit.MILLISECONDS))
  }

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
                     mockCommitterFactory: Option[CommitterFactory] = None,
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
    val committerFactory = mockCommitterFactory.getOrElse(new MockCommitterFactory(hasSynchronousErrors, hasAsyncErrors))

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
    "handle the basic case with an empty message set in the middle" in new actorSystemAndCurator {
      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)

      val firstMessageSet: MessageSet = makeMockMessageSet(List(a, b, c))
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

      val secondMessageSet = makeMockMessageSet(List())
      val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

      val d = makeMockMessageAndOffset(offset + 3, offset + 4, 235235)
      val e = makeMockMessageAndOffset(offset + 4, offset + 5, 8334)
      val f = makeMockMessageAndOffset(offset + 5, offset + 6, 43)

      val thirdMessageSet = makeMockMessageSet(List(d, e, f))
      val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
          2 -> Map(topicAndPartition -> Map(offset + 3 -> secondPartitionData)),
          3 -> Map(topicAndPartition -> Map(offset + 3 -> thirdPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      val (mockBoxSimpleConsumer, committerActor) = createActorRef(mockFetchData, mockMetricsLogger)

      val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

      // By default there should be only one mockCommitter
      mockCommitters.size must_== 1

      val mockCommitter = mockCommitters.head

      mustEventuallyBeTrue({
        mockCommitter.asInstanceOf[MockCommitter].committedMessageBatches ==
          Queue(firstMessageSet.toIndexedSeq.map(_.message), thirdMessageSet.toIndexedSeq.map(_.message))
      })

      mustEventuallyBeTrue({
        mockBoxSimpleConsumer.committedConsumerOffsets ==
          Queue((consumerId, topicAndPartition, OffsetMetadataAndError(offset + 3)), (consumerId, topicAndPartition, OffsetMetadataAndError(offset + 6)))
      })

      mockMetricsLogger.getCountFor(const.Metrics.NumMessagesCommitted) must_== 6

      mockMetricsLogger.getCountFor(const.Metrics.BytesProcessed) must_== MessageSet.messageSetSize(
        firstMessageSet.iterator.map(_.message).toList) +
        MessageSet.messageSetSize(thirdMessageSet.iterator.map(_.message).toList)
    }
  }

  "CommitterActor" should {
    "correctly handle parallelism" in new actorSystemAndCurator {

      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)
      val d = makeMockMessageAndOffset(offset + 3, offset + 4, 235235)
      val e = makeMockMessageAndOffset(offset + 4, offset + 5, 8334)
      val f = makeMockMessageAndOffset(offset + 5, offset + 6, 43)
      val g = makeMockMessageAndOffset(offset + 6, offset + 7, 23424)

      val firstMessageSet: MessageSet = makeMockMessageSet(List(a, b, c, d, e, f, g))
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

      val secondMessageSet = makeMockMessageSet(List())
      val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

      val h = makeMockMessageAndOffset(offset + 7, offset + 8, 384832)

      val thirdMessageSet = makeMockMessageSet(List(h))
      val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
          2 -> Map(topicAndPartition -> Map(offset + 7 -> secondPartitionData)),
          3 -> Map(topicAndPartition -> Map(offset + 7 -> thirdPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      val (mockBoxSimpleConsumer, committerActor) = createActorRef(mockFetchData, mockMetricsLogger, 2)

      val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

      // By default there should be only one mockCommitter
      mockCommitters.size must_== 2

      val mockCommitterOne = mockCommitters.head
      mockCommitterOne.asInstanceOf[MockCommitter].id must_== 0
      val mockCommitterOneSplitBatch = makeMockMessageSet(List(a, b, c, d))

      mustEventuallyBeTrue({
        mockCommitterOne.asInstanceOf[MockCommitter].committedMessageBatches ==
          Queue(mockCommitterOneSplitBatch.toIndexedSeq.map(_.message), thirdMessageSet.toIndexedSeq.map(_.message))
      })

      val mockCommitterTwo = mockCommitters(1)
      mockCommitterTwo.asInstanceOf[MockCommitter].id must_== 1
      val mockCommitterTwoSplitBatch = makeMockMessageSet(List(e, f, g))

      mustEventuallyBeTrue({
        mockCommitterTwo.asInstanceOf[MockCommitter].committedMessageBatches ==
          Queue(mockCommitterTwoSplitBatch.toIndexedSeq.map(_.message))
      })

      mustEventuallyBeTrue({
        mockBoxSimpleConsumer.committedConsumerOffsets ==
          Queue((consumerId, topicAndPartition, OffsetMetadataAndError(offset + 7)), (consumerId, topicAndPartition, OffsetMetadataAndError(offset + 8)))
      })

      mockMetricsLogger.getCountFor(const.Metrics.NumMessagesCommitted) must_== 8

      mockMetricsLogger.getCountFor(const.Metrics.BytesProcessed) must_== MessageSet.messageSetSize(
        firstMessageSet.iterator.map(_.message).toList) +
        MessageSet.messageSetSize(thirdMessageSet.iterator.map(_.message).toList)
    }
  }

  "CommitterActor" should {
    "handle synchronous errors inside the commitAsync call of the committer" in new actorSystemAndCurator {
      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)

      val firstMessageSet: MessageSet = makeMockMessageSet(List(a, b, c))
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

      val secondMessageSet = makeMockMessageSet(List())
      val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

      val d = makeMockMessageAndOffset(offset + 3, offset + 4, 235235)
      val e = makeMockMessageAndOffset(offset + 4, offset + 5, 8334)
      val f = makeMockMessageAndOffset(offset + 5, offset + 6, 43)

      val thirdMessageSet = makeMockMessageSet(List(d, e, f))
      val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
          2 -> Map(topicAndPartition -> Map(offset + 3 -> secondPartitionData)),
          3 -> Map(topicAndPartition -> Map(offset + 3 -> thirdPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      val (mockBoxSimpleConsumer, committerActor) =
        createActorRef(mockFetchData, mockMetricsLogger, hasSynchronousErrors = true)

      // this is a way for us to "block" here until the actor is restarted and the final committer is created
      // if we don't do this here and simply use Await.result, we will get a hold of a committer instance
      // that gets thrown away when the actor is restarted
      mustEventuallyBeTrue({
        println(s"CHECKING numSynchronousCommitCallsWithErrorEnabled ==> $numSynchronousCommitCallsWithErrorEnabled")
        numSynchronousCommitCallsWithErrorEnabled >= 3
      })

      val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

      // By default there should be only one mockCommitter
      mockCommitters.size must_== 1

      val mockCommitter = mockCommitters.head

      mustEventuallyBeTrue({
        mockCommitter.asInstanceOf[MockCommitter].committedMessageBatches ==
          Queue(firstMessageSet.toIndexedSeq.map(_.message), thirdMessageSet.toIndexedSeq.map(_.message))
      })

      mustEventuallyBeTrue({
        mockBoxSimpleConsumer.committedConsumerOffsets ==
          Queue((consumerId, topicAndPartition, OffsetMetadataAndError(offset + 3)), (consumerId, topicAndPartition, OffsetMetadataAndError(offset + 6)))
      })

      mockMetricsLogger.getCountFor(const.Metrics.NumMessagesCommitted) must_== 6

      mockMetricsLogger.getCountFor(const.Metrics.BytesProcessed) must_== MessageSet.messageSetSize(
        firstMessageSet.iterator.map(_.message).toList) +
        MessageSet.messageSetSize(thirdMessageSet.iterator.map(_.message).toList)

      // Make sure we count them in our metrics
      mockMetricsLogger.getCountFor(const.Metrics.RecoverableFailures) must_== 2
    }
  }

  "CommitterActor" should {
    "handle async errors inside the commitAsync call of the committer" in new actorSystemAndCurator {
      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)

      val firstMessageSet: MessageSet = makeMockMessageSet(List(a, b, c))
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

      val secondMessageSet = makeMockMessageSet(List())
      val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

      val d = makeMockMessageAndOffset(offset + 3, offset + 4, 235235)
      val e = makeMockMessageAndOffset(offset + 4, offset + 5, 8334)
      val f = makeMockMessageAndOffset(offset + 5, offset + 6, 43)

      val thirdMessageSet = makeMockMessageSet(List(d, e, f))
      val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
          2 -> Map(topicAndPartition -> Map(offset + 3 -> secondPartitionData)),
          3 -> Map(topicAndPartition -> Map(offset + 3 -> thirdPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      val (mockBoxSimpleConsumer, committerActor) =
        createActorRef(mockFetchData, mockMetricsLogger, hasAsyncErrors = true)

      // this is a way for us to "block" here until the actor is restarted and the final committer is created
      // if we don't do this here and simply use Await.result, we will get a hold of a committer instance
      // that gets thrown away when the actor is restarted
      mustEventuallyBeTrue({
        println(s"CHECKING numAsyncCommitCallsWithErrorEnabled ==> $numAsyncCommitCallsWithErrorEnabled")
        numAsyncCommitCallsWithErrorEnabled >= 4
      })

      val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

      // By default there should be only one mockCommitter
      mockCommitters.size must_== 1

      val mockCommitter = mockCommitters.head

      mustEventuallyBeTrue({
        mockCommitter.asInstanceOf[MockCommitter].committedMessageBatches ==
          Queue(firstMessageSet.toIndexedSeq.map(_.message), thirdMessageSet.toIndexedSeq.map(_.message))
      })

      mustEventuallyBeTrue({
        mockBoxSimpleConsumer.committedConsumerOffsets ==
          Queue((consumerId, topicAndPartition, OffsetMetadataAndError(offset + 3)), (consumerId, topicAndPartition, OffsetMetadataAndError(offset + 6)))
      })

      mockMetricsLogger.getCountFor(const.Metrics.NumMessagesCommitted) must_== 6

      mockMetricsLogger.getCountFor(const.Metrics.BytesProcessed) must_== MessageSet.messageSetSize(
        firstMessageSet.iterator.map(_.message).toList) +
        MessageSet.messageSetSize(thirdMessageSet.iterator.map(_.message).toList)

      // Make sure we count them in our metrics
      mockMetricsLogger.getCountFor(const.Metrics.RecoverableFailures) must_== 3
    }
  }

  "CommitterActor" should {
    "resend partial batch if committer requests it" in new actorSystemAndCurator {
      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)
      val d = makeMockMessageAndOffset(offset + 3, offset + 4, 235235)
      val e = makeMockMessageAndOffset(offset + 4, offset + 5, 8334)
      val f = makeMockMessageAndOffset(offset + 5, offset + 6, 43)

      val messageSet: MessageSet = makeMockMessageSet(List(a, b, c, d, e, f))
      val firstPartitionData = FetchResponsePartitionData(messages = messageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)))

      val messages = messageSet.toIndexedSeq.map(_.message)
      val mockMetricsLogger = new MockMetricsLogger()

      class RateLimitedCommitter extends Committer {
        private val tooManyRequestsCommitterLock = new Object()

        var commitCounter = 0
        var messagesInCommitCall: IndexedSeq[IndexedSeq[Message]] = IndexedSeq.empty

        override def commit(messageBatch: IndexedSeq[Message], metadata: Option[String]): CommitResult = {
          tooManyRequestsCommitterLock synchronized {
            if (commitCounter < 3) {
              commitCounter += 1

              val (consume, retry) = (messageBatch.splitAt(2))
              messagesInCommitCall :+= consume
              RetryBatch(0.seconds, retry)
            }
            else {
              messagesInCommitCall :+= messageBatch
              BatchCommittedSuccessfully()
            }
          }
        }
      }

      val mockCommitter = new RateLimitedCommitter()

      val mockCommitterFactory = new CommitterFactory {
        override def create(topic: String, partition: Int, id: Int): Committer = mockCommitter
      }

      val (mockBoxSimpleConsumer, committerActor) =
        createActorRef(mockFetchData, mockMetricsLogger,
          mockCommitterFactory = Some(mockCommitterFactory))

      mustEventuallyBeTrue({
        mockCommitter.commitCounter == 3 &&
          mockCommitter.messagesInCommitCall.forall(_.size == 2)
      })
    }
  }

  "CommitterActor" should {
    "resend partial batches in correct order" in new actorSystemAndCurator {
      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)
      val d = makeMockMessageAndOffset(offset + 3, offset + 4, 235235)
      val e = makeMockMessageAndOffset(offset + 4, offset + 5, 8334)
      val f = makeMockMessageAndOffset(offset + 5, offset + 6, 43)
      val g = makeMockMessageAndOffset(offset + 6, offset + 7, 23424)

      val messageSet: MessageSet = makeMockMessageSet(List(a, b, c, d, e, f, g))
      val partitionData = FetchResponsePartitionData(messages = messageSet)

      val allMessages = (messageSet).map(_.message)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> partitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      /*
       * Definitions of the mock CommitterActor and mock CommitterActorFactory for this test
       */

      class MockCommitterActor(topic: String,
                               partition: Int,
                               castleConfig: CastleConfig,
                               override val committerConfig: CommitterConfig,
                               committerFactory: CommitterFactory,
                               override val router: RouterRef,
                               metricsLogger: MetricsLogger)
        extends CommitterActor(topic, partition, castleConfig, committerConfig, committerFactory, router, metricsLogger) {

        case class ScheduledMessages(duration: FiniteDuration, message: Any)

        var scheduledMessageQueue: IndexedSeq[(FiniteDuration, Any)] = IndexedSeq.empty
        var processedMessageQueue: IndexedSeq[(FiniteDuration, Any)] = IndexedSeq.empty
        val schedulerLock = new Object()

        override def scheduleOnce(duration: FiniteDuration, message: Any): Unit =
          schedulerLock.synchronized {
            scheduledMessageQueue = (scheduledMessageQueue :+ ((duration, message))).sortBy(_._1)
          }

        def processScheduledMessageQueue(): Unit = schedulerLock.synchronized {
          val (delay, message) = scheduledMessageQueue.head
          scheduledMessageQueue = scheduledMessageQueue.tail
          processedMessageQueue = processedMessageQueue :+ ((delay, message))
          self ! message
        }
      }

      class MockCommitterActorFactory(castleConfig: CastleConfig,
                                      committerFactoryMap: Map[String, CommitterFactory],
                                      metricsLogger: MetricsLogger)
        extends CommitterActorFactory(castleConfig, committerFactoryMap, metricsLogger) {

        override def props(topic: String, partition: Int, committerConfig: CommitterConfig, router: RouterRef): Props = {
          committerFactoryMap.get(committerConfig.id) match {
            case Some(committerFactory) =>
              Props(new MockCommitterActor(topic, partition, castleConfig, committerConfig, committerFactory, router, metricsLogger))
            case None =>
              throw new CastleFatalException(s"No committer factory found for committer id: ${committerConfig.id}")
          }
        }
      }

      /*
       * Definitions of the mock Committer and the mock CommitterFactory for this test
       */

      object RateLimitedCommitter {
        var commitCounter: Map[Int, Int] = Map.empty.withDefaultValue(0)
        var messagesInCommitCall: IndexedSeq[(Int, IndexedSeq[Message])] = IndexedSeq.empty
        private val rateLimitedCommitterLock = new Object()
      }

      class RateLimitedCommitter(id: Int) extends Committer {

        import RateLimitedCommitter._

        override def commit(messageBatch: IndexedSeq[Message], metadata: Option[String] = None): CommitResult = {
          rateLimitedCommitterLock.synchronized {
            commitCounter = commitCounter + ((id -> (commitCounter(id) + 1)))
            messagesInCommitCall = messagesInCommitCall :+ ((id, messageBatch))
          }
          /*
           * Committer with id 1 will respond with retry with delay of 200ms
           * Committer with id 2 will respond with retry with delay of 100ms
           * the first time, and delay of 50ms the second time
           */
          if (id == 0) {
            RetryBatch(FiniteDuration(200, TimeUnit.MILLISECONDS), messageBatch)
          } else {
            if (commitCounter(id) == 1) {
              RetryBatch(FiniteDuration(100, TimeUnit.MILLISECONDS), messageBatch.drop(1))
            } else
              RetryBatch(FiniteDuration(50, TimeUnit.MILLISECONDS), messageBatch.drop(1))
          }
        }
      }

      class MockCommitterFactory extends CommitterFactory {
        override def create(topic: String, partition: Int, id: Int): Committer = new RateLimitedCommitter(id)
      }

      /*
       * Instantiation of mock CommitterActorFactory and mock CommitterFactory
       */

      val mockCommitterFactory = new MockCommitterFactory()

      val castleConfig = mock[CastleConfig]
      castleConfig.namespace returns mockNamespace

      val mockCommitterActorFactory = new MockCommitterActorFactory(
        castleConfig,
        Map(committerId -> mockCommitterFactory),
        mockMetricsLogger)

      val committerConfig = createCommitterConfig(id = committerId, initialOffset = InitialOffset.oldest)

      val (mockBoxSimpleConsumer, committerActor) =
        createActorRef(mockFetchData, mockMetricsLogger,
          parallelismFactor = 2,
          mockCommitterFactory = Some(mockCommitterFactory),
          mockCommitterActorFactory = Some(mockCommitterActorFactory))

      val mockCommitterActor = committerActor.underlyingActor.asInstanceOf[MockCommitterActor]

      // We should have two scheduled messages
      mustEventuallyBeTrue({
        mockCommitterActor.scheduledMessageQueue.size == 2
      })

      committerActor.underlyingActor.asInstanceOf[MockCommitterActor].processScheduledMessageQueue()

      /*
       * We process one message which causes another message to get scheduled, so we expect 2 scheduled messages
       * and 1 processed message
       */
      mustEventuallyBeTrue({
        mockCommitterActor.scheduledMessageQueue.size == 2 &&
          mockCommitterActor.processedMessageQueue.size == 1
      })

      // Assert the retries are in the correct order
      mockCommitterActor.scheduledMessageQueue(0)._1 must_== FiniteDuration(50, TimeUnit.MILLISECONDS)
      mockCommitterActor.scheduledMessageQueue(1)._1 must_== FiniteDuration(200, TimeUnit.MILLISECONDS)
      mockCommitterActor.processedMessageQueue(0)._1 must_== FiniteDuration(100, TimeUnit.MILLISECONDS)

      val committers = Await.result(mockCommitterActor.userCommittersFuture, timeout)
      committers.size must_== 2

      // There should have been 3 commit calls in all
      mustEventuallyBeTrue({
        RateLimitedCommitter.messagesInCommitCall.size == 3
      })

      // Assert the calls to commit() were made with the correct messages
      val firstCommitCall = RateLimitedCommitter.messagesInCommitCall(0)
      val secondCommitCall = RateLimitedCommitter.messagesInCommitCall(1)
      val thirdCommitCall = RateLimitedCommitter.messagesInCommitCall(2)

      // first commit call could have been either to committerId 0 or committerId 1. We do not control this
      IndexedSeq(firstCommitCall, secondCommitCall).sortBy(_._1) must_==
        IndexedSeq(
          (0, allMessages.take(4).toIndexedSeq),
          (1, allMessages.drop(4).toIndexedSeq))

      thirdCommitCall must_== ((1, allMessages.drop(5)))
    }
  }

  "CommitterActor" should {
    "not trigger usercommitter heartbeat on the main thread" in new actorSystemAndCurator {
      val firstMessageSet: MessageSet = makeMockMessageSet(List())
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

      val secondMessageSet: MessageSet = makeMockMessageSet(List())
      val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

      val thirdMessageSet: MessageSet = makeMockMessageSet(List())
      val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
          2 -> Map(topicAndPartition -> Map(offset -> secondPartitionData)),
          3 -> Map(topicAndPartition -> Map(offset -> thirdPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      class TestHeartbeatCommmitter(override val id: Int) extends MockCommitter(false, false, id) {
        override val mockSleepDuringHeartbeat = 0

        val lock = new Object()

        var testCounter = 0

        var committerBlock = new Object()
        var waitForCommitterBlockSignal = true
        var hbCount = new AtomicInteger(0)

        override def heartbeat(metadata: Option[String]): Option[String] = {
          testCounter += 1
          committerBlock.synchronized {
            committerBlock.wait(600000)
          }
          hbCount.getAndIncrement()
          metadata
        }
      }

      val mockCommitterFactory = new CommitterFactory {
        override def create(topic: String, partition: Int, id: Int): Committer = new TestHeartbeatCommmitter(id)
      }

      val castleConfig = mock[CastleConfig]
      castleConfig.namespace returns mockNamespace

      val committerConfig = createCommitterConfig(id = committerId, initialOffset = InitialOffset.oldest,
        parallelismFactor = 1, heartbeatCadenceInMillis = Some(new Duration(TESTHEARTBEATCADENCEINMILLIS)))

      val (mockBoxSimpleConsumer, committerActor) =
        createActorRef(mockFetchData, mockMetricsLogger,
          parallelismFactor = 1,
          mockCommitterFactory = Some(mockCommitterFactory),
          mockCommitterConfig = Some(committerConfig))


      val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

      committerActor.underlyingActor.performHeartbeat(mockCommitters, OffsetAndMetadata(offset, Some("metadata")))



      // the fact that the below assertions are executed means heartbeat was not
      //   done synchronously
      mockCommitters(0).asInstanceOf[TestHeartbeatCommmitter].hbCount.get() must_== 0

      mockCommitters(0).asInstanceOf[TestHeartbeatCommmitter].committerBlock.synchronized {
        mockCommitters(0).asInstanceOf[TestHeartbeatCommmitter].committerBlock.notify()
      }

      mustEventuallyBeTrue({
        mockCommitters(0).asInstanceOf[TestHeartbeatCommmitter].hbCount.get() == 1
      })
    }
  }

  "throw exception when heartbeat is enabled and parallism factor larger than 1" in new actorSystemAndCurator {
    val firstMessageSet: MessageSet = makeMockMessageSet(List())
    val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

    val secondMessageSet: MessageSet = makeMockMessageSet(List())
    val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

    val thirdMessageSet: MessageSet = makeMockMessageSet(List())
    val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

    val mockFetchData =
      Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
        2 -> Map(topicAndPartition -> Map(offset -> secondPartitionData)),
        3 -> Map(topicAndPartition -> Map(offset -> thirdPartitionData)))

    val mockMetricsLogger = new MockMetricsLogger()

    class TestHeartbeatCommmitter(override val id: Int) extends MockCommitter(false, false, id) {
      override val mockSleepDuringHeartbeat = 0

      override def heartbeat(metadata: Option[String]): Option[String] = {
        metadata
      }
    }

    val mockCommitterFactory = new CommitterFactory {
      override def create(topic: String, partition: Int, id: Int): Committer = new TestHeartbeatCommmitter(id)
    }

    val castleConfig = mock[CastleConfig]
    castleConfig.namespace returns mockNamespace

    val committerConfig = createCommitterConfig(id = committerId, initialOffset = InitialOffset.oldest,
      parallelismFactor = 2, heartbeatCadenceInMillis = Some(new Duration(TESTHEARTBEATCADENCEINMILLIS)))

    val (mockBoxSimpleConsumer, committerActor) =
      createActorRef(mockFetchData, mockMetricsLogger,
        parallelismFactor = 2,
        mockCommitterFactory = Some(mockCommitterFactory),
        mockCommitterConfig = Some(committerConfig))


    val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

    committerActor.underlyingActor.performHeartbeat(mockCommitters, OffsetAndMetadata(offset, Some("metadata"))) must throwA[UnrecoverableCommitterActorException]
  }

  "CommitterActor" should {
    "when in idling state, compute the correct delay and next message" in new actorSystemAndCurator {

      // I. nextHeartbeattime comes before nextFetchTime but after currentTime
      val currentTime = new DateTime(2016, 7, 25, 8, 0, 0)

      val earlyLastHeartbeatTime = new DateTime(2016, 7, 25, 7, 59, 58)
      val lateNextFetchTime = new DateTime(2016, 7, 25, 8, 20, 0)


      val (delayDuration, idlingMessage) =
        Idling.nextMessageAndDelay(currentTime, earlyLastHeartbeatTime,
          new Duration(TESTHEARTBEATCADENCEINMILLIS), lateNextFetchTime, OffsetAndMetadata(offset, None))

      delayDuration.toMillis must_== 1000

      idlingMessage must_== PerformHeartbeat(OffsetAndMetadata(23842, None))

      // II. nextHeartbeattime comes before currentTime but after nextFetchTime
      val earlyLastHeartbeatTimeTwo = new DateTime(2016, 7, 25, 7, 50, 0)

      val (delayDurationTwo, idlingMessageTwo) =
        Idling.nextMessageAndDelay(currentTime, earlyLastHeartbeatTimeTwo,
          new Duration(TESTHEARTBEATCADENCEINMILLIS), lateNextFetchTime, OffsetAndMetadata(offset, None))

      delayDurationTwo.toMillis must_== 0

      idlingMessageTwo must_== PerformHeartbeat(OffsetAndMetadata(23842, None))

      // III. nextFetchTime comes before nextHeartbeatTime and after currentTime
      val earlyNextFetchTime = new DateTime(2016, 7, 25, 8, 0, 1)
      val lateLastHeartbeatTime = currentTime


      val (delayDurationThree, idlingMessageThree) =
        Idling.nextMessageAndDelay(currentTime, lateLastHeartbeatTime,
          new Duration(TESTHEARTBEATCADENCEINMILLIS), earlyNextFetchTime, OffsetAndMetadata(offset, None))

      delayDurationThree.toMillis must_== 1000
      idlingMessageThree must_== FetchDataAfterDelay(OffsetAndMetadata(23842, None))

      // IV. nextFetchTime comes before currentTime but after nextHeartbeatTime
      val earlyNextFetchTimeTwo = new DateTime(2016, 7, 25, 7, 50, 0)
      val lateLastHeartbeatTimeTwo = new DateTime(2016, 7, 25, 7, 52, 0)


      val (delayDurationFour, idlingMessageFour) =
        Idling.nextMessageAndDelay(currentTime, lateLastHeartbeatTimeTwo,
          new Duration(TESTHEARTBEATCADENCEINMILLIS), earlyNextFetchTimeTwo, OffsetAndMetadata(offset, None))


      delayDurationFour.toMillis must_== 0
      idlingMessageFour must_== FetchDataAfterDelay(OffsetAndMetadata(23842, None))
    }
  }

  "CommitterActor" should {
    "complete heartbeat and schedule next iteration regardless when heartbeat is successful" in new actorSystemAndCurator {

      // Initialize test messages
      val firstMessageSet: MessageSet = makeMockMessageSet(List())
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

      val secondMessageSet: MessageSet = makeMockMessageSet(List())
      val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

      val thirdMessageSet: MessageSet = makeMockMessageSet(List())
      val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
          2 -> Map(topicAndPartition -> Map(offset -> secondPartitionData)),
          3 -> Map(topicAndPartition -> Map(offset -> thirdPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      class MockCommitterActor(topic: String,
                               partition: Int,
                               castleConfig: CastleConfig,
                               override val committerConfig: CommitterConfig,
                               committerFactory: CommitterFactory,
                               override val router: RouterRef,
                               metricsLogger: MetricsLogger)
        extends CommitterActor(topic, partition, castleConfig, committerConfig, committerFactory, router, metricsLogger) {

        override def generateFetchDelay(): Duration = new Duration(TESTFETCHDELAYINMILLIS)

        var scheduledNextIterationFlag = false
        var offsetAndMetadata: OffsetAndMetadata = null

        context.become(idling)


        override def scheduleNextIdleIteration(offsetAndMetadata: OffsetAndMetadata): Unit = {
          scheduledNextIterationFlag = true
          this.offsetAndMetadata = offsetAndMetadata
          super.scheduleNextIdleIteration(offsetAndMetadata)
        }
      }

      class MockCommitterActorFactory(castleConfig: CastleConfig,
                                      committerFactoryMap: Map[String, CommitterFactory],
                                      metricsLogger: MetricsLogger)
        extends CommitterActorFactory(castleConfig, committerFactoryMap, metricsLogger) {

        override def props(topic: String, partition: Int, committerConfig: CommitterConfig, router: RouterRef): Props = {
          committerFactoryMap.get(committerConfig.id) match {
            case Some(committerFactory) =>
              Props(new MockCommitterActor(topic, partition, castleConfig, committerConfig, committerFactory, router, metricsLogger))
            case None =>
              throw new CastleFatalException(s"No committer factory found for committer id: ${committerConfig.id}")
          }
        }
      }

      class TestHeartbeatCommmitter(override val id: Int) extends MockCommitter(false, false, id) {
        override val mockSleepDuringHeartbeat = 0
        var hbCount = new AtomicInteger(0)

        override def heartbeat(metadata: Option[String] = None): Option[String] = {
          hbCount.getAndIncrement()
          metadata
        }
      }

      val mockCommitterFactory = new CommitterFactory {
        override def create(topic: String, partition: Int, id: Int): Committer = new TestHeartbeatCommmitter(id)
      }

      val castleConfig = mock[CastleConfig]
      castleConfig.namespace returns mockNamespace

      val mockCommitterActorFactory = new MockCommitterActorFactory(
        castleConfig,
        Map(committerId -> mockCommitterFactory),
        mockMetricsLogger)

      val committerConfig = createCommitterConfig(id = committerId, initialOffset = InitialOffset.oldest,
        heartbeatCadenceInMillis = Some(new Duration(TESTHEARTBEATCADENCEINMILLIS)))

      val (_, committerActor) =
        createActorRef(mockFetchData, mockMetricsLogger,
          parallelismFactor = 1,
          mockCommitterFactory = Some(mockCommitterFactory),
          mockCommitterActorFactory = Some(mockCommitterActorFactory),
          mockCommitterConfig = Some(committerConfig))

      val mockCommitterActor = committerActor.underlyingActor.asInstanceOf[MockCommitterActor]

      val mockCommitters = Await.result(mockCommitterActor.userCommittersFuture, timeout)

      mockCommitterActor.performHeartbeat(mockCommitters, OffsetAndMetadata(offset, Some("metadata")))

      mustEventuallyBeTrue({
        mockCommitterActor.scheduledNextIterationFlag == true
      }, FiniteDuration(15, TimeUnit.SECONDS))

      mockCommitterActor.offsetAndMetadata.metadata must beSome("metadata")
    }
  }

  "CommitterActor" should {
    "complete heartbeat and schedule next iteration despite heartbeat failure" in new actorSystemAndCurator {

      // Initialize test messages
      val firstMessageSet: MessageSet = makeMockMessageSet(List())
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)

      val secondMessageSet: MessageSet = makeMockMessageSet(List())
      val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

      val thirdMessageSet: MessageSet = makeMockMessageSet(List())
      val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
          2 -> Map(topicAndPartition -> Map(offset -> secondPartitionData)),
          3 -> Map(topicAndPartition -> Map(offset -> thirdPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()

      class MockCommitterActor(topic: String,
                               partition: Int,
                               castleConfig: CastleConfig,
                               override val committerConfig: CommitterConfig,
                               committerFactory: CommitterFactory,
                               override val router: RouterRef,
                               metricsLogger: MetricsLogger)
        extends CommitterActor(topic, partition, castleConfig, committerConfig, committerFactory, router, metricsLogger) {

        override def generateFetchDelay(): Duration = new Duration(TESTFETCHDELAYINMILLIS)

        context.become(idling)

        var scheduledNextIterationFlag = false
        var offsetAndMetadata: OffsetAndMetadata = null

        override def scheduleNextIdleIteration(offsetAndMetadata: OffsetAndMetadata): Unit = {
          scheduledNextIterationFlag = true
          this.offsetAndMetadata = offsetAndMetadata
          super.scheduleNextIdleIteration(offsetAndMetadata)
        }
      }

      class MockCommitterActorFactory(castleConfig: CastleConfig,
                                      committerFactoryMap: Map[String, CommitterFactory],
                                      metricsLogger: MetricsLogger)
        extends CommitterActorFactory(castleConfig, committerFactoryMap, metricsLogger) {

        override def props(topic: String, partition: Int, committerConfig: CommitterConfig, router: RouterRef): Props = {
          committerFactoryMap.get(committerConfig.id) match {
            case Some(committerFactory) =>
              Props(new MockCommitterActor(topic, partition, castleConfig, committerConfig, committerFactory, router, metricsLogger))
            case None =>
              throw new CastleFatalException(s"No committer factory found for committer id: ${committerConfig.id}")
          }
        }
      }

      class TestHeartbeatCommmitter(override val id: Int) extends MockCommitter(false, false, id) {
        override val mockSleepDuringHeartbeat = 0
        var hbCount = new AtomicInteger(0)

        override def heartbeat(metadata: Option[String] = None): Option[String] = {
          throw new Exception("Failed Heartbeat")
        }
      }

      val mockCommitterFactory = new CommitterFactory {
        override def create(topic: String, partition: Int, id: Int): Committer = new TestHeartbeatCommmitter(id)
      }

      val castleConfig = mock[CastleConfig]
      castleConfig.namespace returns mockNamespace

      val mockCommitterActorFactory = new MockCommitterActorFactory(
        castleConfig,
        Map(committerId -> mockCommitterFactory),
        mockMetricsLogger)

      val committerConfig = createCommitterConfig(id = committerId, initialOffset = InitialOffset.oldest,
        heartbeatCadenceInMillis = Some(new Duration(TESTHEARTBEATCADENCEINMILLIS)))

      val (_, committerActorFailure) =
        createActorRef(mockFetchData, mockMetricsLogger,
          parallelismFactor = 1,
          mockCommitterFactory = Some(mockCommitterFactory),
          mockCommitterActorFactory = Some(mockCommitterActorFactory),
          mockCommitterConfig = Some(committerConfig))

      val mockCommitterActorFailure = committerActorFailure.underlyingActor.asInstanceOf[MockCommitterActor]

      val mockCommittersFailure = Await.result(mockCommitterActorFailure.userCommittersFuture, timeout)

      mockCommitterActorFailure.performHeartbeat(mockCommittersFailure, OffsetAndMetadata(offset, Some("some_other_metadata")))

      mustEventuallyBeTrue({
        mockCommitterActorFailure.scheduledNextIterationFlag == true
      }, FiniteDuration(15, TimeUnit.SECONDS))

      mockCommitterActorFailure.offsetAndMetadata.metadata must beSome("some_other_metadata")
    }
  }


  "CommitterActor" should {
    "handle committing message with metadata" in new actorSystemAndCurator {

      val metadataDelimiter = ","
      val baseMetadata = "metadata"

      class MockCommitterWithMetadata(val id: Int) extends Committer {
        var committedMessageBatches = Queue.empty[IndexedSeq[Message]]
        var offsetMetadata: Option[String] = None


        //can the user committer.commit got metadata, do something to it and return the new metadata
        def generateMetadata(metadata: Option[String]): Option[String] = {
          metadata match {
            case Some(data) => Some(data + metadataDelimiter + data.split(metadataDelimiter)(0))
            case None => Some(baseMetadata)
          }
        }

        override def commit(messageBatch: IndexedSeq[Message], metadata: Option[String] = None): CommitResult = {
          committedMessageBatches = committedMessageBatches.enqueue(messageBatch)
          //Everytime commit is called, we generate new metadata based on the data we received
          offsetMetadata = generateMetadata(metadata)
          BatchCommittedSuccessfully(offsetMetadata)
        }

        override def commitAsync(messageBatch: IndexedSeq[Message], metadata: Option[String] = None)(implicit execctx: ExecutionContext): Future[CommitResult] = {
          Future(commit(messageBatch, metadata))(execctx)
        }

        override def close(): Unit = {}

        override def heartbeat(metadata: Option[String] = None): Option[String] = {
          metadata
        }
      }

      val mockCommitterWithMetadataFactory = new CommitterFactory {
        override def create(topic: String, partition: Int, id: Int): Committer = new MockCommitterWithMetadata(id)
      }


      val a = makeMockMessageAndOffset(offset + 0, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)

      val firstMessageSet: MessageSet = makeMockMessageSet(List(a, b, c))
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)


      val d = makeMockMessageAndOffset(offset + 3, offset + 4, 235235)
      val e = makeMockMessageAndOffset(offset + 4, offset + 5, 8334)
      val f = makeMockMessageAndOffset(offset + 5, offset + 6, 43)

      val secondMessageSet = makeMockMessageSet(List(d, e, f))
      val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

      val g = makeMockMessageAndOffset(offset + 6, offset + 7, 822222)
      val h = makeMockMessageAndOffset(offset + 7, offset + 8, 111000)
      val i = makeMockMessageAndOffset(offset + 8, offset + 9, 601111)

      val thirdMessageSet = makeMockMessageSet(List(g, h, i))
      val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
          2 -> Map(topicAndPartition -> Map(offset + 3 -> secondPartitionData)),
          3 -> Map(topicAndPartition -> Map(offset + 6 -> thirdPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()
      val (mockBoxSimpleConsumer, committerActor) = createActorRef(mockFetchData, mockMetricsLogger,
        mockCommitterFactory = Some(mockCommitterWithMetadataFactory))

      val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

      // By default there should be only one mockCommitter
      mockCommitters.size must_== 1

      val mockCommitter = mockCommitters.head

      mustEventuallyBeTrue({
        mockCommitter.asInstanceOf[MockCommitterWithMetadata].committedMessageBatches ==
          Queue(firstMessageSet.toIndexedSeq.map(_.message), secondMessageSet.toIndexedSeq.map(_.message), thirdMessageSet.toIndexedSeq.map(_.message))
      })

      //here we called commit three times, so we are expecting
      mustEventuallyBeTrue({
        mockCommitter.asInstanceOf[MockCommitterWithMetadata].offsetMetadata == Some((1 to 3).map(_ => baseMetadata).mkString(metadataDelimiter))
      })

      mustEventuallyBeTrue({
        println(s"NumMessagesCommitted ---> ${mockMetricsLogger.getCountFor(const.Metrics.NumMessagesCommitted)}")
        System.out.println(s"Current mockBoxSimpleConsumer.committedConsumerOffsets=${mockBoxSimpleConsumer.committedConsumerOffsets}")
        // Queue(
        // (mock_namespace_my_mock_committer_id,[preview,1],OffsetAndMetadata[23845,V1|CONSUMER_METADATAmetadata,0]),
        // (mock_namespace_my_mock_committer_id,[preview,1],OffsetAndMetadata[23851,V1|CONSUMER_METADATAmetadata,metadata,metadata,0]))

        // expected: Current mockBoxSimpleConsumer.committedConsumerOffsets=
        // Queue(
        // (mock_namespace_my_mock_committer_id,[preview,1],OffsetAndMetadata[23845,V1|CONSUMER_METADATAmetadata,0]),
        // (mock_namespace_my_mock_committer_id,[preview,1],OffsetAndMetadata[23848,V1|CONSUMER_METADATAmetadata,metadata,0]),
        // (mock_namespace_my_mock_committer_id,[preview,1],OffsetAndMetadata[23851,V1|CONSUMER_METADATAmetadata,metadata,metadata,0]))
        mockBoxSimpleConsumer.committedConsumerOffsets ==
          Queue((consumerId, topicAndPartition, OffsetMetadataAndError(offset + 3, OffsetAndMetadata.ConsumerMetadataPrefix + baseMetadata)),
                (consumerId, topicAndPartition, OffsetMetadataAndError(offset + 6, OffsetAndMetadata.ConsumerMetadataPrefix + (1 to 2).map(_ => baseMetadata).mkString(metadataDelimiter))),
                (consumerId, topicAndPartition, OffsetMetadataAndError(offset + 9, OffsetAndMetadata.ConsumerMetadataPrefix + (1 to 3).map(_ => baseMetadata).mkString(metadataDelimiter))))
      }, 120, TimeUnit.SECONDS)
    }
  }

  "CommitterActor" should {
    "handle partial committed message with metadata" in new actorSystemAndCurator {

      val metadataDelimiter = ","
      val baseMetadata = "metadata"
      val half = "half"

      class MockCommitterWithMetadata(val id: Int) extends Committer {
        var offsetMetadata: Option[String] = None


        //can the user committer.commit got metadata, do something to it and return the new metadata
        def generateMetadata(metadata: Option[String]): Option[String] = {
          metadata match {
            case Some(data) => Some(data + metadataDelimiter + data.split(metadataDelimiter)(0))
            case None => Some(baseMetadata)
          }
        }

        override def commit(messageBatch: IndexedSeq[Message], metadata: Option[String] = None): CommitResult = {
          if (messageBatch.size == 3) {
            //first commit one message
            val (first, second) = messageBatch.splitAt(1)
            offsetMetadata = metadata match {
              case None => Some(half)
              case Some(data) => Some(data + metadataDelimiter + half)
            }
            RetryBatch(0.seconds, second, offsetMetadata)
          } else {
            //next time we should see the rest of the message and we append the metadata to it
            offsetMetadata = metadata match {
              case Some(data) => Some(data + baseMetadata)
              case None => None // should never happen in this test case
            }
            BatchCommittedSuccessfully(offsetMetadata)
          }
        }

        override def commitAsync(messageBatch: IndexedSeq[Message], metadata: Option[String] = None)(implicit execctx: ExecutionContext): Future[CommitResult] = {
          Future(commit(messageBatch, metadata))(execctx)
        }

        override def close(): Unit = {}

        override def heartbeat(metadata: Option[String] = None): Option[String] = {
          metadata
        }
      }

      val mockCommitterWithMetadataFactory = new CommitterFactory {
        override def create(topic: String, partition: Int, id: Int): Committer = new MockCommitterWithMetadata(id)
      }


      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)

      val firstMessageSet: MessageSet = makeMockMessageSet(List(a, b, c))
      val firstPartitionData = FetchResponsePartitionData(messages = firstMessageSet)


      val d = makeMockMessageAndOffset(offset + 3, offset + 4, 235235)
      val e = makeMockMessageAndOffset(offset + 4, offset + 5, 8334)
      val f = makeMockMessageAndOffset(offset + 5, offset + 6, 43)

      val secondMessageSet = makeMockMessageSet(List(d, e, f))
      val secondPartitionData = FetchResponsePartitionData(messages = secondMessageSet)

      val g = makeMockMessageAndOffset(offset + 6, offset + 7, 822222)
      val h = makeMockMessageAndOffset(offset + 7, offset + 8, 111000)
      val i = makeMockMessageAndOffset(offset + 8, offset + 9, 601111)

      val thirdMessageSet = makeMockMessageSet(List(g, h, i))
      val thirdPartitionData = FetchResponsePartitionData(messages = thirdMessageSet)

      val mockFetchData =
        Map(1 -> Map(topicAndPartition -> Map(offset -> firstPartitionData)),
          2 -> Map(topicAndPartition -> Map(offset + 3 -> secondPartitionData)),
          3 -> Map(topicAndPartition -> Map(offset + 6 -> thirdPartitionData)))

      val mockMetricsLogger = new MockMetricsLogger()
      val (mockBoxSimpleConsumer, committerActor) = createActorRef(mockFetchData, mockMetricsLogger,
        mockCommitterFactory = Some(mockCommitterWithMetadataFactory))

      val mockCommitters = Await.result(committerActor.underlyingActor.userCommittersFuture, timeout)

      // By default there should be only one mockCommitter
      mockCommitters.size must_== 1

      val mockCommitter = mockCommitters.head

      //here we called commit three times, so we are expecting three metadata string separated by comma
      mustEventuallyBeTrue({
        mockCommitter.asInstanceOf[MockCommitterWithMetadata].offsetMetadata == Some((1 to 3).map(_ => half + baseMetadata).mkString(metadataDelimiter))
      })

      mustEventuallyBeTrue({
        mockBoxSimpleConsumer.committedConsumerOffsets ==
          Queue((consumerId, topicAndPartition, OffsetMetadataAndError(offset + 3, OffsetAndMetadata.ConsumerMetadataPrefix + half + baseMetadata)),
            (consumerId, topicAndPartition, OffsetMetadataAndError(offset + 6, OffsetAndMetadata.ConsumerMetadataPrefix + (1 to 2).map(_ => half + baseMetadata).mkString(metadataDelimiter))),
            (consumerId, topicAndPartition, OffsetMetadataAndError(offset + 9, OffsetAndMetadata.ConsumerMetadataPrefix + (1 to 3).map(_ => half + baseMetadata).mkString(metadataDelimiter))))
      })
    }
  }
}

