package com.box.castle.core.committer.manager

import com.box.castle.core.mock.MockTools
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification


//class MockCommitterActor(val offset: Long, val committerId: String,
//                         val raiseException: Boolean,
//                         val executePreStart: Boolean) extends Actor with Logging {
//
//  val fetcher = context.actorSelection("..")
//
//  override def preStart() = {
//    if (executePreStart) {
//      fetcher ! FetchData(offset, committerId)
//    }
//  }
//
//  override def receive: Receive = {
//    case _ => {
//      if (raiseException)
//        throw new CommitterActorException(committerId, "mock unrecoverable exception", new Exception("cause"))
//      log.info("MockCommitterActor got a message")
//    }
//  }
//}

//class MockCommitterActorFactory(val offset: Long, val committerId: String, val raiseException: Boolean = false,
//                                val executePreStart: Boolean = false)
//    extends CommitterActorFactory(null, null, null) {
//
//  var commiterConfig: CommitterConfig = null
//  var numCallsToProps = 0
//  var callsToProps: Queue[(String, Int, String)] = Queue.empty[(String, Int, String)]
//  override def props(topic: String, partition: Int, committerConfig: CommitterConfig): Props = {
//    callsToProps = callsToProps.enqueue((topic, partition, committerConfig.id))
//    numCallsToProps += 1
//    commiterConfig = committerConfig
//    Props(new MockCommitterActor(offset, committerId, raiseException, executePreStart))
//  }
//}



class CommitterManagerActorTest extends Specification with Mockito with MockTools {

//  class env(system: ActorSystem) extends TestKit(system) with ImplicitSender {
//
//    class MockFetcherFactory(val mockFetcher: Fetcher) extends FetcherFactory(null, null, null) {
//      override def create(fetcherActor: ActorRef, topic: String, partition: Int) = {
//        mockFetcher
//      }
//    }
//
//    def checkFetchData() = {
//      val topic = "performance"
//      val partition = 5
//
//      val MOCK_OFFSET = 837
//      val MOCK_COMMITTER_ID = "mockCommitterId_777"
//      val mockFetcher = mock[Fetcher]
//
//      val mockMessageBatch = mock[CastleMessageBatch]
//      mockMessageBatch.offset returns MOCK_OFFSET
//      mockMessageBatch.nextOffset returns MOCK_OFFSET + 1
//
//      mockFetcher.getMessageBatch(MOCK_OFFSET, MOCK_COMMITTER_ID) returns Future.successful(mockMessageBatch)
//
//      val mockCommitterActorFactory = new MockCommitterActorFactory(MOCK_OFFSET, MOCK_COMMITTER_ID)
//
//      val committerConfig = createCommitterConfig(InitialOffset.latest, MOCK_COMMITTER_ID)
//
//      val mockMetricsLogger = new MockMetricsLogger()
//      val mockFetcherFactory = new MockFetcherFactory(mockFetcher)
//      val fetcherActorFactory = new FetcherActorFactory(mockCommitterActorFactory, mockFetcherFactory, List(committerConfig),
//                                      new MockMetricsLogger())
//
//      val probe = TestProbe()(system)
//
//      val fetcherActor = TestActorRef(fetcherActorFactory.props(Task(topic, partition, Set(MOCK_COMMITTER_ID))),
//                                      probe.ref, "FetcherActor")(system)
//
//
//      fetcherActor ! FetchData(MOCK_OFFSET, MOCK_COMMITTER_ID)
//      expectMsg(ProcessData(mockMessageBatch))
//    }
//
//    def checkCommitterCreation() = {
//      val topic = "performance"
//      val partition = 5
//
//      val MOCK_OFFSET = 837
//      val MOCK_COMMITTER_ID = "mockCommitterId_777"
//      val mockFetcher = mock[Fetcher]
//
//      def fetcherFactoryProps(committerIds: Set[String]): (MockCommitterActorFactory, Props) = {
//        val mockMessageBatch = mock[CastleMessageBatch]
//        mockMessageBatch.offset returns MOCK_OFFSET
//        mockMessageBatch.nextOffset returns MOCK_OFFSET + 1
//
//        mockFetcher.getMessageBatch(MOCK_OFFSET, MOCK_COMMITTER_ID) returns Future.successful(mockMessageBatch)
//
//        val mockCommitterActorFactory_inner = new MockCommitterActorFactory(MOCK_OFFSET, MOCK_COMMITTER_ID)
//
//        val committerConfig = createCommitterConfig(InitialOffset.latest, MOCK_COMMITTER_ID)
//        val committerConfig2 = createCommitterConfig(InitialOffset.latest, "zzzz")
//
//        val mockMetricsLogger = new MockMetricsLogger()
//        val mockFetcherFactory = new MockFetcherFactory(mockFetcher)
//        val fetcherActorFactory = new FetcherActorFactory(mockCommitterActorFactory_inner, mockFetcherFactory,
//          List(committerConfig, committerConfig2),
//          new MockMetricsLogger())
//
//        (mockCommitterActorFactory_inner, fetcherActorFactory.props(Task(topic, partition, committerIds)))
//      }
//
//      val probe = TestProbe()(system)
//      val (mockCommitterActorFactory, fetcherFactory) = fetcherFactoryProps(Set(MOCK_COMMITTER_ID))
//      val fetcherActor = TestActorRef(fetcherFactory, probe.ref, "FetcherActor")(system)
//
//      // The committer factory is only asked to create the mock committer id because our Task only contained that in the set
//      mockCommitterActorFactory.callsToProps.toList must_== List((topic, partition, MOCK_COMMITTER_ID))
//
//
//      val probe2 = TestProbe()(system)
//      val (mockCommitterActorFactory2, fetcherFactory2) = fetcherFactoryProps(Set(MOCK_COMMITTER_ID, "zzzz"))
//      val fetcherActor2 = TestActorRef(fetcherFactory2, probe.ref, "FetcherActor2")(system)
//
//      // The committer factory is asked to create both here
//      mockCommitterActorFactory2.callsToProps.toList must_== List((topic, partition, MOCK_COMMITTER_ID), (topic, partition, "zzzz"))
//    }
//
//    def checkStopCommitter() = {
//      val topic = "performance"
//      val partition = 5
//
//      val MOCK_OFFSET = 837
//      val MOCK_COMMITTER_ID = "mockCommitterId_777"
//      val mockFetcher = mock[Fetcher]
//
//      val mockMessageBatch = mock[CastleMessageBatch]
//      mockMessageBatch.offset returns MOCK_OFFSET
//      mockMessageBatch.nextOffset returns MOCK_OFFSET + 1
//
//      mockFetcher.getMessageBatch(MOCK_OFFSET, MOCK_COMMITTER_ID) returns Future.successful(mockMessageBatch)
//
//      val mockCommitterActorFactory = new MockCommitterActorFactory(MOCK_OFFSET, MOCK_COMMITTER_ID,
//                                                                    raiseException = true, executePreStart = true)
//
//      val committerConfig = createCommitterConfig(InitialOffset.latest, MOCK_COMMITTER_ID)
//
//      val mockMetricsLogger = new MockMetricsLogger()
//      val probe = TestProbe()(system)
//
//
//      val mockFetcherFactory = new MockFetcherFactory(mockFetcher)
//      val fetcherActorFactory = new FetcherActorFactory(mockCommitterActorFactory, mockFetcherFactory, List(committerConfig),
//        mockMetricsLogger)
//
//
//
//      TestActorRef(fetcherActorFactory.props(Task(topic, partition, Set(MOCK_COMMITTER_ID))),
//                                            probe.ref, "FetcherActor")(system)
//
//      val num_committer_fatals = Await.result(mockMetricsLogger.getCountPromises("committer_fatal").future, Duration("5 seconds"))
//      num_committer_fatals must_== 1
//    }
//  }
//
//  "FetcherActor" should {
//    "start up child committer actors and instantiate their committers" in new actorSystem {
//      val topic = "performance"
//      val partition = 5
//
//      val MOCK_OFFSET = 837
//      val MOCK_COMMITTER_ID = "mockCommitterId_777"
//      val mockFetcher = mock[Fetcher]
//
//      val mockMessageBatch = mock[CastleMessageBatch]
//      mockMessageBatch.offset returns MOCK_OFFSET
//      mockMessageBatch.nextOffset returns MOCK_OFFSET + 1
//
//      mockFetcher.getMessageBatch(MOCK_OFFSET, MOCK_COMMITTER_ID) returns Future.successful(mockMessageBatch)
//
//      val mockCommitterActorFactory = new MockCommitterActorFactory(MOCK_OFFSET, MOCK_COMMITTER_ID)
//
//      val committerConfig = createCommitterConfig(InitialOffset.latest, MOCK_COMMITTER_ID)
//
//      val mockFetcherFactory = mock[FetcherFactory]
//      mockFetcherFactory.create(null, topic, partition) returns mockFetcher
//      val fetcherActorFactory = new FetcherActorFactory(mockCommitterActorFactory, mockFetcherFactory, List(committerConfig),
//        new MockMetricsLogger())
//
//      val probe = TestProbe()(system)
//
//      TestActorRef(fetcherActorFactory.props(Task(topic, partition, Set(MOCK_COMMITTER_ID))),
//                   probe.ref, "FetcherActor")(system)
//
//      mockCommitterActorFactory.numCallsToProps must_== 1
//      mockCommitterActorFactory.commiterConfig.id must_== MOCK_COMMITTER_ID
//    }
//  }
//
//  "FetcherActor" should {
//    "properly call back the sender of FetchData" in new actorSystem {
//      new env(system).checkFetchData()
//      ok
//    }
//
//    "should stop the committer when if there is an unrecoverable exception and update the metrics for committer_fatals" in new actorSystem {
//      new env(system).checkStopCommitter()
//      ok
//    }
//
//    "should create only the committers in its assigned task" in new actorSystem {
//      new env(system).checkCommitterCreation()
//      ok
//    }
//  }
}
