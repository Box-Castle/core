package com.box.castle.core.committer.manager

import com.box.castle.core.mock.MockTools
import kafka.message.MessageSet
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.duration.Duration

//class MockKafkaSource(val allMessageSets: IndexedSeq[MessageSet], val sourceId: String = "mockSource", val desiredFailures: Int = 0) extends DataSource {
//  @volatile var getMessageSetNumCalls = 0
//  @volatile var numFailures = 0
//
//  // Immutable so does not need any synchronization
//  val data = allMessageSets.map(messageSet => messageSet.iterator.next().offset -> messageSet).toMap
//
//  def getMessageSet(topic: String, partition: Int, startOffset: Long): MessageSet = {
//    getMessageSetNumCalls += 1
//
//    if (numFailures < desiredFailures) {
//      numFailures += 1
//      throw new Exception(s"Mock failure, sourceId = ${sourceId}")
//    }
//    Thread.sleep(math.max(10, scala.util.Random.nextInt(200)))
//    data(startOffset)
//  }
//}



class FetcherTest extends Specification with Mockito with MockTools  {
  val WAIT = Duration(30, "seconds")
  val NUM_CACHE_SLOTS = 4
  val NUM_COMMITTER_THREADS = 10

//  def createBasicFetcher(fetcherActor: ActorRef, desiredFailures: Int = 0, sourceId: String = "basicFetcheMockSource",
//                         metricsLogger: MetricsLogger = new MockMetricsLogger()) = {
//    val a = makeMockMessageAndOffset(50, 51, 1)
//    val b = makeMockMessageAndOffset(51, 52, 1)
//    val c = makeMockMessageAndOffset(52, 53, 1)
//
//    val d = makeMockMessageAndOffset(53, 54, 1)
//    val e = makeMockMessageAndOffset(54, 55, 1)
//    val f = makeMockMessageAndOffset(55, 56, 1)
//
//    val g = makeMockMessageAndOffset(52, 53, 1)
//
//    val m = makeMockMessageSet(List(a, b, c), 1)
//    val m2 = makeMockMessageSet(List(d, e, f), 1)
//    val m3 = makeMockMessageSet(List(g), 1)
//
//    val fetcherConfig = new FetcherConfig(NUM_COMMITTER_THREADS, NUM_CACHE_SLOTS, 0, OffsetOutOfRangePolicy.useLatestOffset,
//                                        OffsetOutOfRangePolicy.useLatestOffset)
//    val dataSource = new MockKafkaSource(Vector(m, m2, m3), sourceId, desiredFailures)
//    val factory = new FetcherFactory(fetcherConfig, dataSource, metricsLogger)
//    val fetcher = factory.create(fetcherActor, "test", 5)
//
//    getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 0
//    getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//    getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//    getCountFor(fetcher, Fetcher.FetchMetric) must_== 0
//    fetcher.cache.size must_== 1
//    fetcher
//  }
//
//  def getCountFor(f: Fetcher, name:String) = f.metricsLogger.asInstanceOf[MockMetricsLogger].getCountFor(name)
//
//  def numDsCalls(f: Fetcher) = f.ds.asInstanceOf[MockKafkaSource].getMessageSetNumCalls
//
//  def getAndCache(offset: Long, fetcher: Fetcher) = {
//    val messageBatchFuture = fetcher.getMessageBatch(offset, "testCommitter")
//    val messageBatch = Await.result(messageBatchFuture, WAIT)
//    fetcher.addToCache(offset, "testCommitter", messageBatchFuture)
//    (messageBatchFuture, messageBatch)
//  }
//
//  "Fetcher" should {
//    "evict old offsets from the cache and not add old offsets if they are supplied" in new actorSystem {
//      val probe = TestProbe()(system)
//      val fetcher = createBasicFetcher(probe.ref)
//
//      fetcher.addToCache(11, null, null)
//      fetcher.cache.size must_== 2
//
//      fetcher.addToCache(12, null, null)
//      fetcher.cache.size must_== 3
//
//      fetcher.addToCache(13, null, null)
//      fetcher.cache.size must_== 4
//
//      fetcher.addToCache(14, null, null)
//      fetcher.cache.size must_== 4
//
//      fetcher.cache.keys.toList must_== List(11, 12, 13, 14)
//
//      // Adding in newer offsets, will evict old ones
//
//      fetcher.addToCache(15, null, null)
//      fetcher.cache.size must_== 4
//      fetcher.cache.keys.toList must_== List(12, 13, 14, 15)
//
//      fetcher.addToCache(16, null, null)
//      fetcher.cache.size must_== 4
//      fetcher.cache.keys.toList must_== List(13, 14, 15, 16)
//
//      // Adding old offsets has no effect on the cache
//      fetcher.addToCache(7, null, null)
//      fetcher.cache.size must_== 4
//      fetcher.cache.keys.toList must_== List(13, 14, 15, 16)
//
//    }
//  }
//
//  "Fetcher" should {
//    "return a message batch" in new actorSystem {
//      val probe = TestProbe()(system)
//      val fetcher = createBasicFetcher(probe.ref)
//
//      val (messageBatchFuture, messageBatch) = getAndCache(50, fetcher)
//      probe.expectMsgPF()({ case CacheMessageBatchFuture(offset, "testCommitter", messageBatchFutureReceived) => ()
//        offset must_== 50
//        val messageBatchReceived = Await.result(messageBatchFutureReceived, WAIT)
//        messageBatchReceived.offset must_== 50
//        messageBatchReceived.size must_== 3
//        messageBatchReceived.nextOffset must_== 53
//      })
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      numDsCalls(fetcher) must_== 1
//      fetcher.cache.size must_== 2
//
//      messageBatch.offset must_== 50
//      messageBatch.size must_== 3
//      messageBatch.nextOffset must_== 53
//    }
//  }
//
//  "Fetcher" should {
//    "retry failures" in new actorSystem {
//      val probe = TestProbe()(system)
//      val DESIRED_FAILURES = 4
//      val fetcher = createBasicFetcher(probe.ref, DESIRED_FAILURES, sourceId="failure_retry")
//
//      val (messageBatchFuture, messageBatch) = getAndCache(50, fetcher)
//      probe.expectMsgPF()({ case CacheMessageBatchFuture(offset, "testCommitter", messageBatchFugureReceived) => ()
//        offset must_== 50
//        val messageBatchReceived = Await.result(messageBatchFugureReceived, WAIT)
//        messageBatchReceived.offset must_== 50
//        messageBatchReceived.size must_== 3
//        messageBatchReceived.nextOffset must_== 53
//      })
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      numDsCalls(fetcher) must_== 5
//      fetcher.cache.size must_== 2
//
//      // Count the number of retries
//      getCountFor(fetcher, Fetcher.RetryMetric) must_== 4
//
//      messageBatch.offset must_== 50
//      messageBatch.size must_== 3
//      messageBatch.nextOffset must_== 53
//
//      fetcher.ds.asInstanceOf[MockKafkaSource].numFailures must_== DESIRED_FAILURES
//
//    }
//  }
//
//  "Fetcher" should {
//    "hit the cache when requesting an offset directly in the cache" in new actorSystem {
//      val probe = TestProbe()(system)
//      val fetcher = createBasicFetcher(probe.ref)
//
//      val (messageBatchFuture, messageBatch) = getAndCache(50, fetcher)
//
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      probe.expectMsgPF()({ case CacheMessageBatchFuture(offset, "testCommitter", messageBatchFugureReceived) => ()
//        offset must_== 50
//        val messageBatchReceived = Await.result(messageBatchFugureReceived, WAIT)
//        messageBatchReceived.offset must_== 50
//        messageBatchReceived.size must_== 3
//        messageBatchReceived.nextOffset must_== 53
//      })
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      numDsCalls(fetcher) must_== 1
//      fetcher.cache.size must_== 2
//
//      Await.result(fetcher.getMessageBatch(50, "testCommitter"), WAIT)
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      numDsCalls(fetcher) must_== 1
//      fetcher.cache.size must_== 2
//    }
//  }
//
//  "Fetcher" should {
//    "hit the cache when requesting an offset in the latest batch itself" in new actorSystem {
//      val probe = TestProbe()(system)
//      val fetcher = createBasicFetcher(probe.ref)
//
//      val (messageBatchFuture, messageBatch) = getAndCache(50, fetcher)
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      numDsCalls(fetcher) must_== 1
//      fetcher.cache.size must_== 2
//
//      // We can't get the associated batch directly, it will be extracted
//      // from the cache as a subsequence
//      fetcher.cache.get(52) must_== None
//
//      val (messageBatchFuture2, messageBatch2) = getAndCache(52, fetcher)
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 2
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 1
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      numDsCalls(fetcher) must_== 1
//      fetcher.cache.size must_== 3
//
//      messageBatch2.offset must_== 52
//      messageBatch2.size must_== 1
//      messageBatch2.nextOffset must_== 53
//    }
//  }
//
//  "Fetcher" should {
//    "hit the cache when requesting an offset in the latest COMPLETED batch itself" in new actorSystem {
//      val probe = TestProbe()(system)
//      val fetcher = createBasicFetcher(probe.ref)
//
//      val (messageBatchFuture, messageBatch) = getAndCache(50, fetcher)
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      numDsCalls(fetcher) must_== 1
//      fetcher.cache.size must_== 2
//
//      // We can't get the associated batch directly, it will be extracted
//      // from the cache as a sub sequence
//      fetcher.cache.get(52) must_== None
//
//      val (messageBatchFuture2, messageBatch2) = getAndCache(52, fetcher)
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 2
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 1
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      numDsCalls(fetcher) must_== 1
//      fetcher.cache.size must_== 3
//
//      messageBatch2.offset must_== 52
//      messageBatch2.size must_== 1
//      messageBatch2.nextOffset must_== 53
//    }
//  }
//
//  "Fetcher" should {
//    "fetch the latest offset if it is not in the latest COMPLETED batch" in new actorSystem {
//      val probe = TestProbe()(system)
//      val fetcher = createBasicFetcher(probe.ref)
//
//      val messageBatchFuture = fetcher.getMessageBatch(50, "testCommitter")
//      Await.result(messageBatchFuture, WAIT)
//      fetcher.cache.size must_== 1
//      fetcher.addToCache(50, "testCommitter", messageBatchFuture)
//      fetcher.cache.size must_== 2
//
//      numDsCalls(fetcher) must_== 1
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//
//      val messageBatchFuture2 = fetcher.getMessageBatch(53, "testCommitter")
//      val messageBatch = Await.result(messageBatchFuture2, WAIT)
//      fetcher.addToCache(53, "testCommitter", messageBatchFuture2)
//      fetcher.cache.size must_== 3
//
//      // Make sure we did indeed make another call to the datasource
//      numDsCalls(fetcher) must_== 2
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 2
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 2
//
//      messageBatch.offset must_== 53
//      messageBatch.size must_== 3
//      messageBatch.nextOffset must_== 56
//    }
//  }
//
//  "Fetcher" should {
//    "hit the datasource when requesting two batches simultaneously" in new actorSystem {
//      val metricsLogger = new MockMetricsLogger()
//      val probe = TestProbe()(system)
//      val fetcher = createBasicFetcher(probe.ref, metricsLogger = metricsLogger)
//
//      val messageBatchFuture = fetcher.getMessageBatch(50, "testCommitter")
//      val messageBatchFuture2 = fetcher.getMessageBatch(53, "testCommitter")
//
//      val messageBatch = Await.result(messageBatchFuture, WAIT)
//      val messageBatch2 = Await.result(messageBatchFuture2, WAIT)
//
//      fetcher.addToCache(50, "testCommitter", messageBatchFuture)
//      fetcher.addToCache(53, "testCommitter", messageBatchFuture2)
//
//      // Make sure there were two calls to the datasource
//      numDsCalls(fetcher) must_== 2
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 2
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 2
//
//      messageBatch.offset must_== 50
//      messageBatch.size must_== 3
//      messageBatch.nextOffset must_== 53
//
//      messageBatch2.offset must_== 53
//      messageBatch2.size must_== 3
//      messageBatch2.nextOffset must_== 56
//    }
//  }
//
//  "Fetcher" should {
//    "not cache old offsets" in new actorSystem {
//      val probe = TestProbe()(system)
//      val fetcher = createBasicFetcher(probe.ref)
//
//      val messageBatchFuture2 = fetcher.getMessageBatch(53, "testCommitter")
//
//      val messageBatch2 = Await.result(messageBatchFuture2, WAIT)
//      fetcher.addToCache(53, "testCommitter", messageBatchFuture2)
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 1
//      fetcher.cache.size must_== 2
//
//      val messageBatchFuture = fetcher.getMessageBatch(50, "testCommitter")
//      val messageBatch = Await.result(messageBatchFuture, WAIT)
//      fetcher.addToCache(50, "testCommitter", messageBatchFuture)
//
//      getCountFor(fetcher, Fetcher.CacheDataMetric) must_== 1
//      getCountFor(fetcher, Fetcher.CacheHitMetric) must_== 0
//      getCountFor(fetcher, Fetcher.CacheHitAlignMetric) must_== 0
//      getCountFor(fetcher, Fetcher.FetchMetric) must_== 2
//      fetcher.cache.size must_== 2
//
//      // Make sure there were two calls to the datasource
//      numDsCalls(fetcher) must_== 2
//
//      // Verify content were returned properly
//      messageBatch.offset must_== 50
//      messageBatch.size must_== 3
//      messageBatch.nextOffset must_== 53
//
//      messageBatch2.offset must_== 53
//      messageBatch2.size must_== 3
//      messageBatch2.nextOffset must_== 56
//    }
//  }
//
//  "Fetcher" should {
//    "handle a messageSet response from the datasource which does not match the requested offset" in new actorSystem {
//      val bb = ByteBuffer.allocate(20)
//      val message = new MessageAndOffset(new kafka.message.Message(bb), 532)
//      val indexedMessages = List().toIndexedSeq
//
//      val messageSet = mock[MessageSet]
//      messageSet.toIndexedSeq returns indexedMessages
//      messageSet.head returns message
//      messageSet.sizeInBytes returns 20
//      messageSet.size returns 1
//
//      val dataSource = mock[DataSource]
//      dataSource.getMessageSet("test", 5, 100) returns messageSet
//
//      val probe = TestProbe()(system)
//
//      val metricsLogger: MetricsLogger = new MockMetricsLogger()
//      val fetcherConfig = new FetcherConfig(NUM_COMMITTER_THREADS, NUM_CACHE_SLOTS, 0, OffsetOutOfRangePolicy.useLatestOffset,
//        OffsetOutOfRangePolicy.useLatestOffset)
//      val factory = new FetcherFactory(fetcherConfig, dataSource, metricsLogger)
//      val fetcher = factory.create(probe.ref, "test", 5)
//
//      val f: Future[CastleMessageBatch] = fetcher.getMessageBatch(100, "mock_committer")
//
//      val castleMessageBatch = Await.result(f, FiniteDuration(600, TimeUnit.SECONDS))
//
//      // We requested 100 but the batch has an offset of 532
//      castleMessageBatch.offset must_== 532
//    }
//  }
}
