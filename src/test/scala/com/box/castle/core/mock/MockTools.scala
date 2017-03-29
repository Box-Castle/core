package com.box.castle.core.mock

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.box.castle.core.CuratorFactory
import com.box.castle.core.common.CastleFatalException
import com.box.castle.core.config.{CommitterConfig, InitialOffset}
import com.box.castle.router.mock.MockBatchTools
import org.slf4s.Logging
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.specs2.matcher.MatchSuccess
import org.specs2.mock.Mockito
import org.specs2.mutable.{BeforeAfter, Specification}
import org.joda.time.Duration

import scala.concurrent.duration._



trait MockTools extends Mockito with Logging with MockBatchTools {
  self: Specification =>

  val DEFAULT_MOCK_COMMITTER_ID = "mockKafkaCommitter"
  val DEFAULT_MOCK_COMMITTER_FACTORY = "com.box.x.y.z.mockKafkaCommitterFactory"
  val MOCK_COMMITTER_CONFIGS = List(createCommitterConfig())


  def mustEventuallyBeTrue(expression: => Boolean, length: Long, unit: TimeUnit): MatchSuccess[None.type] = {
    mustEventuallyBeTrue(expression, FiniteDuration(length, unit))
  }

  // Primitive, but effective
  def mustEventuallyBeTrue(expression: => Boolean, timeout: FiniteDuration = FiniteDuration(15, TimeUnit.SECONDS)): MatchSuccess[None.type] = {
    val maxIterations = timeout.toMillis / 50
    var i = 0
    while (i < maxIterations && !expression) {
      Thread.sleep(50)
      i += 1
    }
    if (i < maxIterations)
      ok
    else
      throw new RuntimeException(s"mustEventuallyBeTrue timed out after $timeout")
  }

  class MockCuratorFactory() extends CuratorFactory(null) {
    lazy val curator = {
      val c = CuratorFrameworkFactory.builder()
        .namespace(MockZookeeperServer.getNamespace)
        .connectString(MockZookeeperServer.getConnectString)
        .sessionTimeoutMs(30000)
        .connectionTimeoutMs(20000)
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .build
      c.start()
      c.blockUntilConnected(5000, MILLISECONDS)
      if (c.getState != CuratorFrameworkState.STARTED) {
        throw new CastleFatalException("Could not connect to the LOCAL MOCK ZK within the given timeout period")
      }
      c
    }

    override def create(): CuratorFramework = {
      curator
    }

    def close(): Unit = {
      curator.close()
    }
  }

  trait actorSystemAndCurator extends BeforeAfter {
    var initializedSystem = false
    lazy implicit val system: ActorSystem = {
      initializedSystem = true
      ActorSystem("EventSourceSpec")
    }

    var initializedCuratorFactory = false
    lazy val curatorFactory = {
      initializedCuratorFactory = true
      new MockCuratorFactory()
    }

    lazy val curator = curatorFactory.curator

    override def before: Unit = {

    }

    override def after = {
      if (initializedSystem)
        TestKit.shutdownActorSystem(system)

      if (initializedCuratorFactory)
        curatorFactory.close()
    }
  }

  def createCommitterConfig(initialOffset: InitialOffset.InitialOffset = InitialOffset.latest,
                            id: String = DEFAULT_MOCK_COMMITTER_ID,
                            committerFactory: String = DEFAULT_MOCK_COMMITTER_FACTORY,
                            customConfig: Map[String, String] = Map(),
                            topicsRegexRaw: Option[String] = Some(".*"),
                            topicsSet: Option[Set[String]] = None,
                            parallelismFactor: Int = 1,
                            heartbeatCadenceInMillis: Option[Duration] = None) = {
    CommitterConfig(id,
      committerFactory,
      customConfig,
      topicsRegexRaw,
      topicsSet,
      parallelismFactor = parallelismFactor,
      heartbeatCadenceInMillis = heartbeatCadenceInMillis)
  }
}
