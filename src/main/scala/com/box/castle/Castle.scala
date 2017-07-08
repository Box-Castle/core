package com.box.castle

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.pattern.gracefulStop
import com.box.castle.committer.api.{CommitterFactory, Filter}
import com.box.castle.consumer.offsetmetadatamanager.ZookeeperOffsetMetadataManagerFactory
import com.box.castle.consumer.{CastleSimpleConsumerFactory, ClientId, ConsumerId}
import com.box.castle.core.{CuratorFactory, util}
import com.box.castle.core.committer.CommitterActorFactory
import com.box.castle.core.committer.manager.CommitterManagerActorFactory
import com.box.castle.core.common.GracefulShutdown
import com.box.castle.core.config.CastleConfig
import com.box.castle.core.leader.{LeaderActorFactory, LeaderFactory, TaskManager}
import com.box.castle.core.supervisor.CastleSupervisorFactory
import com.box.castle.core.worker.{WorkerActorFactory, WorkerFactory}
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterFactory
import com.box.castle.router.kafkadispatcher.KafkaDispatcherFactory
import com.box.castle.router.proxy.KafkaDispatcherProxyPoolFactory
import org.slf4s.Logging
import java.nio.file.{Path, Paths}

import scala.concurrent.Await

/**
 * This is the entry point class into the Castle framework.  An instance of this class can be used to start and
 * stop the Castle service.
 */
class Castle private (val castleConfig: CastleConfig, val metricsLogger: MetricsLogger, val clientId: ClientId) extends Logging {

  val committerFactoryMap = castleConfig.committerConfigs.map(committerConfig => {
    committerConfig.id -> {
      val clazz = Class.forName(committerConfig.factoryClassName)
      val clazzConstructor =
        try
          clazz.getConstructor(classOf[Map[String, String]])
        catch {
          case _: NoSuchMethodException => throw new NoSuchMethodException(
            s"${committerConfig.factoryClassName} factory must provide a single argument constructor that takes a Map[String,String] type")
        }
      clazzConstructor.newInstance(committerConfig.customConfig).asInstanceOf[CommitterFactory]
    }
  }).toMap

  val filterMap: Map[String, Filter] = committerFactoryMap map {
    idCommitterFactory => {
      idCommitterFactory._1 -> idCommitterFactory._2.createFilter()
    }
  }
  // Let's wire up our dependency tree...
  val curatorFactory = new CuratorFactory(castleConfig)

  val committerActorFactory = new CommitterActorFactory(castleConfig, committerFactoryMap, metricsLogger)

  val useKafkaOffsetMetadataManager = castleConfig.committerConfigs.map(committerConfig =>
    ConsumerId(castleConfig.namespace, committerConfig.id) -> committerConfig.useKafkaMetadataManager
  ).toMap

  val zkConfig = castleConfig.castleZookeeperConfig

  val zookeeperOffsetMetadataManagerFactory =
    new ZookeeperOffsetMetadataManagerFactory(Paths.get(util.join(castleConfig.castleZookeeperConfig.root, castleConfig.namespace)),
      zkConfig.connect, zkConfig.retryPolicy, zkConfig.initialConnectTimeout,
      zkConfig.connectionTimeout, zkConfig.sessionTimeout)

  val boxSimpleConsumerFactory = new CastleSimpleConsumerFactory(clientId,
    Some(zookeeperOffsetMetadataManagerFactory),
    castleConfig.brokerTimeout,
    castleConfig.bufferSizeInBytes,
    useKafkaOffsetMetadataManager=useKafkaOffsetMetadataManager)

  val kafkaDispatcherFactory = new KafkaDispatcherFactory(boxSimpleConsumerFactory, metricsLogger, castleConfig.routerConfig)

  val consumerPoolFactory = new KafkaDispatcherProxyPoolFactory(kafkaDispatcherFactory,
    castleConfig.cacheSizeInBytes,
    metricsLogger)

  val routerActorFactory = new RouterFactory(consumerPoolFactory,
    castleConfig.brokers,
    metricsLogger)

  val fetcherActorFactory = new CommitterManagerActorFactory(committerActorFactory,
    castleConfig.committerConfigs,
    metricsLogger)

  val workerFactory = new WorkerFactory(clientId, curatorFactory)

  val workerActorFactory = new WorkerActorFactory(fetcherActorFactory, workerFactory, castleConfig.committerConfigs, metricsLogger)

  val taskManager = TaskManager(castleConfig.leaderConfig, castleConfig.committerConfigs, filterMap)

  val leaderFactory = new LeaderFactory(castleConfig.leaderConfig,
    clientId,
    curatorFactory,
    taskManager,
    metricsLogger)

  val leaderActorFactory = new LeaderActorFactory(leaderFactory,
    castleConfig.leaderConfig,
    metricsLogger)

  val castleSupervisorFactory = new CastleSupervisorFactory(workerActorFactory,
    leaderActorFactory,
    routerActorFactory,
    committerFactoryMap,
    metricsLogger)

  val system = ActorSystem("castle")

  def start(): Unit = {
    system.actorOf(castleSupervisorFactory.props(), "castle-supervisor")
  }

  /**
   * Stops Castle, this method will block until all committers have shut down gracefully or the grace period
   * specified in the Castle configuration expires.
   */
  def stop(): Unit = {
    val timeout = castleConfig.gracefulShutdownTimeout
    val supervisor = Await.result(system.actorSelection("/user/castle-supervisor").resolveOne(timeout), timeout)
    try {
      Await.result(gracefulStop(supervisor, timeout, GracefulShutdown), timeout)
      log.info("CastleSupervisor has shutdown gracefully, proceeding with shut down of the Akka System")
    } catch {
      case e @ (_: akka.pattern.AskTimeoutException | _: java.util.concurrent.TimeoutException) => {
        log.warn(s"Failed to gracefully shut down CastleSupervisor within the specified grace period of: $timeout")
      }
    }
    system.shutdown()
  }
}

object Castle {
  def apply(castleConfig: CastleConfig, metricsLogger: MetricsLogger = MetricsLogger.defaultLogger, clientId: Option[ClientId] = None): Castle = {
    new Castle(castleConfig,
      metricsLogger, clientId.getOrElse(ClientId(castleConfig.namespace + "-" + InetAddress.getLocalHost.toString)))
  }
}