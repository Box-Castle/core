package com.box.castle.core.committer

import akka.actor.Actor
import com.box.castle.committer.api._
import com.box.castle.consumer.ConsumerId
import com.box.castle.core.committer.states._
import com.box.castle.core.common.{GracefulShutdown, StartActor}
import com.box.castle.core.config.{CastleConfig, CommitterConfig}
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.messages.{CommitConsumerOffset, RouterResult}
import com.box.castle.router.{RouterRef, RouterRequestManager}
import org.slf4s.Logging
import kafka.common.TopicAndPartition

import scala.concurrent.Future
import scala.util.{Failure, Success}


class CommitterActor(val topic: String,
                     val partition: Int,
                     val castleConfig: CastleConfig,
                     val committerConfig: CommitterConfig,
                     val committerFactory: CommitterFactory,
                     val router: RouterRef,
                     val metricsLogger: MetricsLogger)
    extends Actor
    with RouterRequestManager
    with Logging
    with CommittingBatch
    with FetchingConsumerOffset
    with FetchingData
    with FetchingOffset
    with PreparingToCommitBatch
    with ShuttingDown
    with WaitingToRestart
    with ConsumerOffsetCommitTracker
    with Idling {

  val committerActorId = s"${committerConfig.id}:$topic-$partition"
  val topicAndPartition = TopicAndPartition(topic, partition)
  val consumerId = ConsumerId(castleConfig.namespace, committerConfig.id)
  implicit val ec = committerConfig.executionContext

  override val parallelismFactor = committerConfig.parallelismFactorByTopic.getOrElse(topic, committerConfig.parallelismFactor)

  override def preStart(): Unit = {
    log.info(s"$committerActorId with path: ${self.path}, parallelismFactor: $parallelismFactor is in preStart")
    self ! StartActor
  }

  def receive: Receive = {
    case StartActor => {
      log.info(s"Starting $committerActorId with path: ${self.path}, parallelismFactor: $parallelismFactor")

      // Start off by fetching the consumer offset
      becomeFetchingConsumerOffset()
    }
    case msg => receiveCommon(msg)
  }

  override def postStop(): Unit = {
    userCommittersFuture onComplete {
      case Success(userCommitters) => userCommitters.foreach(userCommitter => Future(userCommitter.close()))
      case Failure(_) => // Ignore
    }
  }

  override def receiveCommon(msg: Any): Unit = {
    msg match {
      case GracefulShutdown => {
        log.info(s"$committerActorId beginning graceful shutdown")
        becomeShuttingDown()
      }
      case routerResult: RouterResult => {
        routerResult match {
          case result: CommitConsumerOffset.Result => receiveCommitConsumerOffsetResult(result)
          case _ => // Ignore
        }
      }
      case _ => {
        log.error(s"$committerActorId ignoring: $msg")
      }
    }
  }
}
