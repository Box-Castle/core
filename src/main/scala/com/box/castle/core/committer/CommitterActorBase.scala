package com.box.castle.core.committer

import akka.actor.Actor
import com.box.castle.committer.api.{CommitterFactory, Committer, UnrecoverableException}
import com.box.castle.consumer.ConsumerId
import com.box.castle.core.config.CommitterConfig
import com.box.castle.core.const
import com.box.castle.metrics.MetricsLogger
import com.box.castle.retry.{RetryAsync, RetryStrategy}
import org.slf4s.Logging
import kafka.common.TopicAndPartition

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.control.{Exception, NonFatal}

import scala.util.{Success, Failure, Try}


trait CommitterActorBase {
  self: Actor with Logging =>

  def consumerId: ConsumerId

  def metricsLogger: MetricsLogger

  def committerFactory: CommitterFactory

  def topic: String

  def partition: Int

  def committerActorId: String

  def committerConfig: CommitterConfig

  implicit def ec: ExecutionContext

  def topicAndPartition: TopicAndPartition

  def receiveCommon(msg: Any): Unit

  def parallelismFactor: Int

  private lazy val createCommitterExceptionStrategy = committerFactory.createCommitterExceptionRetryStrategy

  private lazy val isRetryable = (t: Throwable) => {
    t match {
      case _: UnrecoverableException => false
      case NonFatal(e) => true
      case _ => false
    }
  }

  private lazy val committerFactoryPreRetry = (numRetries: Int, delay: FiniteDuration, cause: Throwable) => {
    log.warn(s"$committerActorId encountered a recoverable exception while calling " +
      s"${committerConfig.factoryClassName}.createAsync() on try number $numRetries. Next try in $delay.", cause)
  }

  def count(metricName: String, value: Long = 1): Unit = {
    Exception.ignoring(classOf[Throwable])(
      metricsLogger.count(
        const.Components.Committer,
        metricName,
        Map(const.TagNames.CommitterId -> committerConfig.id,
          const.TagNames.Topic -> topic, const.TagNames.Partition -> partition.toString),
        value))
  }

  def time(metricName: String, nanoSeconds: Long): Unit = {
    Exception.ignoring(classOf[Throwable])(
      metricsLogger.time(
        const.Components.Committer,
        metricName,
        Map(const.TagNames.CommitterId -> committerConfig.id,
          const.TagNames.Topic -> topic, const.TagNames.Partition -> partition.toString),
        nanoSeconds))
  }

  def scheduleOnce(duration: FiniteDuration, message: Any): Unit = {
    context.system.scheduler.scheduleOnce(duration, context.self, message)
  }

  lazy val userCommittersFuture: Future[IndexedSeq[Committer]] = Future.sequence(
    (0 until parallelismFactor).map(
      id => {
        RetryAsync(Future({
          log.info(s"$committerActorId calling ${committerConfig.factoryClassName}.createAsync() for sub-id $id")
          val committerFuture: Future[Committer] = committerFactory.createAsync(topic, partition, id)
          committerFuture onSuccess {
            case _ =>
              log.info(s"$committerActorId successfully created a committer for sub-id $id using ${committerConfig.factoryClassName}.createAsync()")
          }
          committerFuture
        }).flatMap(identity), createCommitterExceptionStrategy, isRetryable, committerFactoryPreRetry)
      }
    )
  )

  protected def toUserCommitters(userCommittersTry: Try[IndexedSeq[Committer]]): IndexedSeq[Committer] = {
    userCommittersTry match {
      case Success(userCommitters) => userCommitters
      case Failure(t) => throw new UnrecoverableCommitterActorException(committerConfig.id,
        s"CommitterFactory for ${committerConfig.id} was unable to create a committer instance for $topic-$partition", t)
    }
  }
}
