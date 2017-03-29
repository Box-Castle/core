package com.box.castle.core.leader

import java.util.concurrent.Executors

import akka.actor.Actor
import com.box.castle.core.common.{GracefulShutdown, StartActor}
import com.box.castle.core.config.LeaderConfig
import com.box.castle.core.leader.messages._
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.messages.FetchTopicMetadata
import com.box.castle.router.{RouterRef, RouterRequestManager}
import com.box.castle.core.supervisor.RestartLeader
import org.slf4s.Logging

import scala.concurrent.ExecutionContext



class LeaderActor(leaderFactory: LeaderFactory,
                  leaderConfig: LeaderConfig,
                  val router: RouterRef,
                  metricsLogger: MetricsLogger)
  extends Actor with Logging with RouterRequestManager {

  // No supervisor strategy exists for LeaderActor because it does not have any children

  lazy val leader = leaderFactory.create(LeaderActorRef(self))

  // We do this to avoid having to import scala.language.reflectiveCalls
  private class SchedulingExecutionContext extends ExecutionContext {
    val threadPool = Executors.newCachedThreadPool()

    def execute(runnable: Runnable): Unit = threadPool.submit(runnable)

    def reportFailure(t: Throwable): Unit = {
      log.error("Unexpected failure in SchedulingExecutionContext for LeaderActor", t)
      context.parent ! RestartLeader(LeaderActorRef(self), router)
    }

    def shutdown(): Unit = threadPool.shutdown()
  }

  private val schedulingEC = new SchedulingExecutionContext()

  private val kafkaTopicPollTimer =
    context.system.scheduler.schedule(leaderConfig.kafkaTopicsPollInterval,
      leaderConfig.kafkaTopicsPollInterval, self, PollAvailableKafkaTopics)(schedulingEC)

  private val availableWorkersPollTimer =
    context.system.scheduler.schedule(leaderConfig.availableWorkersPollInterval,
      leaderConfig.availableWorkersPollInterval, self, PollAvailableWorkers)(schedulingEC)

  var fetchTopicMetadataRequestId: Long = 0

  var isShutdown = false

  override def preStart() = {
    self ! StartActor
  }

  def hasLeadership: Receive =  {
    case msg: LeaderMessage => {
      msg match {
        case ProcessAvailableWorkersChange => {
          log.info("Available workers have changed, getting latest topics from Kafka")
          fetchTopicMetadata()
        }
        case BecomeLeader => {
          // Ignore these since we are the leader already
        }
        case PollAvailableWorkers => {
          leader.pollAvailableWorkers()
        }
        case WaitForLeadership => {
          context.become(waitingForLeadership)
          leader.waitForLeadership()
        }
        case PollAvailableKafkaTopics => {
          fetchTopicMetadata()
        }
        case FailedToAcquireLeadership => {
          context.parent ! RestartLeader(LeaderActorRef(self), router)
        }
      }
    }
    case result: FetchTopicMetadata.Result => {
      checkResult(result) {
        case FetchTopicMetadata.Success(_, topicMetadata) => {
          leader.processLatestTopics(topicMetadata.keys)
        }
      }
    }
    case GracefulShutdown => {
      context.become(shuttingDown)
      receiveGracefulShutdown()
    }
  }

  def waitingForLeadership: Receive =  {
    case msg: LeaderMessage => {
      msg match {
        case ProcessAvailableWorkersChange => {
          // Ignore worker changes when waiting for leadership
        }
        case BecomeLeader => {
          log.info("Just became leader, polling available workers and fetching topic metadata from Kafka")
          context.become(hasLeadership)
          leader.pollAvailableWorkers()
          fetchTopicMetadata()
        }
        case PollAvailableWorkers => {
          // Ignore while waiting for leadership
        }
        case WaitForLeadership => {
          // Already waiting for leadership
        }
        case PollAvailableKafkaTopics => {
          // Ignore while waiting for leadership
        }
        case FailedToAcquireLeadership => {
          context.parent ! RestartLeader(LeaderActorRef(self), router)
        }
      }
    }
    case StartActor => {
      log.info(s"Starting Leader Actor")
      leader.waitForLeadership()
    }
    case result: FetchTopicMetadata.Result => {
      // Ignore while waiting for leadership
    }
    case GracefulShutdown => {
      context.become(shuttingDown)
      receiveGracefulShutdown()
    }
  }

  def shuttingDown: Receive = {
    case _ => log.info(s"Leader is currently shutting down, ignoring all messages...")
  }

  private def receiveGracefulShutdown(): Unit = {
    log.info("Stopping LeaderActor")
    shutdown(isActorStopped=false)
  }

  override def postStop(): Unit = {
    shutdown(isActorStopped=true)
  }

  private def shutdown(isActorStopped: Boolean): Unit = {
    if (!isShutdown) {
      try {
        kafkaTopicPollTimer.cancel()
        availableWorkersPollTimer.cancel()
        leader.relinquishLeadership()
        leader.close()
        schedulingEC.shutdown()
      }
      catch {
        case t: Throwable => log.warn(s"LeaderActor ignored exception while shutting down", t)
      }
      finally {
        if (!isActorStopped) context.stop(self)
      }
    }
  }

  private def fetchTopicMetadata(): Unit = {
    fetchTopicMetadataRequestId += 1
    sendRequestToRouter(FetchTopicMetadata(fetchTopicMetadataRequestId))
  }

  // We start off waiting for leadership
  def receive = waitingForLeadership
}
