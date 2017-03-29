package com.box.castle.core.supervisor

import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.actor.{Terminated, Actor, OneForOneStrategy}
import com.box.castle.committer.api.CommitterFactory
import com.box.castle.core.common.{GracefulShutdown, CastleFatalException, StartActor}
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.exceptions.RouterFatalException
import com.box.castle.router.{RouterRef, RouterFactory}
import com.box.castle.core.leader.{LeaderActorRef, LeaderActorFactory}
import com.box.castle.core.worker.{WorkerActorRef, WorkerActorFactory}
import com.box.castle.core.const
import org.slf4s.Logging

import scala.util.control.NonFatal

// $COVERAGE-OFF$

/**
 * This is the top level supervisor for the Castle service that
 * has the Worker, Leader, and Dispatcher actors as children.
 */
class CastleSupervisor(workerActorFactory: WorkerActorFactory,
                       leaderActorFactory: LeaderActorFactory,
                       routerActorFactory: RouterFactory,
                       committerFactoryMap: Map[String, CommitterFactory],
                       metricsLogger: MetricsLogger)
  extends Actor with Logging {

  var workerRef: Option[WorkerActorRef] = None
  var leaderRef: Option[LeaderActorRef] = None
  var numTerminationsReceived = 0

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e: RouterFatalException => {
        log.error("FATAL: CastleSupervisor got a RouterFatalException, stopping actor...", e)
        metricsLogger.count(const.Components.Supervisor, const.Metrics.Fatals)
        Stop
      }
      case e: CastleFatalException => {
        log.error("FATAL: CastleSupervisor got an CastleFatalException, stopping actor...", e)
        metricsLogger.count(const.Components.Supervisor, const.Metrics.Fatals)
        Stop
      }
      case e: IllegalArgumentException => {
        log.error("FATAL: CastleSupervisor got an IllegalArgumentException, stopping actor...", e)
        metricsLogger.count(const.Components.Supervisor, const.Metrics.Fatals)
        Stop
      }
      case NonFatal(e) => {
        log.warn(s"CastleSupervisor encountered a recoverable exception for a child: ${e.getMessage}")
        Restart
      }
      case t => super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

  override def preStart(): Unit = {
    self ! StartActor
  }

  def receive = {
    case StartActor => {
      log.info("Starting CastleSupervisor")
      start()
    }
    case RestartLeader(leader, router) => {
      log.info("Leader actor requested a restart")
      context.stop(leader)
      context.actorOf(leaderActorFactory.props(router))
    }
    case GracefulShutdown => {
      log.info("Shutting down CastleSupervisor")
      (workerRef, leaderRef) match {
        case (Some(workerActorRef), Some(leaderActorRef)) => {
          workerActorRef ! GracefulShutdown
          context.watch(workerActorRef)
          leaderActorRef ! GracefulShutdown
          context.watch(leaderActorRef)
          context.become(shuttingDown)
        }
        case _ => {
          log.info("CastleSupervisor does not yet have a WorkerActor or LeaderActor child, stopping it immediately")
          context.stop(self)
        }
      }
    }
  }

  def shuttingDown: Receive = {
    case Terminated(workerActorRef) => {
      numTerminationsReceived += 1
      if (numTerminationsReceived == 2) {
        log.info(s"Child WorkerActor and child LeaderActor have both shutdown gracefully, stopping CastleSupervisor")
        try {
          committerFactoryMap.foreach {
            case (committerId, committerFactory) => {
              try {
                committerFactory.close()
              }
              catch {
                case t: Throwable => log.warn(s"CastleSupervisor ignored exception while calling committerFactory.close() for $committerId", t)
              }
            }
          }
        }
        catch {
          case t: Throwable => log.warn(s"CastleSupervisor ignored exception while closing committer factories", t)
        }
        finally {
          context.stop(self)
        }
      }
    }
    case _ => log.info(s"CastleSupervisor is currently shutting down, ignoring all messages...")
  }

  private def start(): Unit = {
    val routerActor = RouterRef(context.actorOf(routerActorFactory.props()))

    // Start up the worker
    workerRef = Some(WorkerActorRef(context.actorOf(workerActorFactory.props(routerActor))))

    // Start up the leader
    leaderRef = Some(LeaderActorRef(context.actorOf(leaderActorFactory.props(routerActor))))
  }
}

// $COVERAGE-ON$