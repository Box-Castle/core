package com.box.castle.core.committer.manager

import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.actor._
import com.box.castle.core._
import com.box.castle.core.committer.{CommitterActorRef, UnrecoverableCommitterActorException, CommitterActorFactory}
import com.box.castle.core.common.{GracefulShutdown, CastleFatalException, StartActor}
import com.box.castle.core.config.CommitterConfig
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterRef
import com.box.castle.core.worker.tasks.Task
import org.slf4s.Logging

import scala.util.control.NonFatal


class CommitterManagerActor(assignedTask: Task,
                   router: RouterRef,
                   committerConfigs: Iterable[CommitterConfig],
                   committerActorFactory: CommitterActorFactory,
                   metricsLogger: MetricsLogger)
  extends Actor with Logging {

  val topic = assignedTask.topic
  val partition = assignedTask.partition

  var committerActors = Set.empty[CommitterActorRef]

  override def preStart() = {
    self ! StartActor
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e: UnrecoverableCommitterActorException => {
        log.error(s"Committer ${e.committerId} for $topic-$partition encountered an " +
          s"unrecoverable error: ${e.getMessage}, stopping this actor", e)
        metricsLogger.count(const.Components.Committer, const.Metrics.Failures,
          Map(const.TagNames.CommitterId -> e.committerId,
            const.TagNames.Topic -> topic, const.TagNames.Partition -> partition.toString))
        Stop
      }
      case _: CastleFatalException => Escalate
      case e: IllegalArgumentException => {
        log.error("FAILURE: CommitterManagerActor got an IllegalArgumentException, stopping CommitterActor...", e)
        metricsLogger.count(const.Components.CommitterManager, const.Metrics.Failures)
        Stop
      }
      case NonFatal(_) => Restart
      case t => super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

  def receive = {
    case StartActor => {
      log.info(s"Starting CommitterManagerActor for $assignedTask with akka path: ${self.path}")
      val assignedCommitters = committerConfigs.foldLeft(Set.empty[String])((assignedCommitters, committerConfig) => {
        if (assignedTask.committerIds.contains(committerConfig.id)) {
          log.info(s"CommitterManagerActor is creating a ${committerConfig.id} committer for $topic-$partition")
          committerActors = committerActors +
            CommitterActorRef(context.actorOf(committerActorFactory.props(topic, partition, committerConfig, router)))
        }
        assignedCommitters + committerConfig.id
      })

      assignedTask.committerIds.foreach(committerId => {
        if (!assignedCommitters.contains(committerId))
          log.warn(s"CommitterManagerActor did not create a $committerId committer for $topic-$partition " +
            s"that was assigned by the leader because it is not configured in Castle.  This is expected if you are doing " +
            s"a rolling deployment and have added or removed this committer from the configuration.")
      })
    }
    case GracefulShutdown => {
      log.info(s"Shutting down CommitterManagerActor for $assignedTask with akka path: ${self.path}")
      context.become(shuttingDown)

      if (committerActors.isEmpty) {
        log.info(s"Stopping CommitterManagerActor for $assignedTask because it has no children.")
        context.stop(self)
      } else {
        committerActors.foreach(committerActor => {
          context.watch(committerActor)
          committerActor ! GracefulShutdown
        })
      }
    }
  }

  def shuttingDown: Receive = {
    case Terminated(committerActorRef) => {
      log.info(s"CommitterActor ${committerActorRef.path} has shutdown gracefully")
      committerActors = committerActors.tail
      if (committerActors.isEmpty) {
        log.info(s"All child CommitterActors have shutdown gracefully, stopping " +
          s"CommitterManagerActor for $assignedTask")
        context.stop(self)
      }
    }
    case _ =>
      log.info(s"The CommitterManagerActor for $assignedTask is currently shutting down, ignoring all messages...")
  }
}
