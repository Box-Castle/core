package com.box.castle.core.worker

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import akka.actor.SupervisorStrategy.{Stop, Escalate, Restart}
import akka.actor.{Terminated, Actor, OneForOneStrategy, Props}
import com.box.castle.core.committer.manager.{CommitterManagerActorRef, CommitterManagerActorFactory}
import com.box.castle.core.common.{GracefulShutdown, CastleFatalException, StartActor}
import com.box.castle.core.config.CommitterConfig
import com.box.castle.core.const
import com.box.castle.core.worker.WorkerActor.CheckKeeperState
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterRef
import com.box.castle.core.worker.messages._
import com.box.castle.core.worker.tasks.{AssignedTasks, Task}
import org.slf4s.Logging
import org.apache.zookeeper.Watcher.Event.KeeperState._
import org.apache.zookeeper.Watcher.Event.EventType._
import scala.concurrent.duration._

import scala.util.control.{Exception, NonFatal}



class WorkerActor(workerFactory: WorkerFactory,
                  router: RouterRef,
                  committerManagerActorFactory: CommitterManagerActorFactory,
                  committerConfigs: Iterable[CommitterConfig],
                  val metricsLogger: MetricsLogger)
  extends Actor with Logging {
  import context.dispatcher
  val workerActorId = WorkerActor.uniqueIdGenerator.getAndIncrement()
  log.info(s"Constructing WorkerActor-$workerActorId")

  lazy val worker = workerFactory.create(WorkerActorRef(self), workerActorId)
  var currentCommitterManagers = Map.empty[Task, CommitterManagerActorRef]
  var isKeeperStateConnected: Boolean = true

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _: CastleFatalException => Escalate
      case e: IllegalArgumentException => {
        log.error("WorkerActor got an IllegalArgumentException, stopping CommitterActorManager...", e)
        metricsLogger.count(const.Components.Worker, const.Metrics.Failures)
        Stop
      }
      case NonFatal(_) => Restart
      case t => super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

  override def preStart(): Unit = {
    log.info(s"WorkerActor-$workerActorId is in preStart state")
    self ! AssignedTasksChanged(workerActorId, worker.assignedTasksWatcherId)
  }

  def receive = {
    case workerMessage: WorkerMessage => {
      if (workerMessage.workerActorId == workerActorId) {
        workerMessage match {
          case AssignedTasksChanged(_, assignedTasksWatcherId) => {
            log.info(s"WorkerActor-$workerActorId processing assigned tasks changes")
            if (assignedTasksWatcherId == worker.assignedTasksWatcherId) {
              processNewTasks(worker.getAssignedTasks)
            }
            else {
              log.error(s"WorkerActor-$workerActorId ignoring assigned tasks changed message " +
                s"with assignedTaskWatcherId: $assignedTasksWatcherId, expected: ${worker.assignedTasksWatcherId}")
              count(const.Metrics.Failures)
            }
          }
          case _: AssignedTasksNodeDeleted => {
            throw new WorkerException(s"WorkerActor-$workerActorId had its assigned tasks node " +
              s"unexpectedly deleted in ZooKeeper.  For safety, the actor will be fully re-initialized")
          }
          case NoneWatcherType(_, keeperState) => {
            log.info(s"WorkerActor-$workerActorId processing None WatcherType with keeperState: ${keeperState}")
            metricsLogger.count(const.Components.Worker, const.Metrics.WatcherEvent,
              Map(
                const.TagNames.WatcherEventType -> None.toString,
                const.TagNames.KeeperState -> keeperState.toString
              ))

            keeperState match {
              case Disconnected => {
                log.info(s"WorkerActor-$workerActorId processing None WatcherType with Disconnected state")
                // we set the local keeper state to be disconnected and schedule a
                // message to self in future to check if we are still disconnected
                isKeeperStateConnected = false
                context.system.scheduler.scheduleOnce(WorkerActor.checkKeeperStateDelay, context.self, CheckKeeperState)
              }
              case SyncConnected => {
                log.info(s"WorkerActor-$workerActorId processing None WatcherType with SyncConnected state")
                isKeeperStateConnected = true
              }
              case Expired => {
                throw new WorkerException(s"ZK connection for WorkerActor-$workerActorId expired. Restarting WorkerActor.")
              }
              case _ => throw new WorkerException(s"WorkerActor-$workerActorId encountered an unexpected " +
                s"keeper state: (${keeperState.toString}) while handling watcher type none for its assigned tasks " +
                s"node in ZooKeeper. For safety, the actor will be fully re-initialized.")
            }
          }
          case unexpectedWatcherType: UnexpectedWatcherType => {
            throw new WorkerException(s"WorkerActor-$workerActorId encountered an unexpected " +
              s"watcher type: (${unexpectedWatcherType.eventType.toString}) for its assigned tasks " +
              s"node in ZooKeeper.  For safety, the actor will be fully re-initialized.")
          }
        }
      }
      else {
        log.info(s"WorkerActor-$workerActorId ignoring AssignedTasksChanged message sent " +
          s"from a different WorkerActor instance: ${workerMessage.workerActorId}")
      }
    }
    case CheckKeeperState => {
      if(!isKeeperStateConnected) {
        metricsLogger.count(const.Components.Worker, const.Metrics.ZkReconnectFailed)
        throw new WorkerException(s"ZK connection for WorkerActor-$workerActorId could not be re-established " +
          s"after ${WorkerActor.checkKeeperStateDelay}. Restarting WorkerActor.")
      }
      else {
        log.info(s"WorkerActor-$workerActorId was able to re-establish the ZK connection within " +
          s"the ${WorkerActor.checkKeeperStateDelay} timeout")
      }
    }
    case GracefulShutdown => {
      log.info(s"WorkerActor-$workerActorId shutting down with akka path: ${self.path}")
      context.become(shuttingDown)
      currentCommitterManagers.values.foreach(committerManagerActor => {
        context.watch(committerManagerActor)
        committerManagerActor ! GracefulShutdown
      })
    }
  }

  def shuttingDown: Receive = {
    case Terminated(committerActorRef) => {
      log.info(s"WorkerActor-$workerActorId - CommitterManagerActor with path ${committerActorRef.path} has shutdown")
      currentCommitterManagers = currentCommitterManagers.tail
      if (currentCommitterManagers.isEmpty) {
        log.info(s"WorkerActor-$workerActorId - All child CommitterManagerActors have shutdown, stopping WorkerActor and shutting " +
          s"down all committer execution contexts")
        Exception.ignoring(classOf[Throwable])(committerConfigs.foreach(_.executionContext.shutdown()))
        context.become(shutdown)
        context.stop(self)
      }
    }
    case _ => log.info(s"WorkerActor-$workerActorId is currently shutting down, ignoring all messages...")
  }

  def shutdown: Receive = {
    case _ => log.info(s"WorkerActor-$workerActorId has already shut down, ignoring all messages...")
  }

  override def postStop(): Unit = {
    Exception.ignoring(classOf[Throwable])(worker.removeAvailability())
    Exception.ignoring(classOf[Throwable])(worker.close())
  }

  private def processNewTasks(newAssignedTasks: AssignedTasks): Unit = {
    val newTasks = newAssignedTasks.tasks
    val currentTasks = currentCommitterManagers.keys.toSet
    val tasksToStop = currentTasks -- newTasks
    val tasksToStart = newTasks -- currentTasks

    tasksToStop.foreach(task => {
      log.info(s"WorkerActor-$workerActorId stopping CommitterManagerActor for task: $task because " +
        s"it is no longer assigned to this Worker")
      context.stop(currentCommitterManagers(task))
      currentCommitterManagers = currentCommitterManagers - task
    })

    tasksToStart.foreach(task => {
      log.info(s"WorkerActor-$workerActorId starting CommitterManagerActor for task: $task because " +
        s"it was assigned to this Worker")
      currentCommitterManagers = currentCommitterManagers + (task ->
        CommitterManagerActorRef(context.actorOf(committerManagerActorFactory.props(task, router))))
    })
  }

  private def count(metricName: String, value: Long = 1): Unit = {
    Exception.ignoring(classOf[Throwable])(metricsLogger.count(const.Components.Worker, metricName, value=value))
  }
}

object WorkerActor {
  val uniqueIdGenerator = new AtomicLong(0)
  val checkKeeperStateDelay = FiniteDuration(5, TimeUnit.SECONDS)
  case object CheckKeeperState
}
