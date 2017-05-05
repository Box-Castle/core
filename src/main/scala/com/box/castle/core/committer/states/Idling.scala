package com.box.castle.core.committer.states

import akka.actor.Actor
import com.box.castle.committer.api._
import com.box.castle.core.committer.{UnrecoverableCommitterActorException, ConsumerOffsetCommitTracker, OffsetLagTracker, CommitterActorBase}
import com.box.castle.router.RouterRequestManager
import com.box.castle.router.messages.{OffsetAndMetadata, FetchOffset, CommitConsumerOffset}
import org.slf4s.Logging

import org.joda.time.{DateTime, Duration, Period}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 *
 * This state represents the idle state. It is in this state that we heartbeat (if heartbeat is enabled).
 * Depending on the user configured delay and heartbeat cadence, this state either does a heartbeat
 * or transitions to FetchingData.
 *
 * The above implies that if data is continuously fetched, we will not enter the Idle state and hence
 * we will not heartbeat.
 *
 */
trait Idling extends CommitterActorBase with CommitterActorStates with OffsetLagTracker {
  self: Actor with RouterRequestManager with Logging =>

  import Idling._

  // heartbeat if custom user committer has heartbeat time set
  //   heartbeatTime is an option where None means we do not heartbeat
  //   and Some(t) means heartbeat with duration t
  private val configuredHeartbeatDuration = committerConfig.heartbeatCadenceInMillis

  private val ZEROTIME = new DateTime(1970, 1, 1, 1, 1)

  private var lastHeartbeatTime = ZEROTIME
  private var nextDataFetchTime = ZEROTIME

  /**
   * Convenience method to get current time, easily overwritten for testing
   */
  private[core] def getCurrentTime: DateTime = DateTime.now()

  /**
   *
   * Heartbeat is triggered if it is configured to do so if no data is fetched
   *
   * @param offsetAndMetadata
   */
  override def becomeIdling(offsetAndMetadata: OffsetAndMetadata, delay: Duration): Unit = {

    context.become(idling)

    configuredHeartbeatDuration match {
      case Some(heartbeatDuration) => {
        this.nextDataFetchTime = getCurrentTime.plus(delay)
        scheduleNextIdleIteration(offsetAndMetadata)
      }
      case None => {
        // heartbeat is not enabled
        scheduleOnce(new FiniteDuration(delay.getMillis, MILLISECONDS), FetchDataAfterDelay(offsetAndMetadata))
      }
    }
  }

  private[core] def scheduleNextIdleIteration(offsetAndMetadata: OffsetAndMetadata): Unit = {
    val (delayDuration, idlingMessage) =
      nextMessageAndDelay(getCurrentTime, this.lastHeartbeatTime,
        configuredHeartbeatDuration.get, this.nextDataFetchTime, offsetAndMetadata)

    scheduleOnce(delayDuration, idlingMessage)
  }

  private[core] def checkParallelismFactor(parallelismFactor: Int): Unit = {
    if (parallelismFactor > 1)
      throw new UnrecoverableCommitterActorException(committerConfig.id,
        s"Committing with metadata currently only supports parallelism factor equal to one. " +
          s"Your factor is ${parallelismFactor}")
  }

  /**
   * Calls heartbeat on userCommitters
   */
  private[core] def performHeartbeat(userCommitters: IndexedSeq[Committer], offsetAndMetadata: OffsetAndMetadata): Unit = {
    //currently, heartbeat only supports one user committer(parallelism factor equals to 1)
    //as when the framework gets the updated metadata back, it has no idea which metadata to use
    checkParallelismFactor(userCommitters.size)

    this.lastHeartbeatTime = getCurrentTime

    Future.traverse(userCommitters) { userCommitter =>
      val heartbeatPromise = Promise[Option[String]]()
      Future {
        try {
          val updatedMetadata = userCommitter.heartbeat(offsetAndMetadata.metadata)
          heartbeatPromise.success(updatedMetadata)
        } catch {
          case t: Throwable => {
            heartbeatPromise.failure(t)
          }
        }
      }
      heartbeatPromise.future
    } onComplete {
      case Success(updatedMetadatas) => {
        log.debug(s"User Committer heartbeat successful")
        context.self ! CompleteHeartbeat(OffsetAndMetadata(0, updatedMetadatas.head))
      }
      case Failure(t) => {
        log.warn(s"Committer failed to heartbeat with error message: ${t.getMessage}")
        context.self ! CompleteHeartbeat(offsetAndMetadata)
      }
    }
  }

  /**
   * Initializes userCommitters if they do not exist before performing heartbeat
   */
  private[core] def triggerHeartbeat(offsetAndMetadata: OffsetAndMetadata): Unit = {
    userCommittersFuture.value match {
      case Some(userCommittersTry: Try[IndexedSeq[Committer]]) => {
        val userCommitters = toUserCommitters(userCommittersTry)

        /**
         * This is the case that happens every single time except the first time. This is an optimization,
         * we could easily do an onComplete every time but that would cause unnecessary thread switching
         */
        performHeartbeat(userCommitters, offsetAndMetadata)
      }
      case None => {
        /**
         * This is the first time userCommittersFuture is called upon, hence we wait for the future to complete
         */
        userCommittersFuture onComplete {
          userCommittersTry => {
            val userCommitters = toUserCommitters(userCommittersTry)
            performHeartbeat(userCommitters, offsetAndMetadata)
          }
        }
      }
    }
  }

  override def idling: Receive = {
    case message: IdlingMessage => {
      message match {
        case FetchDataAfterDelay(offsetAndMetadata) => becomeFetchingData(offsetAndMetadata)
        case PerformHeartbeat(offsetAndMetadata) => triggerHeartbeat(offsetAndMetadata)
        case CompleteHeartbeat(offsetAndMetadata) => {
          scheduleNextIdleIteration(offsetAndMetadata)
        }
      }
    }
    case result: FetchOffset.Result => receiveFetchOffset(result)
    case message => receiveCommon(message)
  }
}


object Idling {

  sealed abstract class IdlingMessage(offsetAndMetadata: OffsetAndMetadata)

  // Idling state can receive three messages:
  //   1. message that triggers transition to FetchingData
  case class FetchDataAfterDelay(offsetAndMetadata: OffsetAndMetadata) extends IdlingMessage(offsetAndMetadata)

  //   2-3 messages sent from this (Idling) state to signify performing and completing hb
  case class PerformHeartbeat(offsetAndMetadata: OffsetAndMetadata) extends IdlingMessage(offsetAndMetadata)

  case class CompleteHeartbeat(offsetAndMetadata: OffsetAndMetadata) extends IdlingMessage(offsetAndMetadata)

  /**
   * Given last heartbeatTime and nextDataFetchTime, return expected delay and next message
   */
  private[core] def nextMessageAndDelay(currentTime: DateTime,
                                        lastheartbeatTime: DateTime,
                                        heartbeatDuration: Duration,
                                        nextDataFetchTime: DateTime,
                                        offsetAndMetadata: OffsetAndMetadata): (FiniteDuration, IdlingMessage) = {
    val nextHeartbeatTime = lastheartbeatTime.plus(heartbeatDuration)

    if (nextHeartbeatTime.isBefore(nextDataFetchTime)) {
      val delayDurationInMs = if (nextHeartbeatTime.isBefore(currentTime)) 0
      else new Period(currentTime, nextHeartbeatTime).toStandardDuration().getMillis()

      (new FiniteDuration(delayDurationInMs, MILLISECONDS), PerformHeartbeat(offsetAndMetadata))
    }
    else {
      val delayDurationInMs = if (nextDataFetchTime.isBefore(currentTime)) 0
      else new Period(currentTime, nextDataFetchTime).toStandardDuration().getMillis()

      (new FiniteDuration(delayDurationInMs, MILLISECONDS), FetchDataAfterDelay(offsetAndMetadata))
    }
  }
}
