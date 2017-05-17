package com.box.castle.core.committer.states

import akka.actor.Actor
import com.box.castle.batch.CastleMessageBatch
import com.box.castle.committer.api.Committer
import com.box.castle.core.committer.CommitterActorBase
import com.box.castle.router.RouterRequestManager
import com.box.castle.router.messages.FetchData.{NoMessages, Success}
import org.slf4s.Logging

import scala.util.Try


private[committer] trait PreparingToCommitBatch extends CommitterActorBase with CommitterActorStates {
  self: Actor with RouterRequestManager with Logging =>

  private case class CommitBatch(userCommitters: IndexedSeq[Committer], batch: Either[CastleMessageBatch, NoMessages], metadata: Option[String])

  override def becomePreparingToCommitBatch(batch: Either[CastleMessageBatch, NoMessages], metadata: Option[String]): Unit = {
    // We use .value here on the Future in order to bypass creating a context for every call which is
    // unnecessary once the initial one time construction of the committer is done by the CommitterFactory
    userCommittersFuture.value match {
      case Some(userCommittersTry: Try[IndexedSeq[Committer]]) =>

        /**
          * This is the case that happens every single time except the first time.  We instantly transition
          * to the committngBatch state and bypass this preparingToCommitBatch state.  This is an optimization,
          * we could easily become preparingToCommitBatch every time and send this message to ourselves, and then
          * transition to committingBatch from the Receive method defined below.  But this is wasteful, so we
          * short circuit the state change to preparingToCommitBatch.
          */
        becomeCommittingBatch(toUserCommitters(userCommittersTry), batch, metadata)
      case None => {
        /**
          * Here we actually become preparingToCommitBatch and wait for the future to complete with the userCommitters
          * we require to actually commit the batch.
          */
        context.become(preparingToCommitBatch)
        userCommittersFuture onComplete {
          userCommittersTry => {
            context.self ! CommitBatch(toUserCommitters(userCommittersTry), batch, metadata)
          }
        }
      }
    }
  }

  override def preparingToCommitBatch: Receive = {
    case CommitBatch(userCommitters, batch, metadata) => becomeCommittingBatch(userCommitters, batch, metadata)
    case msg => receiveCommon(msg)
  }
}
