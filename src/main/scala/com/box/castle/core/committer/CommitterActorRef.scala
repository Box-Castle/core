package com.box.castle.core.committer

import akka.actor.ActorRef
import com.box.castle.common.TypedActorRef


case class CommitterActorRef(ref: ActorRef) extends TypedActorRef
