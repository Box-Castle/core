package com.box.castle.core.committer.manager

import akka.actor.ActorRef
import com.box.castle.common.TypedActorRef


case class CommitterManagerActorRef(ref: ActorRef) extends TypedActorRef
