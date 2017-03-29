package com.box.castle.core.leader

import akka.actor.ActorRef
import com.box.castle.common.TypedActorRef



case class LeaderActorRef(ref: ActorRef) extends TypedActorRef