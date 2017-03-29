package com.box.castle.core.worker

import akka.actor.ActorRef
import com.box.castle.common.TypedActorRef



case class WorkerActorRef(ref: ActorRef) extends TypedActorRef