package com.box.castle.core.supervisor

import com.box.castle.core.leader.LeaderActorRef
import com.box.castle.router.RouterRef



case class RestartLeader(leader: LeaderActorRef, router: RouterRef)
