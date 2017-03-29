package com.box.castle.core.worker

import com.box.castle.core.CuratorFactory
import com.box.castle.core.config.CommitterConfig
import com.box.castle.consumer.ClientId



class WorkerFactory(val clientId: ClientId,
                    val curatorFactory: CuratorFactory) {

  def create(workerActor: WorkerActorRef, workerActorId: Long): Worker =
    new Worker(workerActor, workerActorId, clientId, curatorFactory)
}
