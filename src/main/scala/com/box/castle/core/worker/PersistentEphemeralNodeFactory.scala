package com.box.castle.core.worker

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode



class PersistentEphemeralNodeFactory {

  def create(client: CuratorFramework,
             mode: PersistentEphemeralNode.Mode,
             basePath: String,
             data: Array[Byte]): PersistentEphemeralNode = {
    new PersistentEphemeralNode(client, mode, basePath, data)
  }
}