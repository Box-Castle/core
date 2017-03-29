package com.box.castle.core.mock

import org.apache.curator.test.TestingServer



object MockZookeeperServer {
  private lazy val testServer = {
    val server = new TestingServer()
    server.start()
    server
  }
  private val lock = new Object()
  private var counter = 1

  def getNamespace: String = {
    lock.synchronized {
      counter += 1
      return "mockCurator-" + counter.toString
    }
  }

  def getConnectString: String = testServer.getConnectString
}
