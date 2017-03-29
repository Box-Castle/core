package com.box.castle.core.mock

import com.box.castle.committer.api.{BatchCommittedSuccessfully, CommitResult, Committer}
import kafka.message.Message



class MockCommitter extends Committer {
  def isMockCommitter = true
  override def close(): Unit = { }
  override def commit(messageBatch: IndexedSeq[Message], metadata: Option[String]): CommitResult = BatchCommittedSuccessfully()
}
