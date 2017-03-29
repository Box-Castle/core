package com.box.castle.core.committer

import com.box.castle.committer.api.Committer
import kafka.message.Message

/**
 * Created by asobti on 9/16/15.
 */
private[committer] case class ChunkAndCommitter(chunk: IndexedSeq[Message], committer: Committer) {
  require(chunk.nonEmpty)
}
