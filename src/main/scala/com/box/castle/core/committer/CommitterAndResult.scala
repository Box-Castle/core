package com.box.castle.core.committer

import com.box.castle.committer.api.{CommitResult, Committer}

/**
 * Created by asobti on 9/16/15.
 */
private[committer] case class CommitterAndResult(committer: Committer, result: CommitResult)
