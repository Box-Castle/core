package com.box.castle.core


package object const {

  object Components {
    val Supervisor = "supervisor"
    val Committer = "committer"
    val CommitterManager = "committer_manager"
    val Worker = "worker"
    val Leader = "leader"
  }

  object Metrics {
    // These are non fatal failures
    val UnexpectedFailures = "unexpected_failures"

    val OffsetOutOfRange = "offset_out_of_range"

    val AvailableWorkersChanged = "available_workers_changed"

    // These are fatal failures where the Castle framework cannot continue to operate in a healthy way
    val Fatals = "fatals"

    val Failures = "failures"

    val RecoverableFailures = "recoverable_failures"

    val NumMessagesCommitted = "num_messages_committed"

    val CorruptMessagesSkipped = "corrupt_messages_skipped"

    val OffsetLag = "offset_lag"

    val CommitTime = "commit_time"
    val FetchTime = "fetch_time"
    val BytesProcessed = "bytes_processed"

    val WatcherEvent = "watcher_event"

    val ZkReconnectFailed = "zk_reconnect_failed"
  }

  object TagNames {
    val CommitterId = "committer"
    val Topic = "topic"
    val Partition = "partition"
    val KeeperState = "keeper_state"
    val WatcherEventType = "event_type"
    val ClientId = "client_id"
  }

  object CastleZkPaths {
    val WorkerAvailability = "/workers/available"
    val WorkerIds = "/workers/ids"
    val WorkerTasks = "/workers/tasks"
    val LeaderElection = "/leader_election"
    val ClusterInfo = "/cluster_info"
  }
}
