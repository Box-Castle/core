package com.box.castle.core.leader.messages



private[leader] sealed abstract class LeaderMessage
private[leader] case object ProcessAvailableWorkersChange extends LeaderMessage
private[leader] case object PollAvailableWorkers extends LeaderMessage
private[leader] case object PollAvailableKafkaTopics extends LeaderMessage
private[leader] case object WaitForLeadership extends LeaderMessage
private[leader] case object BecomeLeader extends LeaderMessage
private[leader] case object FailedToAcquireLeadership extends LeaderMessage