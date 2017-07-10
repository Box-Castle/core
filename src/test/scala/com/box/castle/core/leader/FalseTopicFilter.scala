package com.box.castle.core.leader

import com.box.castle.committer.api.TopicFilter

/**
  * Created by bravishanker on 7/10/17.
  */
class FalseTopicFilter extends TopicFilter {

  override def matches(topic: String): Boolean = false
}
