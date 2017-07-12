package com.box.castle.core.leader

import com.box.castle.committer.api.TopicFilter

/**
  * Created by bravishanker on 7/11/17.
  * Returns false for login and true for all other topics
  */
class SplitTopicFilter extends TopicFilter {
  override def matches(topic: String): Boolean = {
    if(topic.equals("login")) false else true
  }

}
