package com.box.castle.core.common

import org.specs2.mutable.Specification

/**
  * Unit test for bounded queue
  */
class BoundedQueueTest extends Specification{

 "BoundedQueue" should {
   "keep fixed number of elements in the queue" in {
     // Setup
     val capacity = 5
     val queue = new BoundedQueue[Int](capacity)

     // Execute
     queue ++= List(1,2,3,4,5)
     queue.enqueue(10,20)

     // Validate
     queue.size shouldEqual capacity
     queue.isFull shouldEqual true
     queue.front shouldEqual 3
     queue.last shouldEqual 20

   }
 }

}
