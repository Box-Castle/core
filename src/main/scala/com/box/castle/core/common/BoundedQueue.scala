package com.box.castle.core.common

import scala.collection.mutable

/**
  * Queue with a fixed upper bound on capacity.
  *
  * NOTE: This data structure is not thread safe.
  */
class BoundedQueue[T](capacity: Int) extends mutable.Queue[T] {

  override def +=(elem: T): this.type = {
    super.+=(elem)
    if (super.size > capacity)
      super.dequeue()
    this
  }

  override def ++=(xs: TraversableOnce[T]): BoundedQueue.this.type = {
    xs.foreach(this += _)
    this
  }

  override def +=(elem1: T, elem2: T, elems: T*): BoundedQueue.this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def enqueue(elems: T*): Unit = {
    this ++= elems
  }

  def isFull: Boolean = {
    super.size == capacity
  }

}
