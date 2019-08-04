package khipu.util

import scala.reflect.ClassTag

object CircularArrayQueue {
  class EmptyCollectionException(message: String) extends Exception(message)

  // --- simple test
  def main(args: Array[String]) {
    val queue = new CircularArrayQueue[String](10)

    println("\n--- enqueue")
    var i = 1
    while (i <= 15) {
      val v = i.toString
      println
      queue.enqueue(v)
      println(queue + ": " + queue.first + " -> " + queue.last)
      i += 1
    }

    println("\n--- dequeue")
    while (queue.nonEmpty) {
      println
      println(queue.dequeue)
      print(queue)
      if (queue.nonEmpty) {
        println(": " + queue.first + " -> " + queue.last)
      } else {
        println
      }
    }
  }
}
final class CircularArrayQueue[T](capacity: Int)(implicit _m: ClassTag[T]) {
  import CircularArrayQueue._

  private var count, front = 0
  private var rear = -1
  private val queue: Array[T] = Array.ofDim[T](capacity)

  def enqueue(element: T) {
    rear = (rear + 1) % queue.length
    queue(rear) = element

    if (count < queue.length) {
      count += 1
    } else {
      front = (front + 1) % queue.length
    }
  }

  @throws(classOf[EmptyCollectionException])
  def dequeue(): T = {
    if (isEmpty) {
      throw new EmptyCollectionException("queue")
    } else {
      val ret = queue(front)
      ret match {
        case _: AnyRef => queue(front) = null.asInstanceOf[T]
        case _         =>
      }
      front = (front + 1) % queue.length
      count -= 1
      ret
    }
  }

  @throws(classOf[EmptyCollectionException])
  def first: T = {
    if (isEmpty) {
      throw new EmptyCollectionException("queue")
    } else {
      queue(front)
    }
  }

  @throws(classOf[EmptyCollectionException])
  def last: T = {
    if (isEmpty) {
      throw new EmptyCollectionException("queue")
    } else {
      queue(rear)
    }
  }

  def size = count
  def isEmpty = count == 0
  def nonEmpty = !isEmpty

  def toArray: Array[T] = {
    val ret = Array.ofDim[T](count)

    var i = 0
    var ptr = front
    while (i < count) {
      ret(i) = (queue(ptr))
      ptr += 1
      if (ptr >= queue.length) {
        ptr = queue.length - ptr
      }
      i += 1
    }

    ret
  }

  override def toString() = toArray.mkString("(", ",", ")")
}
