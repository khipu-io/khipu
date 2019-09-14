package khipu.util

import scala.reflect.ClassTag
import scala.collection.mutable

object CircularArrayQueue {
  class EmptyCollectionException(message: String) extends Exception(message)

  // --- simple test
  def main(args: Array[String]) {
    val queue = new CircularArrayQueue[String](10)

    def printQueue() {
      print(queue)
      if (queue.nonEmpty) {
        println(": " + queue.head + " -> " + queue.last)
      } else {
        println
      }
    }

    println("\n--- enqueue")
    var i = 1
    while (i <= 15) {
      val v = i.toString
      println
      queue.enqueue(v)
      printQueue()
      i += 1
    }

    println("\n--- foreach")
    queue.foreach { x =>
      print(x + ",")
    }

    println("\n--- foldLeft")
    val sum = queue.foldLeft(0) {
      case (acc, x) =>
        println(acc)
        acc + x.toInt
    }
    println(s"sum $sum")

    println("\n--- dequeue")
    while (queue.nonEmpty) {
      println
      println(queue.dequeue)
      print(queue)
      printQueue()
    }

    println("\n--- clear")
    queue.clear()
    printQueue()

    println("\n--- enqueue")
    queue.enqueue("abcd")
    printQueue()

  }
}
class CircularArrayQueue[T: ClassTag](capacity: Int) {
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
      if (ret.isInstanceOf[AnyRef]) {
        queue(front) = null.asInstanceOf[T]
      }
      front = (front + 1) % queue.length
      count -= 1
      ret
    }
  }

  @throws(classOf[EmptyCollectionException])
  def head: T = {
    if (isEmpty) {
      throw new EmptyCollectionException("queue")
    } else {
      queue(front)
    }
  }

  def headOption: Option[T] = {
    if (isEmpty) {
      None
    } else {
      Some(queue(front))
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

  def lastOption: Option[T] = {
    if (isEmpty) {
      None
    } else {
      Some(queue(rear))
    }
  }

  def clear() {
    var i = 0
    var ptr = front
    while (i < count) {
      val elem = queue(ptr)
      if (elem.isInstanceOf[AnyRef]) {
        queue(ptr) = null.asInstanceOf[T]
      }
      ptr += 1
      if (ptr >= queue.length) {
        ptr = queue.length - ptr
      }
      i += 1
    }

    count = 0
    front = 0
    rear = -1
  }

  def size = count
  def isFull = count == capacity
  def isEmpty = count == 0
  def nonEmpty = !isEmpty

  def toArray: Array[T] = {
    val ret = Array.ofDim[T](count)

    var i = 0
    var ptr = front
    while (i < count) {
      ret(i) = queue(ptr)
      ptr += 1
      if (ptr >= queue.length) {
        ptr = queue.length - ptr
      }
      i += 1
    }

    ret
  }

  def foreach(f: (T) => Unit) {
    var i = 0
    var ptr = front
    while (i < count) {
      val elem = queue(ptr)
      f(elem)
      ptr += 1
      if (ptr >= queue.length) {
        ptr = queue.length - ptr
      }
      i += 1
    }
  }

  def foldLeft[B](z: B)(op: (B, T) => B): B = {
    var ret = z

    var i = 0
    var ptr = front
    while (i < count) {
      val elem = queue(ptr)
      ret = op(ret, elem)
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

class KeyValueCircularArrayQueue[K, V](val capacity: Int) extends CircularArrayQueue[Iterable[(K, V)]](capacity) {
  private val cache = mutable.HashMap[K, V]()

  override def enqueue(element: Iterable[(K, V)]) {
    super.enqueue(element)
    element foreach { kv => cache += kv }
  }

  override def dequeue(): Iterable[(K, V)] = {
    val element = super.dequeue()
    element foreach { kv => cache.remove(kv._1) }
    element
  }

  override def clear() {
    super.clear()
    cache.clear()
  }

  def get(key: K): Option[V] = cache.get(key)
}
