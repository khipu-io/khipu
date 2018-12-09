package kesque

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable

object FIFOCache {
  final case class Entity[K, V](key: K, val value: V)

  // --- simple test
  def main(args: Array[String]) {
    val cache = new FIFOCache[Int, Int](10)
    (1 to 15) map (i => cache.put(i, i))
    var i = 0
    while (i <= 15) {
      if (i <= 5) {
        println(s"$i -> ${cache.get(i)}, should be None")
      } else {
        println(s"$i -> ${cache.get(i)}, should be Some($i)")
      }
      i += 1
    }
    println("size: " + cache.size)
  }
}
final class FIFOCache[K, V](val capacity: Int, threshold: Float = 0.9f) {
  import FIFOCache._

  assert(capacity > 0, "capacity must be positive!")
  assert(threshold > 0 && threshold < 1.0, "threshold must be in (0, 1)")

  private val pruneSize = math.max(1, (capacity / threshold - capacity).toInt)

  private val underlying = new mutable.LinkedHashMap[K, V]()

  private val cacheLock = new ReentrantReadWriteLock()
  private val readLock = cacheLock.readLock
  private val writeLock = cacheLock.writeLock

  private var _hitCount: Long = _
  private var _missCount: Long = _

  def hitRate = (_hitCount + _missCount) match {
    case 0 => 0.0
    case x => _hitCount * 1.0 / x
  }

  def missRate = (_hitCount + _missCount) match {
    case 0 => 0.0
    case x => _missCount * 1.0 / x
  }

  def put(key: K, value: V) {
    try {
      writeLock.lock()
      if (isFull) {
        pruneCache()
      }
      underlying.put(key, value)
    } finally {
      writeLock.unlock()
    }
  }

  def get(key: K): Option[V] = {
    try {
      readLock.lock()

      underlying.get(key) match {
        case None =>
          _missCount += 1
          None
        case some =>
          _hitCount += 1
          some
      }
    } finally {
      readLock.unlock()
    }
  }

  private def pruneCache(): Int = {
    var keys = List[K]()

    var count = 0
    val entries = underlying.iterator
    while (count < pruneSize && entries.hasNext) {
      keys ::= entries.next()._1
      count += 1
    }

    keys foreach underlying.remove
    count
  }

  def prune(): Int = {
    try {
      writeLock.lock()
      pruneCache()
    } finally {
      writeLock.unlock()
    }
  }

  private def isFull = underlying.size >= capacity

  def remove(keys: Seq[K]) {
    try {
      writeLock.lock()
      keys foreach underlying.remove
    } finally {
      writeLock.unlock()
    }
  }

  def remove(key: K) {
    try {
      writeLock.lock()
      underlying.remove(key)
    } finally {
      writeLock.unlock()
    }
  }

  def clear() {
    try {
      writeLock.lock()
      underlying.clear()
    } finally {
      writeLock.unlock()
    }
  }

  def resetHitRate() {
    _hitCount = 0
    _missCount = 0
  }

  def size = underlying.size

  def isEmpty = underlying.isEmpty

  override def toString = underlying.toString
}