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

  private val cacheMap = new mutable.LinkedHashMap[K, V]()

  private val cacheLock = new ReentrantReadWriteLock()
  private val readLock = cacheLock.readLock
  private val writeLock = cacheLock.writeLock

  private var _hitCount: Int = _
  def hitCount = _hitCount

  private var _missCount: Int = _
  def missCount = _missCount

  def put(key: K, value: V) {
    try {
      writeLock.lock()
      if (isFull) {
        pruneCache()
      }
      cacheMap.put(key, value)
    } finally {
      writeLock.unlock()
    }
  }

  def get(key: K): Option[V] = {
    try {
      readLock.lock()

      cacheMap.get(key) match {
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
    val entries = cacheMap.iterator
    while (count < pruneSize && entries.hasNext) {
      keys ::= entries.next()._1
      count += 1
    }

    keys foreach cacheMap.remove
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

  private def isFull = cacheMap.size >= capacity

  def remove(keys: Seq[K]) {
    try {
      writeLock.lock()
      keys foreach cacheMap.remove
    } finally {
      writeLock.unlock()
    }
  }

  def remove(key: K) {
    try {
      writeLock.lock()
      cacheMap.remove(key)
    } finally {
      writeLock.unlock()
    }
  }

  def clear() {
    try {
      writeLock.lock()
      cacheMap.clear()
    } finally {
      writeLock.unlock()
    }
  }

  def size = cacheMap.size

  def isEmpty = cacheMap.isEmpty

  override def toString = cacheMap.toString
}