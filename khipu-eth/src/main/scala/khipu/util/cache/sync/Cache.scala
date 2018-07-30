package khipu.util.cache.sync

import scala.collection.JavaConverters._
import scala.collection.immutable

/**
 * General interface implemented by all akka-http cache implementations.
 */
abstract class Cache[K, V] { cache =>

  /**
   * Returns either the cached Future for the given key or evaluates the given value generating
   * function producing a `Future[V]`.
   */
  def apply(key: K, genValue: () => V): V

  /**
   * Returns either the cached for the given key, or applies the given value loading
   * function on the key, producing a `Future[V]`.
   */
  def getOrLoad(key: K, loadValue: K => V): V

  /**
   * Returns either the cached for the given key or the given value
   */
  def get(key: K, block: () => V): V =
    cache.apply(key, () => block())

  /**
   * Retrieves the future instance that is currently in the cache for the given key.
   * Returns None if the key has no corresponding cache entry.
   */
  def get(key: K): Option[V]

  def put(key: K, value: V): Unit

  /**
   * Removes the cache item for the given key.
   */
  def remove(key: K): Unit

  /**
   * Clears the cache by removing all entries.
   */
  def clear(): Unit

  /**
   * Returns the set of keys in the cache, in no particular order
   * Should return in roughly constant time.
   * Note that this number might not reflect the exact keys of active, unexpired
   * cache entries, since expired entries are only evicted upon next access
   * (or by being thrown out by a capacity constraint).
   */
  def keys: immutable.Set[K]

  /**
   * Returns the upper bound for the number of currently cached entries.
   * Note that this number might not reflect the exact number of active, unexpired
   * cache entries, since expired entries are only evicted upon next access
   * (or by being thrown out by a capacity constraint).
   */
  def size(): Int

  final def +(kv: (K, V)) = put(kv._1, kv._2)
  final def -(key: K) = remove(key)
}

