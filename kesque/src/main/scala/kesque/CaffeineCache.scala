package kesque

import com.github.benmanes.caffeine.cache.Caffeine

final class CaffeineCache[K, V](capacity: Int, isRecordingStats: Boolean = false) {
  private val underlying = {
    val caffeine = if (isRecordingStats) {
      Caffeine.newBuilder().asInstanceOf[Caffeine[K, V]].recordStats()
    } else {
      Caffeine.newBuilder().asInstanceOf[Caffeine[K, V]]
    }

    caffeine.maximumSize(capacity).build[K, V]()
  }

  def put(key: K, value: V): Unit = underlying.put(key, value)
  def get(key: K): Option[V] = Option(underlying.getIfPresent(key))
  def remove(key: K): Unit = underlying.invalidate(key)
  def remove(keys: Seq[K]): Unit = keys foreach underlying.invalidate

  def hitRate = underlying.stats.hitRate
  def missRate = underlying.stats.missRate
}
