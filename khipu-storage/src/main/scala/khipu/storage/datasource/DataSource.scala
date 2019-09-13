package khipu.storage.datasource

import khipu.util.Clock
import khipu.util.SimpleMap

trait DataSource[K, V] extends SimpleMap[K, V] {
  val clock = new Clock()

  def count: Long

  def cacheHitRate: Double
  def cacheReadCount: Long
  def resetCacheHitRate(): Unit

  def stop(): Unit
}
