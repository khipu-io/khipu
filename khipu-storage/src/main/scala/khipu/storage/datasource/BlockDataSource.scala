package khipu.storage.datasource

import khipu.util.Clock
import khipu.util.SimpleMap

trait BlockDataSource[K, V] extends SimpleMap[K, V] {
  type This <: BlockDataSource[K, V]

  def topic: String

  def clock: Clock

  def count: Long
  def cacheHitRate: Double
  def cacheReadCount: Long
  def resetCacheHitRate(): Unit

  def terminate(): Unit
}
