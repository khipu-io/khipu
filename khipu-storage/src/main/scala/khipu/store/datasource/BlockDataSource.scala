package khipu.store.datasource

import khipu.util.Clock
import khipu.util.SimpleMap

trait BlockDataSource extends SimpleMap[Long, Array[Byte]] {
  type This <: BlockDataSource

  def topic: String

  def clock: Clock

  def count: Long
  def cacheHitRate: Double
  def cacheReadCount: Long
  def resetCacheHitRate(): Unit

  def close(): Unit
}
