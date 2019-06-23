package khipu.store.datasource

import kesque.TVal
import khipu.util.Clock
import khipu.util.SimpleMap

trait BlockDataSource extends SimpleMap[Long, TVal] {
  type This <: BlockDataSource

  def topic: String

  def clock: Clock

  def count: Long
  def cacheHitRate: Double
  def cacheReadCount: Long
  def resetCacheHitRate(): Unit

  def close(): Unit
}
