package khipu.store.datasource

import kesque.TVal
import khipu.Hash
import khipu.util.Clock
import khipu.util.SimpleMap

trait NodeDataSource extends SimpleMap[Hash, TVal] {
  type This <: NodeDataSource

  def topic: String

  def clock: Clock

  def count: Long
  def cacheHitRate: Double
  def cacheReadCount: Long
  def resetCacheHitRate(): Unit

  def close(): Unit
}
