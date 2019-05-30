package khipu.store.datasource

import kesque.TVal
import khipu.Hash
import khipu.util.Clock
import khipu.util.SimpleMap

trait HeavyDataSource extends SimpleMap[Hash, TVal] {
  type This <: HeavyDataSource

  def topic: String

  def clock: Clock

  def setWritingTimestamp(time: Long)

  def getKeyByTimestamp(time: Long): Option[Hash]
  def putTimestampToKey(time: Long, key: Hash)

  def cacheHitRate: Double
  def cacheReadCount: Long
  def resetCacheHitRate(): Unit
}
