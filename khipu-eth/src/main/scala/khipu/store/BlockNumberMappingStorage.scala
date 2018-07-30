package khipu.store

import java.math.BigInteger
import khipu.Hash
import khipu.store.datasource.DataSource
import khipu.util.cache.sync.Cache

class BlockNumberMappingStorage(val source: DataSource, protected val cache: Cache[Long, Hash]) extends CachedKeyValueStorage[Long, Hash, BlockNumberMappingStorage] {

  val namespace: Array[Byte] = Namespaces.HeightsNamespace
  def keySerializer: Long => Array[Byte] = index => BigInteger.valueOf(index).toByteArray
  def valueSerializer: Hash => Array[Byte] = _.bytes
  def valueDeserializer: Array[Byte] => Hash = bytes => Hash(bytes)

  protected def apply(dataSource: DataSource): BlockNumberMappingStorage = new BlockNumberMappingStorage(dataSource, cache)
}
