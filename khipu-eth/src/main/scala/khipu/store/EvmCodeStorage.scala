package khipu.store

import akka.util.ByteString
import kesque.TVal
import khipu.Hash
import khipu.store.datasource.HeavyDataSource
import khipu.util.SimpleMap

/**
 * This class is used to store the EVM Code, by using:
 *   Key: hash of the code
 *   Value: the code
 */
final class EvmCodeStorage(val source: HeavyDataSource) extends SimpleMap[Hash, ByteString] {
  type This = EvmCodeStorage

  val namespace: Array[Byte] = Namespaces.CodeNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: ByteString => Array[Byte] = _.toArray
  def valueDeserializer: Array[Byte] => ByteString = (code: Array[Byte]) => ByteString(code)

  override def get(key: Hash): Option[ByteString] = {
    source.get(key).map(x => ByteString(x.value))
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, ByteString]): EvmCodeStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    //toUpsert foreach { case (key, value) => nodeTable.put(key, () => Future(value)) }
    toUpsert foreach { case (key, value) => source.put(key, TVal(value.toArray, -1, -1L)) }
    toRemove foreach { key => source.remove(key) }
    this
  }

  def setWritingBlockNumber(writingBlockNumber: Long) = source.setWritingTimestamp(writingBlockNumber)

  protected def apply(source: HeavyDataSource): EvmCodeStorage = new EvmCodeStorage(source)
}

