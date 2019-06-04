package khipu.store

import akka.util.ByteString
import khipu.Hash
import khipu.store.datasource.DataSource
import khipu.util.SimpleMap

/**
 * This class is used to store the EVM Code, by using:
 *   Key: hash of the code
 *   Value: the code
 */
final class EvmCodeStorage(val source: DataSource) extends SimpleMap[Hash, ByteString] {
  type This = EvmCodeStorage

  val namespace: Array[Byte] = Array[Byte]()
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: ByteString => Array[Byte] = _.toArray
  def valueDeserializer: Array[Byte] => ByteString = (code: Array[Byte]) => ByteString(code)

  override def get(key: Hash): Option[ByteString] = {
    source.get(namespace, key.bytes).map(x => ByteString(x))
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, ByteString]): EvmCodeStorage = {
    val upsert = toUpsert map { case (key, value) => (key.bytes -> value.toArray) }
    val remove = toRemove map { key => key.bytes }
    source.update(namespace, remove, upsert)
    this
  }

  protected def apply(source: DataSource): EvmCodeStorage = new EvmCodeStorage(source)
}

