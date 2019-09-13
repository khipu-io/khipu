package khipu.storage

import java.nio.ByteBuffer
import khipu.Hash
import khipu.storage.datasource.KeyValueDataSource
import khipu.util.SimpleMapWithUnconfirmed
import scala.collection.mutable

/**
 * This class is used to store the blockhash -> blocknumber
 */
final class BlockNumberStorage(val source: KeyValueDataSource, unconfirmedDepth: Int) extends SimpleMapWithUnconfirmed[Hash, Long](unconfirmedDepth) {
  type This = BlockNumberStorage

  def topic = source.topic

  private def keyToBytes(k: Hash): Array[Byte] = k.bytes
  private def valueToBytes(v: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(v).array
  private def valueFromBytes(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong

  override protected def getFromSource(key: Hash): Option[Long] = {
    source.get(keyToBytes(key)).map(valueFromBytes)
  }

  override protected def updateToSource(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Long)]): This = {
    val remove = toRemove map { key => keyToBytes(key) }
    val upsert = toUpsert map {
      case (key, value) => (keyToBytes(key) -> valueToBytes(value))
    }
    source.update(remove, upsert)
    this
  }

  def count = source.count
}

