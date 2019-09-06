package khipu.storage

import akka.util.ByteString
import java.nio.ByteOrder
import khipu.Hash
import khipu.storage.datasource.KeyValueDataSource
import khipu.util.SimpleMapWithUnconfirmed

object TransactionStorage {
  final case class TxLocation(blockNumber: Long, txIndex: Int)
}
import TransactionStorage._
final class TransactionStorage(val source: KeyValueDataSource, unconfirmedDepth: Int) extends SimpleMapWithUnconfirmed[Hash, TxLocation](unconfirmedDepth) {
  type This = TransactionStorage

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  private val namespace = Namespaces.Transaction

  def topic = source.topic

  private def keyToBytes(k: Hash): Array[Byte] = k.bytes
  private def valueToBytes(v: TxLocation): Array[Byte] = {
    val builder = ByteString.newBuilder

    builder.putLong(v.blockNumber)
    builder.putInt(v.txIndex)

    builder.result.toArray
  }
  private def valueFromBytes(bytes: Array[Byte]): TxLocation = {
    val data = ByteString(bytes).iterator

    val blockNumber = data.getLong
    val txIndex = data.getInt

    TxLocation(blockNumber, txIndex)
  }

  override protected def getFromSource(key: Hash): Option[TxLocation] =
    source.get(namespace, keyToBytes(key)).map(valueFromBytes)

  override protected def updateToSource(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, TxLocation)]): This = {
    val remove = toRemove.map(keyToBytes)
    val upsert = toUpsert.map { case (k, v) => keyToBytes(k) -> valueToBytes(v) }
    source.update(namespace, remove, upsert)
    this
  }
}

