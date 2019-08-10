package khipu.store

import akka.util.ByteString
import java.nio.ByteOrder
import khipu.Hash
import khipu.store.datasource.DataSource
import khipu.util.SimpleMapWithUnconfirmed

object TransactionStorage {
  final case class TxLocation(blockHash: Hash, txIndex: Int)
}
import TransactionStorage._
final class TransactionStorage(val source: DataSource, unconfirmedDepth: Int) extends SimpleMapWithUnconfirmed[Hash, TxLocation](unconfirmedDepth) {
  type This = TransactionStorage

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  private val namespace = Namespaces.Transaction

  private def keyToBytes(k: Hash): Array[Byte] = k.bytes
  private def valueToBytes(v: TxLocation): Array[Byte] = {
    val builder = ByteString.newBuilder

    val hashBytes = v.blockHash.bytes
    builder.putInt(hashBytes.length)
    builder.putBytes(hashBytes)
    builder.putInt(v.txIndex)

    builder.result.toArray
  }
  private def valueFromBytes(bytes: Array[Byte]): TxLocation = {
    val data = ByteString(bytes).iterator

    val hashLength = data.getInt
    val blockHash = Hash(data.getBytes(hashLength))
    val txIndex = data.getInt

    TxLocation(blockHash, txIndex)
  }

  override protected def doGet(key: Hash): Option[TxLocation] =
    source.get(namespace, keyToBytes(key)).map(valueFromBytes)

  override protected def doUpdate(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, TxLocation)]): This = {
    val remove = toRemove.map(keyToBytes)
    val upsert = toUpsert.map { case (k, v) => keyToBytes(k) -> valueToBytes(v) }
    source.update(namespace, remove, upsert)
    this
  }
}

