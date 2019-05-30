package khipu.store

import akka.util.ByteString
import java.nio.ByteOrder
import khipu.Hash
import khipu.store.datasource.DataSource

object TransactionMappingStorage {
  final case class TransactionLocation(blockHash: Hash, txIndex: Int)
}
import TransactionMappingStorage._
final class TransactionMappingStorage(val source: DataSource) extends KeyValueStorage[Hash, TransactionLocation] {
  type This = TransactionMappingStorage

  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  val namespace: Array[Byte] = Namespaces.TransactionMappingNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes

  override def valueSerializer: TransactionLocation => Array[Byte] = tl => {
    val builder = ByteString.newBuilder

    val hashBytes = tl.blockHash.bytes
    builder.putInt(hashBytes.length)
    builder.putBytes(hashBytes)
    builder.putInt(tl.txIndex)

    builder.result.toArray
  }

  override def valueDeserializer: Array[Byte] => TransactionLocation = bytes => {
    val data = ByteString(bytes).iterator

    val hashLength = data.getInt
    val blockHash = Hash(data.getBytes(hashLength))
    val txIndex = data.getInt

    TransactionLocation(blockHash, txIndex)
  }

  protected def apply(dataSource: DataSource): TransactionMappingStorage = new TransactionMappingStorage(dataSource)
}

