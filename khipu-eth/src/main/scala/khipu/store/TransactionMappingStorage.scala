package khipu.store

//import akka.util.ByteString
import boopickle.Default
import khipu.Hash
import khipu.store.datasource.DataSource
import khipu.util.BytesUtil.compactPickledBytes
import java.nio.ByteBuffer

object TransactionMappingStorage {
  final case class TransactionLocation(blockHash: Hash, txIndex: Int)
}
import TransactionMappingStorage._
final class TransactionMappingStorage(val source: DataSource) extends KeyValueStorage[Hash, TransactionLocation, TransactionMappingStorage] {
  import boopickle.Default._

  val namespace: Array[Byte] = Namespaces.TransactionMappingNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: TransactionLocation => Array[Byte] = tl => compactPickledBytes(Default.Pickle.intoBytes(tl)).toArray
  def valueDeserializer: Array[Byte] => TransactionLocation = bytes => Default.Unpickle[TransactionLocation].fromBytes(ByteBuffer.wrap(bytes))

  implicit val HashKeyPickler: Default.Pickler[Hash] = Default.transformPickler[Hash, Array[Byte]](Hash(_))(_.bytes)
  //implicit val byteStringPickler: Default.Pickler[ByteString] = Default.transformPickler[ByteString, Array[Byte]](ByteString(_))(_.toArray)

  protected def apply(dataSource: DataSource): TransactionMappingStorage = new TransactionMappingStorage(dataSource)
}

