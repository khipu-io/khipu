package khipu.store

import kesque.TVal
import khipu.Hash
import khipu.domain.Receipt
import khipu.ledger.BloomFilter
import khipu.rlp
import khipu.rlp.RLPEncodeable
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.store.datasource.KesqueDataSource
import khipu.util.SimpleMap

object ReceiptsStorage {

  /**
   * Calculate logsBloomFilter on the fly from logs instead of putting it in store.
   * logsBloomFilter is 256 bytes
   */
  object ReceiptsSerializer {

    def toBytes(receipts: Seq[Receipt]): Array[Byte] =
      rlp.encode(toRLPEncodable(receipts))

    def toReceipts(bytes: Array[Byte]): Seq[Receipt] = rlp.rawDecode(bytes) match {
      case RLPList(items @ _*) => items.map(toReceipt)
      case _                   => throw new RuntimeException("Cannot decode Receipts")
    }

    private def toRLPEncodable(receipts: Seq[Receipt]): RLPEncodeable =
      RLPList(receipts.map(toRLPEncodable): _*)

    private def toRLPEncodable(receipt: Receipt): RLPEncodeable = receipt match {
      case Receipt(postTxState, cumulativeGasUsed, _logsBloomFilter, logs) =>
        import khipu.network.p2p.messages.PV63.TxLogEntryImplicits._
        RLPList(postTxState, cumulativeGasUsed, RLPList(logs.map(_.toRLPEncodable): _*))
    }

    private def toReceipt(rlpEncodeable: RLPEncodeable): Receipt = rlpEncodeable match {
      case RLPList(postTxState, cumulativeGasUsed, logs: RLPList) =>
        import khipu.network.p2p.messages.PV63.TxLogEntryImplicits._
        val txLogs = logs.items.map(_.toTxLogEntry)
        val logsBloomFilter = BloomFilter.create(txLogs)
        Receipt(postTxState, cumulativeGasUsed, logsBloomFilter, txLogs)
      case _ =>
        throw new RuntimeException("Cannot decode Receipt")
    }

    private def toReceipt(bytes: Array[Byte]): Receipt =
      toReceipt(rlp.rawDecode(bytes))
  }

}
/**
 * This class is used to store the Receipts, by using:
 *   Key: hash of the block to which the list of receipts belong
 *   Value: the list of receipts
 */
final class ReceiptsStorage(val source: KesqueDataSource) extends SimpleMap[Hash, Seq[Receipt], ReceiptsStorage] {
  import ReceiptsStorage.ReceiptsSerializer._

  val namespace: Array[Byte] = Namespaces.ReceiptsNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: Seq[Receipt] => Array[Byte] = toBytes
  def valueDeserializer: Array[Byte] => Seq[Receipt] = toReceipts

  override def get(key: Hash): Option[Seq[Receipt]] = {
    source.get(key).map(x => toReceipts(x.value))
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, Seq[Receipt]]): ReceiptsStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    //toUpsert foreach { case (key, value) => nodeTable.put(key, () => Future(value)) }
    toUpsert foreach { case (key, value) => source.put(key, TVal(toBytes(value), -1L)) }
    toRemove foreach { key => source.remove(key) }
    this
  }

  protected def apply(source: KesqueDataSource): ReceiptsStorage = new ReceiptsStorage(source)
}

