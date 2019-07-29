package khipu.store

import khipu.Hash
import khipu.TVal
import khipu.domain.Receipt
import khipu.ledger.BloomFilter
import khipu.rlp
import khipu.rlp.RLPEncodeable
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.store.datasource.BlockDataSource
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
final class ReceiptsStorage(storages: Storages, val source: BlockDataSource) extends SimpleMap[Hash, Seq[Receipt]] {
  type This = ReceiptsStorage

  import ReceiptsStorage.ReceiptsSerializer._

  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: Seq[Receipt] => Array[Byte] = toBytes
  def valueDeserializer: Array[Byte] => Seq[Receipt] = toReceipts

  override def get(key: Hash): Option[Seq[Receipt]] = {
    storages.getBlockNumberByHash(key) flatMap {
      blockNum => source.get(blockNum).map(x => toReceipts(x.value))
    }
  }

  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Seq[Receipt])]): ReceiptsStorage = {
    val upsert = toUpsert flatMap {
      case (key, value) =>
        storages.getBlockNumberByHash(key) map {
          blockNum => (blockNum -> TVal(toBytes(value), -1, blockNum))
        }
    }
    val remove = toRemove flatMap {
      key => storages.getBlockNumberByHash(key)
    }
    source.update(remove, upsert)
    this
  }

  protected def apply(storages: Storages, source: BlockDataSource): ReceiptsStorage = new ReceiptsStorage(storages, source)
}

