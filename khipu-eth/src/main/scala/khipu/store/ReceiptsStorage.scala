package khipu.store

import kesque.TVal
import khipu.Hash
import khipu.domain.Receipt
import khipu.store.datasource.KesqueDataSource
import khipu.util.SimpleMap

/**
 * This class is used to store the Receipts, by using:
 *   Key: hash of the block to which the list of receipts belong
 *   Value: the list of receipts
 */
final class ReceiptsStorage(val source: KesqueDataSource) extends SimpleMap[Hash, Seq[Receipt], ReceiptsStorage] {
  import khipu.network.p2p.messages.PV63.ReceiptImplicits._

  val namespace: Array[Byte] = Namespaces.ReceiptsNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: Seq[Receipt] => Array[Byte] = _.toBytes
  def valueDeserializer: Array[Byte] => Seq[Receipt] = b => b.toReceipts

  override def get(key: Hash): Option[Seq[Receipt]] = {
    source.get(key).map(_.value.toReceipts)
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, Seq[Receipt]]): ReceiptsStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    //toUpsert foreach { case (key, value) => nodeTable.put(key, () => Future(value)) }
    toUpsert foreach { case (key, value) => source.put(key, TVal(value.toBytes, -1L)) }
    toRemove foreach { key => source.remove(key) }
    this
  }

  protected def apply(source: KesqueDataSource): ReceiptsStorage = new ReceiptsStorage(source)
}

