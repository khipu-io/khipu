package khipu.store

import kesque.TVal
import khipu.Hash
import khipu.network.p2p.messages.PV62.BlockBody
import khipu.store.datasource.KesqueDataSource
import khipu.util.SimpleMap
import scala.collection.mutable

/**
 * This class is used to store the BlockBody, by using:
 *   Key: hash of the block to which the BlockBody belong
 *   Value: the block body
 */
final class BlockBodyStorage(val source: KesqueDataSource) extends SimpleMap[Hash, BlockBody, BlockBodyStorage] {
  import BlockBody.BlockBodyDec

  val namespace: Array[Byte] = Namespaces.BodyNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: BlockBody => Array[Byte] = _.toBytes
  def valueDeserializer: Array[Byte] => BlockBody = b => b.toBlockBody

  override def get(key: Hash): Option[BlockBody] = {
    source.get(key).map(_.value.toBlockBody)
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, BlockBody]): BlockBodyStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    //toUpsert foreach { case (key, value) => nodeTable.put(key, () => Future(value)) }
    toUpsert foreach { case (key, value) => source.put(key, TVal(value.toBytes, -1, -1L)) }
    toRemove foreach { key => source.remove(key) }
    this
  }

  def setWritingBlockNumber(writingBlockNumber: Long) = source.setWritingBlockNumber(writingBlockNumber)

  protected def apply(source: KesqueDataSource): BlockBodyStorage = new BlockBodyStorage(source)
}
