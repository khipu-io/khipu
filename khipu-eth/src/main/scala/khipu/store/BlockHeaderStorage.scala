package khipu.store

import kesque.TVal
import khipu.Hash
import khipu.domain.BlockHeader
import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
import khipu.store.datasource.KesqueDataSource
import khipu.util.SimpleMap
import scala.collection.mutable

/**
 * This class is used to store the BlockHeader, by using:
 *   Key: hash of the block to which the BlockHeader belong
 *   Value: the block header
 */
final class BlockHeaderStorage(val source: KesqueDataSource) extends SimpleMap[Hash, BlockHeader, BlockHeaderStorage] {
  val namespace: Array[Byte] = Namespaces.HeaderNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: BlockHeader => Array[Byte] = _.toBytes
  def valueDeserializer: Array[Byte] => BlockHeader = b => b.toBlockHeader

  override def get(key: Hash): Option[BlockHeader] = {
    source.get(key).map(_.value.toBlockHeader)
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, BlockHeader]): BlockHeaderStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    //toUpsert foreach { case (key, value) => nodeTable.put(key, () => Future(value)) }
    toUpsert foreach { case (key, value) => source.put(key, TVal(value.toBytes, -1L)) }
    toRemove foreach { key => source.remove(key) }
    this
  }

  def setWritingBlockNumber(writingBlockNumber: Long) = source.setWritingBlockNumber(writingBlockNumber)

  def getBlockHash(blockNumber: Long) = source.table.getKeyByTime(blockNumber).map(Hash(_))
  def putBlockHash(blockNumber: Long, key: Hash) = source.table.putTimeToKey(blockNumber, key.bytes)

  protected def apply(source: KesqueDataSource): BlockHeaderStorage = new BlockHeaderStorage(source)
}

