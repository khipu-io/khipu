package khipu.store

import akka.actor.ActorSystem
import akka.event.Logging
import kesque.TVal
import khipu.Hash
import khipu.domain.BlockHeader
import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
import khipu.store.datasource.BlockDataSource
import khipu.store.datasource.LmdbBlockDataSource
import khipu.util.SimpleMap
import scala.collection.mutable

/**
 * This class is used to store the BlockHeader, by using:
 *   Key: hash of the block to which the BlockHeader belong
 *   Value: the block header
 */
final class BlockHeaderStorage(val source: BlockDataSource)(implicit system: ActorSystem) extends SimpleMap[Hash, BlockHeader] {
  type This = BlockHeaderStorage

  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: BlockHeader => Array[Byte] = _.toBytes
  def valueDeserializer: Array[Byte] => BlockHeader = b => b.toBlockHeader

  private val log = Logging(system, this.getClass)

  private val lmdbSource = source.asInstanceOf[LmdbBlockDataSource]
  private val env = lmdbSource.env
  private val table = lmdbSource.table

  override def get(key: Hash): Option[BlockHeader] = {
    LmdbBlockDataSource.getTimestampByKey(key) flatMap {
      blockNum => source.get(blockNum).map(_.value.toBlockHeader)
    }
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, BlockHeader]): BlockHeaderStorage = {
    val upsert = toUpsert map {
      case (key, value) =>
        LmdbBlockDataSource.putTimestampToKey(value.number, key)
        (value.number -> TVal(value.toBytes, -1, value.number))
    }
    val remove = toRemove flatMap {
      key =>
        val blockNum = LmdbBlockDataSource.getTimestampByKey(key)
        LmdbBlockDataSource.removeTimestamp(key)
        blockNum
    }
    source.update(remove, upsert)
    this
  }

  def getBlockNumber(hash: Hash) = LmdbBlockDataSource.getTimestampByKey(hash)
  def getBlockHash(blockNumber: Long) = LmdbBlockDataSource.getKeyByTimestamp(blockNumber)
  def getBlockHashs(from: Long, to: Long) = LmdbBlockDataSource.getKeysByTimestamp(from, to)

  protected def apply(source: BlockDataSource): BlockHeaderStorage = new BlockHeaderStorage(source)
}

