package khipu.storage

import khipu.domain.BlockHeader
import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
import khipu.storage.datasource.BlockDataSource
import khipu.util.SimpleMapWithUnconfirmed
import scala.collection.mutable

/**
 * This class is used to store the BlockHeader, by using:
 *   Key: hash of the block to which the BlockHeader belong
 *   Value: the block header
 */
final class BlockHeaderStorage(val source: BlockDataSource, unconfirmedDepth: Int) extends SimpleMapWithUnconfirmed[Long, BlockHeader](unconfirmedDepth) {
  type This = BlockHeaderStorage

  def topic = source.topic

  override protected def doGet(key: Long): Option[BlockHeader] = {
    source.get(key).map(_.toBlockHeader)
  }

  override protected def doUpdate(toRemove: Iterable[Long], toUpsert: Iterable[(Long, BlockHeader)]): This = {
    val upsert = toUpsert map {
      case (key, value) => (value.number -> value.toBytes)
    }
    source.update(toRemove, upsert)
    this
  }

  def bestBlockNumber = unconfirmed.lastOption.flatMap(_.lastOption.map(_._1)).getOrElse(source.bestBlockNumber)
}

