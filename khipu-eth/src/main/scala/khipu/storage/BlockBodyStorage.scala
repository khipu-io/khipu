package khipu.storage

import khipu.network.p2p.messages.PV62.BlockBody
import khipu.storage.datasource.BlockDataSource
import khipu.util.SimpleMapWithUnconfirmed
import scala.collection.mutable

/**
 * This class is used to store the BlockBody, by using:
 *   Key: hash of the block to which the BlockBody belong
 *   Value: the block body
 */
final class BlockBodyStorage(val source: BlockDataSource, unconfirmedDepth: Int) extends SimpleMapWithUnconfirmed[Long, BlockBody](unconfirmedDepth) {
  type This = BlockBodyStorage

  import BlockBody.BlockBodyDec

  def topic = source.topic

  protected def getFromSource(key: Long): Option[BlockBody] = {
    source.get(key).map(_.toBlockBody)
  }

  protected def updateToSource(toRemove: Iterable[Long], toUpsert: Iterable[(Long, BlockBody)]): This = {
    val upsert = toUpsert map {
      case (key, value) => (key -> value.toBytes)
    }
    source.update(toRemove, upsert)
    this
  }

  def bestBlockNumber = unconfirmed.lastOption.flatMap(_.lastOption.map(_._1)).getOrElse(source.bestBlockNumber)
}
