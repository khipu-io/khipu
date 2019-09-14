package khipu.storage

import khipu.DataWord
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

  override protected def getFromSource(key: Long): Option[BlockHeader] = {
    source.get(key).map(_.toBlockHeader)
  }

  override protected def updateToSource(toRemove: Iterable[Long], toUpsert: Iterable[(Long, BlockHeader)]): This = {
    val upsert = toUpsert map {
      case (key, value) => (value.number -> value.toBytes)
    }
    source.update(toRemove, upsert)
    this
  }

  def bestBlockNumber = unconfirmed.lastOption.flatMap(_.lastOption.map(_._1)).getOrElse(source.bestBlockNumber)

  def totalDifficultyOfUncomfirmed = unconfirmed.foldLeft(DataWord.Zero) {
    case (acc, header) => acc + header.lastOption.map(_._2.difficulty).fold(acc)(acc + _)
  }
}
