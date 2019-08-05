package khipu.store

import khipu.Hash
import khipu.TVal
import khipu.network.p2p.messages.PV62.BlockBody
import khipu.store.datasource.BlockDataSource
import khipu.util.SimpleMap
import scala.collection.mutable

/**
 * This class is used to store the BlockBody, by using:
 *   Key: hash of the block to which the BlockBody belong
 *   Value: the block body
 */
final class BlockBodyStorage(storages: Storages, val source: BlockDataSource) extends SimpleMap[Hash, BlockBody] {
  type This = BlockBodyStorage

  import BlockBody.BlockBodyDec

  override def get(key: Hash): Option[BlockBody] = {
    storages.getBlockNumberByHash(key) flatMap {
      blockNum => source.get(blockNum).map(_.value.toBlockBody)
    }
  }

  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, BlockBody)]): BlockBodyStorage = {
    val upsert = toUpsert flatMap {
      case (key, value) =>
        storages.getBlockNumberByHash(key) map {
          blockNum => (blockNum -> TVal(value.toBytes, -1, blockNum))
        }
    }
    val remove = toRemove flatMap {
      key => storages.getBlockNumberByHash(key)
    }
    source.update(remove, upsert)
    this
  }
}
