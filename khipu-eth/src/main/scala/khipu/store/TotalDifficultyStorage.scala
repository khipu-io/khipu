package khipu.store

import khipu.DataWord
import khipu.Hash
import khipu.TVal
import khipu.store.datasource.BlockDataSource
import khipu.util.SimpleMap

/**
 * This class is used to store the total difficulty of blocks, by using:
 *   Key: hash of the block
 *   Value: the total difficulty
 */
final class TotalDifficultyStorage(storages: Storages, val source: BlockDataSource) extends SimpleMap[Hash, DataWord] {
  type This = TotalDifficultyStorage

  override def get(key: Hash): Option[DataWord] = {
    storages.getBlockNumberByHash(key) flatMap {
      blockNum => source.get(blockNum).map(x => DataWord.safe(x.value))
    }
  }

  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, DataWord)]): TotalDifficultyStorage = {
    val upsert = toUpsert flatMap {
      case (key, value) =>
        storages.getBlockNumberByHash(key) map {
          blockNum => (blockNum -> TVal(value.bigEndianMag, -1, blockNum))
        }
    }
    val remove = toRemove flatMap {
      key => storages.getBlockNumberByHash(key)
    }
    source.update(remove, upsert)
    this
  }
}

