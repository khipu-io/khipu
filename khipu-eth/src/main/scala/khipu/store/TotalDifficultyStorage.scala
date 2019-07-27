package khipu.store

import khipu.Hash
import khipu.TVal
import khipu.EvmWord
import khipu.store.datasource.BlockDataSource
import khipu.util.SimpleMap

/**
 * This class is used to store the total difficulty of blocks, by using:
 *   Key: hash of the block
 *   Value: the total difficulty
 */
final class TotalDifficultyStorage(storages: Storages, val source: BlockDataSource) extends SimpleMap[Hash, EvmWord] {
  type This = TotalDifficultyStorage

  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: EvmWord => Array[Byte] = _.bigEndianMag
  def valueDeserializer: Array[Byte] => EvmWord = EvmWord.safe

  override def get(key: Hash): Option[EvmWord] = {
    storages.getBlockNumberByHash(key) flatMap {
      blockNum => source.get(blockNum).map(x => EvmWord.safe(x.value))
    }
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, EvmWord]): TotalDifficultyStorage = {
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

  protected def apply(storages: Storages, dataSource: BlockDataSource): TotalDifficultyStorage = new TotalDifficultyStorage(storages, dataSource)
}

