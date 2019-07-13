package khipu.store

import khipu.Hash
import khipu.TVal
import khipu.UInt256
import khipu.store.datasource.BlockDataSource
import khipu.util.SimpleMap

/**
 * This class is used to store the total difficulty of blocks, by using:
 *   Key: hash of the block
 *   Value: the total difficulty
 */
final class TotalDifficultyStorage(storages: Storages, val source: BlockDataSource) extends SimpleMap[Hash, UInt256] {
  type This = TotalDifficultyStorage

  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: UInt256 => Array[Byte] = _.bigEndianMag
  def valueDeserializer: Array[Byte] => UInt256 = UInt256.safe

  override def get(key: Hash): Option[UInt256] = {
    storages.getBlockNumberByHash(key) flatMap {
      blockNum => source.get(blockNum).map(x => UInt256.safe(x.value))
    }
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, UInt256]): TotalDifficultyStorage = {
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

