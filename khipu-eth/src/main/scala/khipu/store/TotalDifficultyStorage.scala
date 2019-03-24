package khipu.store

import kesque.TVal
import khipu.Hash
import khipu.UInt256
import khipu.store.datasource.KesqueDataSource
import khipu.util.SimpleMap

/**
 * This class is used to store the total difficulty of blocks, by using:
 *   Key: hash of the block
 *   Value: the total difficulty
 */
final class TotalDifficultyStorage(val source: KesqueDataSource) extends SimpleMap[Hash, UInt256, TotalDifficultyStorage] {

  val namespace: Array[Byte] = Namespaces.TotalDifficultyNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: UInt256 => Array[Byte] = _.bigEndianMag
  def valueDeserializer: Array[Byte] => UInt256 = UInt256.safe

  override def get(key: Hash): Option[UInt256] = {
    source.get(key).map(x => UInt256.safe(x.value))
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, UInt256]): TotalDifficultyStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    //toUpsert foreach { case (key, value) => nodeTable.put(key, () => Future(value)) }
    toUpsert foreach { case (key, value) => source.put(key, TVal(value.bigEndianMag, -1, -1L)) }
    toRemove foreach { key => source.remove(key) }
    this
  }

  protected def apply(dataSource: KesqueDataSource): TotalDifficultyStorage = new TotalDifficultyStorage(dataSource)
}

