package khipu.store

import java.math.BigInteger
import kesque.TVal
import khipu.Hash
import khipu.store.datasource.KesqueDataSource
import khipu.util.SimpleMap

/**
 * This class is used to store the total difficulty of blocks, by using:
 *   Key: hash of the block
 *   Value: the total difficulty
 */
final class TotalDifficultyStorage(val source: KesqueDataSource) extends SimpleMap[Hash, BigInteger, TotalDifficultyStorage] {

  val namespace: Array[Byte] = Namespaces.TotalDifficultyNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: BigInteger => Array[Byte] = _.toByteArray
  def valueDeserializer: Array[Byte] => BigInteger = (valueBytes: Array[Byte]) => new BigInteger(1, valueBytes)

  override def get(key: Hash): Option[BigInteger] = {
    source.get(key).map(x => new BigInteger(1, x.value))
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, BigInteger]): TotalDifficultyStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    //toUpsert foreach { case (key, value) => nodeTable.put(key, () => Future(value)) }
    toUpsert foreach { case (key, value) => source.put(key, TVal(value.toByteArray, -1L)) }
    toRemove foreach { key => source.remove(key) }
    this
  }

  protected def apply(dataSource: KesqueDataSource): TotalDifficultyStorage = new TotalDifficultyStorage(dataSource)
}

