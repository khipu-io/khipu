package khipu.store

import java.math.BigInteger
import khipu.Hash
import khipu.store.datasource.DataSource

/**
 * This class is used to store the total difficulty of blocks, by using:
 *   Key: hash of the block
 *   Value: the total difficulty
 */
final class TotalDifficultyStorage(val source: DataSource) extends KeyValueStorage[Hash, BigInteger, TotalDifficultyStorage] {

  val namespace: Array[Byte] = Namespaces.TotalDifficultyNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: BigInteger => Array[Byte] = _.toByteArray
  def valueDeserializer: Array[Byte] => BigInteger = (valueBytes: Array[Byte]) => new BigInteger(1, valueBytes)

  protected def apply(dataSource: DataSource): TotalDifficultyStorage = new TotalDifficultyStorage(dataSource)
}

