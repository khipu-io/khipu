package khipu.store.trienode

import khipu.Hash
import khipu.store.KeyValueStorage
import khipu.store.Namespaces
import khipu.store.datasource.DataSource

/**
 * This class is used to store Nodes (defined in mpt/Node.scala) directly, by using:
 * - Key: hash of the RLP encoded node
 * - Value: the RLP encoded node
 *
 * It's the direct access storage on dataSource, and usually wrapped by
 * NodeKeyValueStorage for various caching/pruning purpose
 */
final class NodeStorage(val source: DataSource) extends KeyValueStorage[Hash, Array[Byte], NodeStorage] {

  val namespace: Array[Byte] = Namespaces.NodeNamespace
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: Array[Byte] => Array[Byte] = identity
  def valueDeserializer: Array[Byte] => Array[Byte] = identity

  protected def apply(dataSource: DataSource): NodeStorage = new NodeStorage(dataSource)
}
