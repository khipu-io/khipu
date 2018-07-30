package khipu.store.trienode

import khipu.Hash

/**
 * This class is used to store Nodes (defined in mpt/Node.scala), by using:
 * Key: hash of the RLP encoded node
 * Value: the RLP encoded node
 */
final class ArchiveNodeStorage(source: NodeStorage) extends NodeKeyValueStorage {

  override def get(key: Hash): Option[Array[Byte]] = source.get(key)

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, Array[Byte]]): ArchiveNodeStorage = {
    source.update(Set(), toUpsert)
    this
  }
}
