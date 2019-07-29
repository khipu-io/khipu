package khipu.store.trienode

import khipu.Hash

/**
 * This class is used to store Nodes (defined in mpt/Node.scala), by using:
 * Key: hash of the RLP encoded node
 * Value: the RLP encoded node
 */
final class ArchiveNodeStorage(source: NodeStorage) extends NodeKeyValueStorage {
  type This = ArchiveNodeStorage

  def tableName = ""
  def count = -1

  override def get(key: Hash): Option[Array[Byte]] = source.get(key)

  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): ArchiveNodeStorage = {
    source.update(Nil, toUpsert)
    this
  }
}
