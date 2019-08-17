package khipu.store

import khipu.Hash
import khipu.util.SimpleMap

/**
 * This class is used to store Nodes (defined in mpt/Node.scala), by using:
 * Key: hash of the RLP encoded node
 * Value: the RLP encoded node
 */
final class ArchiveNodeStorage(source: SimpleMap[Hash, Array[Byte]]) extends SimpleMap[Hash, Array[Byte]] {
  type This = ArchiveNodeStorage

  def topic = source.topic

  override def get(key: Hash): Option[Array[Byte]] = source.get(key)

  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): ArchiveNodeStorage = {
    source.update(Nil, toUpsert)
    this
  }
}
