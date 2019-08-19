package khipu.storage

import khipu.Hash
import khipu.storage.datasource.BlockDataSource
import khipu.util.SimpleMapWithUnconfirmed

final class NodeStorage(source: BlockDataSource[Hash, Array[Byte]], unconfirmedDepth: Int) extends SimpleMapWithUnconfirmed[Hash, Array[Byte]](unconfirmedDepth) {
  type This = NodeStorage

  def topic = source.topic

  override protected def doGet(key: Hash): Option[Array[Byte]] = {
    source.get(key)
  }

  override protected def doUpdate(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): NodeStorage = {
    // do not do remove ?
    source.update(Nil, toUpsert)
    this
  }
}