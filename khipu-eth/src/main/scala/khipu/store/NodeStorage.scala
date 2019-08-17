package khipu.store

import khipu.Hash
import khipu.store.datasource.NodeDataSource
import khipu.util.SimpleMapWithUnconfirmed

final class NodeStorage(source: NodeDataSource, unconfirmedDepth: Int) extends SimpleMapWithUnconfirmed[Hash, Array[Byte]](unconfirmedDepth) {
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