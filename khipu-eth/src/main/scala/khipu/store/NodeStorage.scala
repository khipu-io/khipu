package khipu.store

import khipu.Hash
import khipu.TVal
import khipu.store.datasource.NodeDataSource
import khipu.util.SimpleMapWithUnconfirmed

final class NodeStorage(source: NodeDataSource, unconfirmedDepth: Int) extends SimpleMapWithUnconfirmed[Hash, Array[Byte]](unconfirmedDepth) {
  type This = NodeStorage

  def tableName = source.topic

  def count = source.count

  override protected def doGet(key: Hash): Option[Array[Byte]] = {
    source.get(key).map(_.value)
  }

  override protected def doUpdate(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): NodeStorage = {
    source.update(toRemove, toUpsert map { case (key, value) => key -> TVal(value, -1, -1L) })
    this
  }
}