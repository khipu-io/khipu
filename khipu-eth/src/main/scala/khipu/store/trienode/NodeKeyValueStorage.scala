package khipu.store.trienode

import akka.actor.ActorSystem
import khipu.Hash
import khipu.TVal
import khipu.store.datasource.NodeDataSource
import khipu.util.SimpleMap

final class NodeKeyValueStorage(source: NodeDataSource)(implicit system: ActorSystem) extends SimpleMap[Hash, Array[Byte]] {
  type This = NodeKeyValueStorage

  import system.dispatcher

  def tableName = source.topic

  def count = source.count

  override def get(key: Hash): Option[Array[Byte]] = {
    source.get(key).map(_.value)
  }

  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): NodeKeyValueStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    source.update(toRemove, toUpsert map { case (key, value) => key -> TVal(value, -1, -1L) })
    this
  }
}