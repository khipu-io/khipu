package khipu.store.trienode

import akka.actor.ActorSystem
import kesque.TVal
import khipu.Hash
import khipu.store.datasource.NodeDataSource

final class NodeTableStorage(source: NodeDataSource)(implicit system: ActorSystem) extends NodeKeyValueStorage {
  type This = NodeTableStorage

  import system.dispatcher

  override def get(key: Hash): Option[Array[Byte]] = {
    source.get(key).map(_.value)
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, Array[Byte]]): NodeTableStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    //toUpsert foreach { case (key, value) => nodeTable.put(key, () => Future(value)) }
    source.update(toRemove, toUpsert map { case (key, value) => key -> TVal(value, -1, -1L) })
    this
  }

  def count = source.count

  override def tableName = source.topic
}