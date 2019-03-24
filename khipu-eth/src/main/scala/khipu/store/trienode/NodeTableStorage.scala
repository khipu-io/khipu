package khipu.store.trienode

import akka.actor.ActorSystem
import kesque.TVal
import khipu.Hash
import khipu.store.datasource.KesqueDataSource

final class NodeTableStorage(source: KesqueDataSource)(implicit system: ActorSystem) extends NodeKeyValueStorage {
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

  def setWritingBlockNumber(writingBlockNumber: Long) = source.setWritingBlockNumber(writingBlockNumber)

  override def tableName = source.topic
}