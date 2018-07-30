package khipu.store.trienode

import akka.actor.ActorSystem
import khipu.store.datasource.KesqueDataSource
import kesque.TVal
import khipu.Hash
import khipu.util.cache.sync.Cache

/**
 * Global node cache
 */
final class CachedNodeStorage(source: NodeStorage, cache: Cache[Hash, Array[Byte]])(implicit system: ActorSystem) extends NodeKeyValueStorage {
  import system.dispatcher

  override def get(key: Hash): Option[Array[Byte]] = {
    cache.get(key) match {
      case None =>
        source.get(key) match {
          case some @ Some(value) =>
            //cache.put(key, () => Future(value))
            cache.put(key, value)
            some
          case None => None
        }
      case Some(value) =>
        //Some(Await.result(value, Duration.Inf))
        Some(value)
    }
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, Array[Byte]]): CachedNodeStorage = {
    //toRemove foreach CachedNodeStorage.remove // TODO remove from repositoty when necessary (pruning)
    source.update(Set(), toUpsert)
    //toUpsert foreach { case (key, value) => cache.put(key, () => Future(value)) }
    toUpsert foreach { case (key, value) => cache.put(key, value) }
    toRemove foreach { key => cache.remove(key) }
    this
  }

}