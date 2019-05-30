package khipu.store

import khipu.util.cache.sync.Cache

trait CachedKeyValueStorage[K, V] extends KeyValueStorage[K, V] {
  type This <: CachedKeyValueStorage[K, V]

  protected val cache: Cache[K, V]

  /**
   * This function obtains the associated value to a key in the current namespace, if there exists one.
   *
   * @param key
   * @return the value associated with the passed key, if there exists one.
   */
  override def get(key: K): Option[V] = {
    cache.get(key) match {
      case None =>
        super.get(key) match {
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

  /**
   * This function updates the KeyValueStorage by deleting, updating and inserting new (key-value) pairs
   * in the current namespace.
   *
   * @param toRemove which includes all the keys to be removed from the KeyValueStorage.
   * @param toUpsert which includes all the (key-value) pairs to be inserted into the KeyValueStorage.
   *                 If a key is already in the DataSource its value will be updated.
   * @return the new KeyValueStorage after the removals and insertions were done.
   */
  override def update(toRemove: Set[K], toUpsert: Map[K, V]) = {
    val updated = super.update(toRemove, toUpsert)
    //toUpsert foreach { case (key, value) => cache.put(key, () => Future(value)) }
    toUpsert foreach { case (key, value) => cache.put(key, value) }
    toRemove foreach { key => cache.remove(key) }
    apply(updated.source)
  }
}

