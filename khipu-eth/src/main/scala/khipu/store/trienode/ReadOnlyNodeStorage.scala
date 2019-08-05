package khipu.store.trienode

import khipu.Hash
import khipu.util.SimpleMap

/**
 * This storage allows to read from another NodeKeyValueStorage but doesn't remove or upsert into database.
 * To do so, it uses an internal in memory cache to apply all the changes.
 */
object ReadOnlyNodeStorage {
  def apply(source: NodeKeyValueStorage): ReadOnlyNodeStorage =
    new ReadOnlyNodeStorage(source, Map())
}
final class ReadOnlyNodeStorage private (source: NodeKeyValueStorage, cache: Map[Hash, Option[Array[Byte]]]) extends SimpleMap[Hash, Array[Byte]] {
  type This = ReadOnlyNodeStorage

  def tableName = ""
  def count = -1

  /**
   * Persists the changes into the underlying [[khipu.common.SimpleMap]]
   *
   * @return Updated store
   */
  def commit(): ReadOnlyNodeStorage = {
    new ReadOnlyNodeStorage(source.update(cache), Map())
  }

  /**
   * Clears the cache without applying the changes
   *
   * @return Updated proxy
   */
  def rollback(): ReadOnlyNodeStorage = new ReadOnlyNodeStorage(source, Map())

  /**
   * This function obtains the value asociated with the key passed, if there exists one.
   *
   * @param key
   * @return Option object with value if there exists one.
   */
  override def get(key: Hash): Option[Array[Byte]] = cache.getOrElse(key, source.get(key))

  /**
   * This function updates the KeyValueStore by deleting, updating and inserting new (key-value) pairs.
   *
   * @param toRemove which includes all the keys to be removed from the KeyValueStore.
   * @param toUpsert which includes all the (key-value) pairs to be inserted into the KeyValueStore.
   *                 If a key is already in the DataSource its value will be updated.
   * @return the new DataSource after the removals and insertions were done.
   */
  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): ReadOnlyNodeStorage = {
    val afterRemove = toRemove.foldLeft(cache) { (updated, k) => updated + (k -> None) }
    val afterUpsert = toUpsert.foldLeft(afterRemove) { case (updated, (k, v)) => updated + (k -> Some(v)) }
    new ReadOnlyNodeStorage(source, afterUpsert)
  }

}