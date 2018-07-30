package khipu.store.datasource

import khipu.util.Clock

trait DataSource {

  type Key = Array[Byte]
  type Value = Array[Byte]
  type Namespace = Array[Byte]

  /**
   * This function obtains the associated value to a key. It requires the (key-value) pair to be in the DataSource
   *
   * @param namespace which will be searched for the key.
   * @param key
   * @return the value associated with the passed key.
   */
  def apply(namespace: Namespace, key: Key): Value = get(namespace, key).get

  /**
   * This function obtains the associated value to a key, if there exists one.
   *
   * @param namespace which will be searched for the key.
   * @param key
   * @return the value associated with the passed key.
   */
  def get(namespace: Namespace, key: Key): Option[Value]

  /**
   * This function updates the DataSource by deleting, updating and inserting new (key-value) pairs.
   *
   * Note: toRemove and toUpsert should have been processed orderly properly, and so as
   * then, the keys in toRemove are not contained in toUpsert and vice versa. This
   * makes the order of toRemove and toUpsert free
   *
   * @param namespace from which the (key-value) pairs will be removed and inserted.
   * @param toRemove which includes all the keys to be removed from the DataSource.
   * @param toUpsert which includes all the (key-value) pairs to be inserted into the DataSource.
   *                 If a key is already in the DataSource its value will be updated.
   * @return the new DataSource after the removals and insertions were done.
   */
  def update(namespace: Namespace, toRemove: Iterable[Key], toUpsert: Iterable[(Key, Value)]): DataSource

  /**
   * This function updates the DataSource by deleting all the (key-value) pairs in it.
   *
   * @return the new DataSource after all the data was removed.
   */
  def clear: DataSource

  /**
   * This function closes the DataSource, without deleting the files used by it.
   */
  def close(): Unit

  /**
   * This function closes the DataSource, if it is not yet closed, and deletes all the files used by it.
   */
  def destroy(): Unit

  // --- for performance analysis
  val clock = new Clock()
}
