package khipu.storage.datasource

import khipu.util.Clock

trait KeyValueDataSource {

  def topic: String

  /**
   * This function obtains the associated value to a key. It requires the (key-value) pair to be in the DataSource
   *
   * @param namespace which will be searched for the key.
   * @param key
   * @return the value associated with the passed key.
   */
  def apply(namespace: Array[Byte], key: Array[Byte]): Array[Byte] = get(namespace, key).get

  /**
   * This function obtains the associated value to a key, if there exists one.
   *
   * @param namespace which will be searched for the key.
   * @param key
   * @return the value associated with the passed key.
   */
  def get(namespace: Array[Byte], key: Array[Byte]): Option[Array[Byte]]

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
  def update(namespace: Array[Byte], toRemove: Iterable[Array[Byte]], toUpsert: Iterable[(Array[Byte], Array[Byte])]): KeyValueDataSource

  /**
   * This function updates the DataSource by deleting all the (key-value) pairs in it.
   *
   * @return the new DataSource after all the data was removed.
   */
  def clear: KeyValueDataSource

  /**
   * This function closes the DataSource, without deleting the files used by it.
   */
  def stop(): Unit

  /**
   * This function closes the DataSource, if it is not yet closed, and deletes all the files used by it.
   */
  def destroy(): Unit

  def count: Long

  // --- for performance analysis
  val clock = new Clock()
}
