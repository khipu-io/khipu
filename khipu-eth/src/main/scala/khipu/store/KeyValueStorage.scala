package khipu.store

import khipu.util.SimpleMap
import khipu.store.datasource.DataSource

object Namespaces {
  val Node = Array[Byte]('n'.toByte)
  val AppState = Array[Byte]('s'.toByte)
  val KnownNodes = Array[Byte]('k'.toByte)
  val Heights = Array[Byte]('i'.toByte)
  val FastSyncState = Array[Byte]('h'.toByte)
  val Transaction = Array[Byte]('l'.toByte)
}

trait KeyValueStorage[K, V] extends SimpleMap[K, V] {
  type This <: KeyValueStorage[K, V]

  val source: DataSource
  protected val namespace: Array[Byte]
  def keyToBytes(k: K): Array[Byte]
  def valueToBytes(k: V): Array[Byte]
  def valueFromBytes(bytes: Array[Byte]): V

  protected def apply(dataSource: DataSource): This

  /**
   * This function obtains the associated value to a key in the current namespace, if there exists one.
   *
   * @param key
   * @return the value associated with the passed key, if there exists one.
   */
  def get(key: K): Option[V] =
    source.get(namespace, keyToBytes(key)).map(valueFromBytes)

  /**
   * This function updates the KeyValueStorage by deleting, updating and inserting new (key-value) pairs
   * in the current namespace.
   *
   * @param toRemove which includes all the keys to be removed from the KeyValueStorage.
   * @param toUpsert which includes all the (key-value) pairs to be inserted into the KeyValueStorage.
   *                 If a key is already in the DataSource its value will be updated.
   * @return the new KeyValueStorage after the removals and insertions were done.
   */
  def update(toRemove: Iterable[K], toUpsert: Iterable[(K, V)]): This = {
    val newDataSource = source.update(
      namespace = namespace,
      toRemove = toRemove.map(keyToBytes),
      toUpsert = toUpsert.map { case (k, v) => keyToBytes(k) -> valueToBytes(v) }
    )
    apply(newDataSource)
  }
}

