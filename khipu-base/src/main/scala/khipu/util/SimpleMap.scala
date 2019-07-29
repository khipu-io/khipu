package khipu.util

import scala.collection.mutable

/**
 * Interface to represent a key-value structure
 * @tparams K should be HashSet/HashMap comparable
 * @tparams V could be Any
 * @tparams T type of self
 */
trait SimpleMap[K, V] {

  type This <: SimpleMap[K, V]

  /**
   * This function obtains the value asociated with the key passed, if there exists one.
   *
   * @param key
   * @return Option object with value if there exists one.
   */
  def get(key: K): Option[V]

  /**
   * This function inserts a (key-value) pair into the trie. If the key is already asociated with another value it is updated.
   *
   * @param key
   * @param value
   * @return New trie with the (key-value) pair inserted.
   */
  def put(key: K, value: V): This = update(key -> Some(value))

  /**
   * This function inserts a (key-value) pair into the trie. If the key is already asociated with another value it is updated.
   *
   * @param kv to insert
   * @return New trie with the (key-value) pair inserted.
   */
  final def +(kv: (K, V)): This = put(kv._1, kv._2)

  /**
   * This function deletes a (key-value) pair from the trie. If no (key-value) pair exists with the passed trie then there's no effect on it.
   *
   * @param key
   * @return New trie with the (key-value) pair associated with the key passed deleted from the trie.
   */
  def remove(key: K): This = update(key -> None)

  /**
   * This function deletes a (key-value) pair from the trie. If no (key-value) pair exists with the passed trie then there's no effect on it.
   *
   * @param key
   * @return New trie with the (key-value) pair associated with the key passed deleted from the trie.
   */
  final def -(key: K): This = remove(key)

  final def update(change: (K, Option[V])): This = {
    change match {
      case (k, None)    => update(Set(k), Nil)
      case (k, Some(v)) => update(Set(), List((k -> v)))
    }
  }

  /**
   * Since the remove may still have to be saved to reposity, we'll let same key
   * in both toRemove and toUpsert
   */
  final def update(changes: Iterable[(K, Option[V])]): This = {
    // use LinkedHashMap to keep the order
    val (toRemove, toUpsert) = changes.foldLeft(mutable.ListBuffer[K](), mutable.ListBuffer[(K, V)]()) {
      case ((toRemove, toUpsert), (k, None))    => (toRemove += k, toUpsert)
      case ((toRemove, toUpsert), (k, Some(v))) => (toRemove, toUpsert += (k -> v))
    }
    update(toRemove, toUpsert)
  }

  /**
   * This function updates the KeyValueStore by deleting, updating and inserting new (key-value) pairs.
   *
   * @param toRemove which includes all the keys to be removed from the KeyValueStore.
   * @param toUpsert which includes all the (key-value) pairs to be inserted into the KeyValueStore.
   *                 If a key is already in the DataSource its value will be updated.
   * @return the new DataSource after the removals and insertions were done.
   */
  def update(toRemove: Iterable[K], toUpsert: Iterable[(K, V)]): This
}
