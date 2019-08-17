package khipu.store.datasource

import khipu.Hash
import khipu.util.SimpleMap

/**
 * Storage's key should be gettable with same elements, do not use Array[Byte] as map key
 */
object EphemNodeDataSource {
  def apply(): EphemNodeDataSource = new EphemNodeDataSource()
}
final class EphemNodeDataSource() extends SimpleMap[Hash, Array[Byte]] {
  type This = EphemNodeDataSource

  private var storage: Map[Hash, Array[Byte]] = Map()

  def topic = "EphemNode"

  override def get(key: Hash): Option[Array[Byte]] = storage.get(key)

  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): EphemNodeDataSource = {
    val afterRemove = toRemove.foldLeft(storage) {
      (storage, key) => storage - key
    }
    val afterUpdate = toUpsert.foldLeft(afterRemove) {
      case (storage, (key, value)) => storage + (key -> value)
    }
    storage = afterUpdate
    this
  }

  def clear: EphemNodeDataSource = {
    storage = Map()
    this
  }

  def close(): Unit = ()

  def toSeq = storage.toSeq

  def toMap = storage
}

