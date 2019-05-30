package khipu.store.datasource

import akka.util.ByteString

/**
 * Storage's key should be gettable with same elements, do not use Array[Byte] as map key
 */
object EphemDataSource {
  def apply(): EphemDataSource = new EphemDataSource(Map())
}
final class EphemDataSource(private var storage: Map[ByteString, Array[Byte]]) extends DataSource {

  override def get(namespace: Array[Byte], key: Array[Byte]): Option[Array[Byte]] = storage.get(ByteString(namespace ++ key))

  override def update(namespace: Array[Byte], toRemove: Iterable[Array[Byte]], toUpsert: Iterable[(Array[Byte], Array[Byte])]): DataSource = {
    val afterRemove = toRemove.foldLeft(storage) { (storage, key) =>
      storage - ByteString(namespace ++ key)
    }
    val afterUpdate = toUpsert.foldLeft(afterRemove) {
      case (storage, (key, value)) =>
        storage + (ByteString(namespace ++ key) -> value)
    }
    storage = afterUpdate
    this
  }

  override def clear: DataSource = {
    storage = Map()
    this
  }

  override def close(): Unit = ()

  override def destroy(): Unit = ()

  def toSeq = storage.toSeq.map(x => x._1.toArray -> x._2)
}

