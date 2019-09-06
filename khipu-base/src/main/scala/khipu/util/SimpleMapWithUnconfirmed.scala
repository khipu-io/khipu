package khipu.util

abstract class SimpleMapWithUnconfirmed[K, V](unconfirmedDepth: Int) extends SimpleMap[K, V] {
  type This <: SimpleMapWithUnconfirmed[K, V]

  protected val unconfirmed = new KeyValueCircularArrayQueue[K, V](unconfirmedDepth)

  private var _withUnconfirmed = false
  def withUnconfirmed = _withUnconfirmed
  def withUnconfirmed_=(b: Boolean) = _withUnconfirmed = b
  def swithToWithUnconfirmed() {
    _withUnconfirmed = true
  }

  def clearUnconfirmed() {
    unconfirmed.clear()
  }

  override def get(key: K): Option[V] = {
    unconfirmed.get(key) orElse getFromSource(key)
  }

  override def update(toRemove: Iterable[K], toUpsert: Iterable[(K, V)]): This = {
    val toFlush = if (withUnconfirmed) {
      if (unconfirmed.isFull) unconfirmed.dequeue else Nil
    } else {
      toUpsert
    }

    if (withUnconfirmed) {
      unconfirmed.enqueue(toUpsert)
    }

    updateToSource(toRemove, toFlush)
  }

  protected def getFromSource(key: K): Option[V]
  protected def updateToSource(toRemove: Iterable[K], toUpsert: Iterable[(K, V)]): This
}
