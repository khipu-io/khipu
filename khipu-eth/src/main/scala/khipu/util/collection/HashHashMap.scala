package khipu.util.collection

import java.util.ConcurrentModificationException
import java.util.NoSuchElementException
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * A hash map whoes key is hash itself
 * TODO
 * If we store (k.hash -> v) only, it's difficult to identity key with same hash when put
 */
object HashHashMap {

  /**
   * value NonNull
   */
  final class Entry[K, V](val key: K, var value: V) {
    var next: Entry[K, V] = _

    override def toString() = s"Entry($key -> $value)"
  }

  private val MaxCapacity = 1 << 30

  /**
   * Calculates the capacity of storage required for storing given number of elements
   */
  private def calculateCapacity(_x: Int): Int = {
    var x = _x
    if (x == 0) {
      16
    } else if (x >= MaxCapacity) {
      MaxCapacity
    } else {
      x = x - 1
      x |= x >> 1
      x |= x >> 2
      x |= x >> 4
      x |= x >> 8
      x |= x >> 16
      x + 1
    }
  }

  def apply[K <: AnyRef, V: ClassTag](): HashHashMap[K, V] = apply(16)
  def apply[K <: AnyRef, V: ClassTag](capacity: Int): HashHashMap[K, V] = apply(capacity, 0.75f)
  def apply[K <: AnyRef, V: ClassTag](capacity: Int, loadFactor: Float): HashHashMap[K, V] = new HashHashMap[K, V](calculateCapacity(capacity), loadFactor)
}
@SerialVersionUID(362340234235222265L)
final class HashHashMap[K <: AnyRef, V: ClassTag](initCapacity: Int, loadFactor: Float) extends Serializable {
  require(initCapacity >= 0 && loadFactor > 0)
  import HashHashMap._

  private var entries = Array.ofDim[Entry[K, V]](initCapacity)
  private var _size = 0

  /**
   * maximum number of elements that can be put in this map before having to rehash
   */
  private var threshold = computeThreshold()

  /**
   * modification count, to keep track of structural modifications between the
   * HashMap and the iterator
   */
  @transient
  private var modCount = 0

  def get(key: K) = internal_get(key)

  def put(key: K, value: V): Option[V] = {
    if (value != null) {
      internal_put(key, value)
    } else {
      None
    }
  }

  def remove(key: K): Option[V] = internal_remove(key)

  private def internal_get(key: K): Option[V] = {
    val index = key.hashCode & (entries.length - 1)
    val entry = findNonNullEntry(key, index)
    Option(entry).map(_.value)
  }

  /**
   * Maps the specified key to the specified value.
   *
   * @param key   the key.
   * @param value the value.
   * @return the value of any previous mapping with the specified key or None
   *         if there was no such mapping.
   */
  private def internal_put(key: K, value: V): Option[V] = {
    val index = key.hashCode & (entries.length - 1)
    findNonNullEntry(key, index) match {
      case null =>
        addEntry(key, value, index)
        modCount += 1
        _size += 1
        if (_size > threshold) {
          rehash()
        }
        None
      case entry =>
        val v = entry.value
        entry.value = value
        Some(v)
    }
  }

  private def internal_remove(key: K): Option[V] = {
    var last: Entry[K, V] = null

    val index = key.hashCode & (entries.length - 1)
    var entry = entries(index)
    while (entry != null && entry.key != key) {
      last = entry
      entry = entry.next
    }

    if (entry == null) {
      return null
    }

    if (last == null) {
      entries(index) = entry.next
    } else {
      last.next = entry.next
    }

    modCount += 1
    _size -= 1

    Option(entry).map(_.value)
  }

  private def findNonNullEntry(key: K, index: Int): Entry[K, V] = {
    var entry = entries(index)
    while (entry != null && entry.key != key) {
      entry = entry.next
    }
    entry
  }

  private def addEntry(key: K, value: V, index: Int): Entry[K, V] = {
    val entry = new Entry[K, V](key, value)
    entry.next = entries(index)
    entries(index) = entry
    entry
  }

  /**
   * Computes the threshold for rehashing
   */
  private def computeThreshold(): Int = {
    (entries.length * loadFactor).toInt
  }

  private def rehash(): Unit = rehash(entries.length)
  private def rehash(capacity: Int) {
    val newLength = calculateCapacity(if (capacity == 0) 1 else capacity << 1)
    val newEntires = Array.ofDim[Entry[K, V]](newLength)

    var i = 0
    while (i < entries.length) {
      var entry = entries(i)
      while (entry != null) {
        val index = entry.key.hashCode & (newEntires.length - 1)
        val next = entry.next
        entry.next = newEntires(index)
        newEntires(index) = entry
        entry = next
      }
      entries(i) = null
      i += 1
    }

    entries = newEntires
    threshold = computeThreshold()
  }

  def size = _size

  def isEmpty = _size == 0

  def clear() {
    if (_size > 0) {
      _size = 0
      entries = Array.ofDim[Entry[K, V]](initCapacity)
      modCount += 1
    }
  }

  def valuesIterator(): MapIterator = new ValueIterator(this)

  def iterator(): MapIterator = new EntryIterator(this)

  override def toString(): String = {
    val sb = new StringBuilder()
    sb.append(getClass.getSimpleName).append('{')
    var first = true
    val itr = iterator()
    while (itr.moveToNext()) {
      sb.append(itr.key)
        .append(" -> ")
        .append(itr.value)
      if (first) {
        first = false
        sb.append(", ")
      }
    }
    sb.append('}')
    sb.toString()
  }

  // --- iterator TODO the iterator ignores collions key currently

  sealed trait MapIterator {
    def hasNext: Boolean
    def moveToNext(): Boolean
    def key: K
    def value: V

    def remove()
  }

  abstract class AbstractMapIterator(associatedMap: HashHashMap[K, V]) extends MapIterator {
    private var position = 0
    private var expectedModCount = associatedMap.modCount
    private var prevEntry: Entry[K, V] = _
    private var nextEntry: Entry[K, V] = _

    protected var currEntry: Entry[K, V] = _

    def hasNext: Boolean = {
      if (nextEntry != null) {
        true
      } else {
        while (position < associatedMap.entries.length) {
          if (associatedMap.entries(position) == null) {
            position += 1
          } else {
            return true
          }
        }

        false
      }
    }

    def moveToNext(): Boolean = {
      if (!hasNext) {
        false
      } else {
        makeNext()
        true
      }
    }

    protected def makeNext() {
      checkConcurrentMod()
      if (!hasNext) {
        throw new NoSuchElementException()
      }
      if (nextEntry == null) {
        currEntry = associatedMap.entries(position)
        position += 1
        nextEntry = currEntry.next
        prevEntry = null
      } else {
        if (currEntry != null) {
          prevEntry = currEntry
        }
        currEntry = nextEntry
        nextEntry = nextEntry.next
      }
    }

    def remove() {
      checkConcurrentMod()
      if (currEntry == null) {
        throw new IllegalStateException()
      }

      if (prevEntry == null) {
        val index = currEntry.key.hashCode & (associatedMap.entries.length - 1)
        associatedMap.entries(index) = associatedMap.entries(index).next
      } else {
        prevEntry.next = currEntry.next
      }

      currEntry = null
      expectedModCount += 1
      associatedMap.modCount += 1
      associatedMap._size -= 1

    }

    @throws(classOf[ConcurrentModificationException])
    protected def checkConcurrentMod() {
      if (expectedModCount != associatedMap.modCount) {
        throw new ConcurrentModificationException()
      }
    }
  }

  final class EntryIterator(map: HashHashMap[K, V]) extends AbstractMapIterator(map) {
    override def key: K = currEntry.key
    override def value: V = currEntry.value
  }

  final class ValueIterator(map: HashHashMap[K, V]) extends AbstractMapIterator(map) {
    def key: K = throw new UnsupportedOperationException("Unsupported opertaion of 'key'")
    def value: V = currEntry.value
  }

}
