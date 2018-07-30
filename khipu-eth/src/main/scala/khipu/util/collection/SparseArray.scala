package khipu.util.collection

//import com.android.internal.util.ArrayUtils;
//import com.android.internal.util.GrowingArrayUtils;
//import libcore.util.EmptyArray;

import scala.reflect.ClassTag

object SparseArray {

  def binarySearch(array: Array[Int], size: Int, value: Int): Int = {
    var lo = 0
    var hi = size - 1
    while (lo <= hi) {
      val mid = (lo + hi) >>> 1
      val midVal = array(mid)
      if (midVal < value) {
        lo = mid + 1
      } else if (midVal > value) {
        hi = mid - 1
      } else {
        return mid // value found
      }
    }
    ~lo // value not present
  }

  def binarySearch(array: Array[Long], size: Int, value: Long): Int = {
    var lo = 0
    var hi = size - 1
    while (lo <= hi) {
      val mid = (lo + hi) >>> 1
      val midVal = array(mid)
      if (midVal < value) {
        lo = mid + 1
      } else if (midVal > value) {
        hi = mid - 1
      } else {
        return mid // value found
      }
    }
    ~lo // value not present
  }

  def apply[Int]() = new SparseArray(10, Int.MinValue, Int.MaxValue, None)
  def apply[E <: AnyRef: ClassTag](DELETED: E): SparseArray[E] = apply(10, DELETED, null.asInstanceOf[E])
  def apply[E: ClassTag](initialSize: Int, DELETED: E, NULL: E, elementClass: Option[Class[E]] = None): SparseArray[E] =
    new SparseArray(initialSize, DELETED, NULL, elementClass)

}
final class SparseArray[E: ClassTag](initialSize: Int, DELETED: E, NULL: E, elementClass: Option[Class[E]]) extends Cloneable {
  import SparseArray._
  type KEY = Int // or Long

  private var mGarbage = false
  private var mKeys = Array.ofDim[KEY](initialSize)
  private var mValues = Array.ofDim[E](initialSize)
  private var mSize = 0

  /**
   * Under case of calling new SparseArray[T] where T is not specified explicitly
   * during compile time, ie. the T is got during runtime, 'm' may be "_ <: Any",
   * or AnyRef. In the case of, we should pass in an elementClass and create the
   * array via
   *   java.lang.reflect.Array.newInstance(x, size)
   */
  private def makeArray(size: Int) = {
    elementClass match {
      case Some(x) =>
        java.lang.reflect.Array.newInstance(x, size).asInstanceOf[Array[E]]
      case None =>
        // If the A is specified under compile time, that's ok, an Array[A] will be 
        // created (if A is primitive type, will also create a primitive typed array @see scala.reflect.Manifest)
        // If A is specified under runtime, will create a Array[AnyRef]
        Array.ofDim[E](size)
    }
  }

  private def insert(array: Array[E], currentSize: Int, index: Int, element: E): Array[E] = {
    assert(currentSize <= array.length)
    if (currentSize + 1 <= array.length) {
      System.arraycopy(array, index, array, index + 1, currentSize - index)
      array(index) = element
      return array
    }
    val newArray = makeArray(growSize(currentSize))
    System.arraycopy(array, 0, newArray, 0, index)
    newArray(index) = element
    System.arraycopy(array, index, newArray, index + 1, array.length - index)
    newArray
  }

  private def insertKey(array: Array[KEY], currentSize: Int, index: Int, element: KEY): Array[KEY] = {
    assert(currentSize <= array.length)
    if (currentSize + 1 <= array.length) {
      System.arraycopy(array, index, array, index + 1, currentSize - index)
      array(index) = element
      return array
    }
    val newArray = Array.ofDim[KEY](growSize(currentSize))
    System.arraycopy(array, 0, newArray, 0, index)
    newArray(index) = element
    System.arraycopy(array, index, newArray, index + 1, array.length - index)
    newArray
  }

  private def append(_array: Array[E], currentSize: Int, element: E): Array[E] = {
    assert(currentSize <= _array.length)
    val array = if (currentSize + 1 > _array.length) {
      val newArray = makeArray(growSize(currentSize))
      System.arraycopy(_array, 0, newArray, 0, currentSize)
      newArray
    } else {
      _array
    }
    array(currentSize) = element
    array
  }

  private def appendKey(_array: Array[KEY], currentSize: Int, element: KEY): Array[KEY] = {
    assert(currentSize <= _array.length)
    val array = if (currentSize + 1 > _array.length) {
      val newArray = Array.ofDim[KEY](growSize(currentSize))
      System.arraycopy(_array, 0, newArray, 0, currentSize)
      newArray
    } else {
      _array
    }
    array(currentSize) = element
    array
  }

  private def growSize(currentSize: Int) = {
    if (currentSize <= 4) 8 else currentSize * 2
  }

  /**
   * Gets the Object mapped from the specified key, or <code>null</code>
   * if no such mapping has been made.
   */
  def get(key: KEY): Option[E] = {
    val i = binarySearch(mKeys, mSize, key)
    if (i < 0 || mValues(i) == DELETED) {
      None
    } else {
      Some(mValues(i))
    }
  }

  /**
   * Gets the Object mapped from the specified key, or the specified Object
   * if no such mapping has been made.
   */
  def getOrElse(key: KEY, valueIfKeyNotFound: E): E = {
    val i = binarySearch(mKeys, mSize, key)
    if (i < 0 || mValues(i) == DELETED) {
      valueIfKeyNotFound
    } else {
      mValues(i)
    }
  }

  /**
   * Removes the mapping from the specified key, if there was any.
   */
  def remove(key: KEY) {
    val i = binarySearch(mKeys, mSize, key)
    if (i >= 0) {
      if (mValues(i) != DELETED) {
        mValues(i) = DELETED
        mGarbage = true
      }
    }
  }

  /**
   * Removes the mapping from the specified key, if there was any, returning the old value.
   */
  def removeReturnOld(key: KEY): Option[E] = {
    val i = binarySearch(mKeys, mSize, key)
    if (i >= 0) {
      if (mValues(i) != DELETED) {
        val old = mValues(i)
        mValues(i) = DELETED
        mGarbage = true
        return Some(old)
      }
    }
    None
  }

  /**
   * Removes the mapping at the specified index.
   *
   * <p>For indices outside of the range <code>0...size()-1</code>,
   * the behavior is undefined.</p>
   */
  def removeAt(index: Int) {
    if (mValues(index) != DELETED) {
      mValues(index) = DELETED
      mGarbage = true
    }
  }

  /**
   * Remove a range of mappings as a batch.
   *
   * @param index Index to begin at
   * @param size Number of mappings to remove
   *
   * <p>For indices outside of the range <code>0...size()-1</code>,
   * the behavior is undefined.</p>
   */
  def removeAtRange(index: Int, size: Int) {
    val end = math.min(mSize, index + size)
    var i = index
    while (i < end) {
      removeAt(i)
      i += 1
    }
  }

  private def gc() {}

  private def gc_() {
    val n = mSize
    var o = 0
    var keys = mKeys
    var values = mValues
    var i = 0
    while (i < n) {
      val v = values(i)
      if (v != DELETED) {
        if (i != o) {
          keys(o) = keys(i)
          values(o) = v
          values(i) = NULL
        }
        o += 1
      }
      i += 1
    }
    mGarbage = false
    mSize = o
  }

  /**
   * Adds a mapping from the specified key to the specified value,
   * replacing the previous mapping from the specified key if there
   * was one.
   */
  def put(key: KEY, value: E) {
    var i = binarySearch(mKeys, mSize, key)
    if (i >= 0) {
      mValues(i) = value
    } else {
      i = ~i
      if (i < mSize && mValues(i) == DELETED) {
        mKeys(i) = key
        mValues(i) = value
        return
      }
      if (mGarbage && mSize >= mKeys.length) {
        gc()
        // search again because indices may have changed.
        i = ~binarySearch(mKeys, mSize, key)
      }
      mKeys = insertKey(mKeys, mSize, i, key)
      mValues = insert(mValues, mSize, i, value)
      mSize += 1
    }
  }

  /**
   * Returns the number of key-value mappings that this SparseArray
   * currently stores.
   */
  def size: Int = {
    if (mGarbage) {
      gc()
    }
    mSize
  }

  /**
   * Given an index in the range <code>0...size()-1</code>, returns
   * the key from the <code>index</code>th key-value mapping that this
   * SparseArray stores.
   *
   * <p>The keys corresponding to indices in ascending order are guaranteed to
   * be in ascending order, e.g., <code>keyAt(0)</code> will return the
   * smallest key and <code>keyAt(size()-1)</code> will return the largest
   * key.</p>
   *
   * <p>For indices outside of the range <code>0...size()-1</code>,
   * the behavior is undefined.</p>
   */
  def keyAt(index: Int): KEY = {
    if (mGarbage) {
      gc()
    }
    mKeys(index)
  }

  /**
   * Given an index in the range <code>0...size()-1</code>, returns
   * the value from the <code>index</code>th key-value mapping that this
   * SparseArray stores.
   *
   * <p>The values corresponding to indices in ascending order are guaranteed
   * to be associated with keys in ascending order, e.g.,
   * <code>valueAt(0)</code> will return the value associated with the
   * smallest key and <code>valueAt(size()-1)</code> will return the value
   * associated with the largest key.</p>
   *
   * <p>For indices outside of the range <code>0...size()-1</code>,
   * the behavior is undefined.</p>
   */
  def valueAt(index: Int): E = {
    if (mGarbage) {
      gc()
    }
    mValues(index)
  }

  /**
   * Given an index in the range <code>0...size()-1</code>, sets a new
   * value for the <code>index</code>th key-value mapping that this
   * SparseArray stores.
   *
   * <p>For indices outside of the range <code>0...size()-1</code>, the behavior is undefined.</p>
   */
  def setValueAt(index: Int, value: E) {
    if (mGarbage) {
      gc()
    }
    mValues(index) = value
  }

  /**
   * Returns the index for which {@link #keyAt} would return the
   * specified key, or a negative number if the specified
   * key is not mapped.
   */
  def indexOfKey(key: KEY): Int = {
    if (mGarbage) {
      gc()
    }
    binarySearch(mKeys, mSize, key)
  }

  /**
   * Returns an index for which {@link #valueAt} would return the
   * specified key, or a negative number if no keys map to the
   * specified value.
   * <p>Beware that this is a linear search, unlike lookups by key,
   * and that multiple keys can map to the same value and this will
   * find only one of them.
   * <p>Note also that unlike most collections' {@code indexOf} methods,
   * this method compares values using {@code ==} rather than {@code equals}.
   */
  def indexOfValue(value: E): Int = {
    if (mGarbage) {
      gc()
    }
    var i = 0
    while (i < mSize) {
      if (mValues(i) == value) {
        return i
      }
      i += 1
    }
    -1
  }

  /**
   * Returns an index for which {@link #valueAt} would return the
   * specified key, or a negative number if no keys map to the
   * specified value.
   * <p>Beware that this is a linear search, unlike lookups by key,
   * and that multiple keys can map to the same value and this will
   * find only one of them.
   * <p>Note also that this method uses {@code equals} unlike {@code indexOfValue}.
   * @hide
   */
  def indexOfValueByValue(value: E): Int = {
    if (mGarbage) {
      gc()
    }
    var i = 0
    while (i < mSize) {
      if (value == NULL) {
        if (mValues(i) == NULL) {
          return i
        }
      } else {
        if (value.equals(mValues(i))) {
          return i
        }
      }
      i += 1
    }
    -1
  }

  /**
   * Removes all key-value mappings from this SparseArray.
   */
  def clear() {
    val n = mSize
    var values = mValues
    var i = 0
    while (i < n) {
      values(i) = NULL
      i += 1
    }
    mSize = 0
    mGarbage = false
  }

  /**
   * Puts a key/value pair into the array, optimizing for the case where
   * the key is greater than all existing keys in the array.
   */
  def append(key: KEY, value: E) {
    if (mSize != 0 && key <= mKeys(mSize - 1)) {
      put(key, value)
      return
    }
    if (mGarbage && mSize >= mKeys.length) {
      gc()
    }
    mKeys = appendKey(mKeys, mSize, key)
    mValues = append(mValues, mSize, value)
    mSize += 1
  }

  override def clone() = {
    try {
      val clone = super.clone().asInstanceOf[SparseArray[E]]
      clone.mKeys = mKeys.clone()
      clone.mValues = mValues.clone()
      clone
    } catch {
      case ex: CloneNotSupportedException => null
      /* ignore */
    }
  }

  override def toString(): String = {
    if (size <= 0) {
      "{}"
    } else {
      val buffer = new StringBuilder(mSize * 28)
      buffer.append('{')
      var i = 0
      while (i < mSize) {
        if (i > 0) {
          buffer.append(", ")
        }
        val key = keyAt(i)
        buffer.append(key)
        buffer.append('=')
        val value = valueAt(i)
        if (value != this) {
          buffer.append(value)
        } else {
          buffer.append("(this Map)")
        }
        i += 1
      }
      buffer.append('}')
      buffer.toString()
    }
  }
}