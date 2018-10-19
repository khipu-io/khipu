package khipu.util.collection

/**
 * Under 4G Xmx, the expention from size 201652706 will cause OutOfMemoryError:
 * 100M records, (4 + 4) bytes x 100M = 100M * 8 = 800M
 */
object IntSet {
  private val FREE_KEY = 0
  val NO_VALUE = Int.MinValue

  /**
   * Return the least power of two greater than or equal to the specified value.
   *
   * Note that this function will return 1 when the argument is 0.
   *
   * @param x a long integer smaller than or equal to 2<sup>62</sup>.
   * @return the least power of two greater than or equal to the specified value.
   */
  def nextPowerOfTwo(_x: Long): Long = {
    var x = _x
    if (x == 0) {
      1
    } else {
      x -= 1
      x |= x >> 1
      x |= x >> 2
      x |= x >> 4
      x |= x >> 8
      x |= x >> 16
      (x | x >> 32) + 1
    }
  }

  /**
   * Returns the least power of two smaller than or equal to 2<sup>30</sup> and larger than or equal to <code>Math.ceil( expected / f )</code>.
   *
   * @param expected the expected number of elements in a hash table.
   * @param f the load factor.
   * @return the minimum possible size for a backing array.
   * @throws IllegalArgumentException if the necessary size is larger than 2<sup>30</sup>.
   */
  private val MAX_SIZE: Long = 1L << 30
  def arraySize(expected: Int, f: Float): Int = {
    val s = math.max(2L, nextPowerOfTwo(math.ceil(expected / f).toLong))
    if (s > MAX_SIZE) {
      throw new IllegalArgumentException("Too large (" + expected + " expected elements with load factor " + f + ")")
    }
    s.toInt
  }

  /**
   * scramble the key by shuffling its bits -- hash of int
   */
  private val INT_PHI = 0x9E3779B9
  def phiMix(x: Int): Int = {
    val h = x * INT_PHI
    h ^ (h >> 16)
  }

  // --- simple test
  def main(args: Array[String]) {
    val max = 1000000
    val map = new IntSet(200)
    var i = -max
    while (i <= max) {
      map.put(i, i)
      i += 1
    }

    i = -max
    while (i <= max) {
      if (map.get(i) != i) {
        println(s"err at $i - before remove")
      }
      i += 1
    }
    println(map.get(max))

    i = -max
    while (i <= max) {
      map.remove(i)
      i += 1
    }

    i = -max
    while (i <= max) {
      if (map.get(i) != NO_VALUE) {
        println(s"err at $i - after remove")
      }
      i += 1
    }
  }
}
/**
 * How about if the int key is already a hash value? do we still need to
 * scramble the key by shuffling its bits using something like phiMix?
 *
 * Fill factor, must be between (0 and 1)
 */
final class IntSet(initSize: Int, fillFactor: Float = 0.75f) {
  import IntSet._

  private val isCapacityByPowTwo = true

  if (fillFactor <= 0 || fillFactor >= 1) {
    throw new IllegalArgumentException("FillFactor must be in (0, 1)")
  }
  if (initSize <= 0) {
    throw new IllegalArgumentException("Size must be positive!")
  }

  /** Do we have 'free' key in the map? */
  private var m_hasFreeKey: Boolean = false
  /** Value of 'free' key */
  private var m_freeValue: Int = NO_VALUE
  /** Current map size */
  private var m_size: Int = _

  /** number of buckets */
  private var m_capacity: Int = _
  /** Keys and values */
  private var m_data: Array[Int] = _
  /** We will resize a map once it reaches this size */
  private var m_threshold: Int = _
  /** Mask to calculate the original position */
  private var m_mask: Int = _
  private var m_mask2: Int = _

  expendCapacity(isInit = true)

  def +=(key: Int): this.type = {
    put(key, 1)
    this
  }

  def contains(key: Int): Boolean = {
    get(key) == 1
  }

  def get(key: Int): Int = {
    var ptr = getStartIndex(key)

    if (key == FREE_KEY) {
      return if (m_hasFreeKey) m_freeValue else NO_VALUE
    }

    var k = m_data(ptr)
    if (k == FREE_KEY) {
      return NO_VALUE // end of chain already
    }
    if (k == key) { // we check FREE prior to this call
      return m_data(ptr + 1)
    }

    while (true) {
      ptr = getNextIndex(ptr) // that's next index
      k = m_data(ptr)
      if (k == FREE_KEY) {
        return NO_VALUE
      }
      if (k == key) {
        return m_data(ptr + 1)
      }
    }

    // should not be here
    return NO_VALUE
  }

  def put(key: Int, value: Int): Int = {
    if (key == FREE_KEY) {
      val ret = m_freeValue
      if (!m_hasFreeKey) {
        m_size += 1
      }

      m_hasFreeKey = true
      m_freeValue = value
      return ret
    }

    var ptr = getStartIndex(key)
    var k = m_data(ptr)
    if (k == FREE_KEY) { // end of chain already
      m_data(ptr) = key
      m_data(ptr + 1) = value
      if (m_size >= m_threshold)
        rehash()
      else
        m_size += 1
      return NO_VALUE
    } else if (k == key) { // we check FREE prior to this call
      val ret = m_data(ptr + 1)
      m_data(ptr + 1) = value
      return ret
    }

    while (true) {
      ptr = getNextIndex(ptr)
      k = m_data(ptr)
      if (k == FREE_KEY) {
        m_data(ptr) = key
        m_data(ptr + 1) = value
        if (m_size >= m_threshold)
          rehash()
        else
          m_size += 1
        return NO_VALUE
      } else if (k == key) {
        val ret = m_data(ptr + 1)
        m_data(ptr + 1) = value
        return ret
      }
    }

    // should not be here
    return NO_VALUE
  }

  def remove(key: Int): Int = {
    if (key == FREE_KEY) {
      if (!m_hasFreeKey)
        return NO_VALUE
      m_hasFreeKey = false
      m_size -= 1
      return m_freeValue // value is not cleaned
    }

    var ptr = getStartIndex(key)
    var k = m_data(ptr)
    if (k == key) { // we check FREE prior to this call
      val res = m_data(ptr + 1)
      shiftKeys(ptr)
      m_size -= 1
      return res
    } else if (k == FREE_KEY)
      return NO_VALUE // end of chain already

    while (true) {
      ptr = getNextIndex(ptr)
      k = m_data(ptr)
      if (k == key) {
        val res = m_data(ptr + 1)
        shiftKeys(ptr)
        m_size -= 1
        return res
      } else if (k == FREE_KEY)
        return NO_VALUE
    }

    // should not be here
    return NO_VALUE
  }

  private def shiftKeys(_pos: Int): Int = {
    // shift entries with the same hash.
    var last = 0
    var slot = 0
    var k = 0
    val data = this.m_data
    var pos = _pos
    while (true) {
      last = pos
      pos = getNextIndex(last)

      var break = false
      while (!break) {
        k = data(pos)
        if (k == FREE_KEY) {
          data(last) = FREE_KEY
          return last
        }
        slot = getStartIndex(k) // calculate the starting slot for the current key
        if (if (last <= pos) last >= slot || slot > pos else last >= slot && slot > pos) {
          break = true
        } else {
          pos = getNextIndex(pos) // go to the next entry
        }
      }
      data(last) = k
      var n = 0
      data(last + 1) = data(pos + 1)
    }

    // should not be here
    return last
  }

  def size = m_size

  private def rehash() {
    val oldLength = m_data.length
    val oldData = m_data

    expendCapacity(isInit = false)

    m_size = if (m_hasFreeKey) 1 else 0
    var i = 0
    while (i < oldLength) {
      val oldKey = oldData(i)
      if (oldKey != FREE_KEY) {
        put(oldKey, oldData(i + 1))
      }
      i += 2
    }
  }

  private def expendCapacity(isInit: Boolean) {
    m_capacity = if (isInit) {
      arraySize(initSize, fillFactor)
    } else {
      if (isCapacityByPowTwo) {
        (m_capacity * 2)
      } else {
        (m_capacity * 1.2).toInt
      }
    }

    m_mask = if (isCapacityByPowTwo) m_capacity - 1 else m_capacity
    m_mask2 = if (isCapacityByPowTwo) m_capacity * 2 - 1 else m_capacity * 2

    m_threshold = (m_capacity * fillFactor).toInt

    m_data = Array.ofDim(m_capacity * 2)
    // should filled m_data's value positions with NO_VALUE, otherwise, for example, m_data(1)
    // filled by key1 may left m_data(2), m_data(2) to be meaningful 0
    var i = 0
    while (i < m_data.length) {
      m_data(i + 1) = NO_VALUE
      i += 2
    }
  }

  /**
   * When length is pow of 2, (i % length) == (i & (length - 1))
   */
  private def getStartIndex(key: Int): Int = {
    if (isCapacityByPowTwo) {
      (phiMix(key) & m_mask) * 2
    } else {
      (phiMix(key) % m_mask) * 2
    }
  }

  private def getNextIndex(currentIndex: Int): Int = {
    if (isCapacityByPowTwo) {
      (currentIndex + 2) & m_mask2
    } else {
      (currentIndex + 2) % m_mask2
    }
  }
}

