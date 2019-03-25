package kesque

import java.util.Arrays

object IntIntsMap {
  type V = Int // unsigned int could be 2^32 = 4,294,967,296

  private val FREE_KEY = 0
  val NO_VALUE = Array[V]()

  // --- simple test
  def main(args: Array[String]) {
    val max = 1000000
    val map = new IntIntsMap(200, 3)

    var n = 0
    while (n < 3) {
      println(s"n is $n")
      // put i and -i
      var i = -max
      while (i <= max) {
        map.put(i, i, n)
        map.put(i, -i, n)
        //println(s"${map.get(i, n).mkString("[", ",", "]")}")
        i += 1
      }

      i = -max
      while (i <= max) {
        if (i == -i) {
          if (!Arrays.equals(map.get(i, n), Array(i))) {
            println(s"err at $i - ${map.get(i, n).mkString("[", ",", "]")} should be [$i]")
            System.exit(-1)
          }
        } else {
          if (!Arrays.equals(map.get(i, n), Array(i, -i))) {
            println(s"err at $i - ${map.get(i, n).mkString("[", ",", "]")} should be [$i, ${-i}]")
            System.exit(-1)
          }
        }
        i += 1
      }
      println(map.get(max, n).mkString(","))
      println(map.get(1, n).mkString(","))
      println(map.get(max - 1, n).mkString(","))

      // remove value -i from map
      i = -max
      while (i <= max) {
        map.removeValue(i, -i, n)
        i += 1
      }
      i = -max
      while (i <= max) {
        if (i == -i) {
          if (map.get(i, n) != NO_VALUE) {
            println(s"Remove value -$i from map: err at $i - ${map.get(i, n).mkString("[", ",", "]")}")
            System.exit(-1)
          }
        } else {
          if (!Arrays.equals(map.get(i, n), Array(i))) {
            println(s"Remove value -$i from map: err at $i - ${map.get(i, n).mkString("[", ",", "]")}")
            System.exit(-1)
          }
        }
        i += 1
      }

      // remove all value
      i = -max
      while (i <= max) {
        map.remove(i, n)
        if (map.get(i, n) != NO_VALUE) {
          println(s"Remove all value: err at $i - ${map.get(i, n).mkString("[", ",", "]")}")
          System.exit(-1)
        }
        i += 1
      }

      n += 1
    }
  }
}

/**
 * It's actually an Int to Ints map
 * We introduce one extra pairs of fields - for key=0, which is used as 'used' flag
 * Memory usage:
 *   Int -> Int[]
 * Key (Int) in bytes: 4
 * Value (Int[]) in bytes: (16(reference) + 4(length) + 4(align)) + 4 * N = 24 + 4 * N
 * KV in Bytes: 28 + 4 * N
 *
 * There are about 99.6% keys have only 1 value, i.e. 28 + 4 = 32 bytes.
 * Thus 100,000,000 kvs in bytes: 100,000,000 * 32 / 1024 / 1024 / 1024 = 3G
 */
final class IntIntsMap(initSize: Int, nValues: Int, fillFactor: Float = 0.75f) {
  import IntIntsMap._

  private val isCapacityByPowTwo = false

  if (fillFactor <= 0 || fillFactor >= 1) {
    throw new IllegalArgumentException("FillFactor must be in (0, 1)")
  }
  if (initSize <= 0) {
    throw new IllegalArgumentException("Size must be positive!")
  }

  /** Do we have 'free' key in the map? */
  private val m_hasFreeKey: Array[Boolean] = Array.ofDim[Boolean](nValues)
  /** Value of 'free' key */
  private val m_freeValue: Array[Array[V]] = Array.ofDim[Array[V]](nValues)
  /** Current map size */
  private var m_size: Int = _

  /** number of buckets */
  private var m_capacity = IntIntMap.arraySize(initSize, fillFactor)
  /** Keys */
  private var m_keys: Array[Int] = _
  /** Values should be var instead of val, see rehash() when move data from oldValues to newValues */
  private var m_values: Array[Array[Array[V]]] = _
  /** We will resize a map once it reaches this size */
  private var m_threshold: Int = _
  /** Mask to calculate the original position */
  private var m_mask: Int = _

  expendCapacity(isInit = true)

  def get(key: Int, col: Int): Array[V] = {
    if (key == FREE_KEY) {
      if (m_hasFreeKey(col)) m_freeValue(col) else NO_VALUE
    } else {
      val idx = getReadIndex(key)
      if (idx != -1) m_values(col)(idx) else NO_VALUE
    }
  }

  def put(key: Int, value: V, col: Int): Array[V] = {
    val oldValues = get(key, col)
    if (oldValues == NO_VALUE) {
      put(key, Array(value), col)
    } else {
      val existed = findValue(oldValues, value)
      if (existed != -1) { // already existed do nothing
        Array(value)
      } else {
        val newValues = Array.ofDim[V](oldValues.length + 1)
        System.arraycopy(oldValues, 0, newValues, 0, oldValues.length)
        newValues(newValues.length - 1) = value
        put(key, newValues, col)
      }
    }
  }

  private def put(key: Int, value: Array[V], col: Int): Array[V] = {
    if (key == FREE_KEY) {
      val ret = m_freeValue(col)
      if (!m_hasFreeKey(col)) {
        m_size += 1
      }
      m_hasFreeKey(col) = true
      m_freeValue(col) = value
      ret
    } else {
      var idx = getPutIndex(key)
      if (idx < 0) { // no insertion point? Should not happen...
        rehash()
        idx = getPutIndex(key)
      }
      val prev = m_values(col)(idx)
      if (m_keys(idx) != key) {
        m_keys(idx) = key
        m_values(col)(idx) = value
        m_size += 1
        if (m_size >= m_threshold) {
          rehash()
        }
      } else { // it means used cell with our key
        assert(m_keys(idx) == key)
        m_values(col)(idx) = value
      }
      prev
    }
  }

  def removeValue(key: Int, value: V, col: Int): Array[V] = {
    val oldValues = get(key, col)
    if (oldValues == NO_VALUE) {
      NO_VALUE
    } else {
      val existed = findValue(oldValues, value)
      if (existed != -1) { // existed
        if (oldValues.length == 1) { // the existed only one will be removed
          remove(key, col)
        } else {
          val newLen = oldValues.length - 1
          val xs = Array.ofDim[V](newLen)
          System.arraycopy(oldValues, 0, xs, 0, existed)
          System.arraycopy(oldValues, existed + 1, xs, existed, newLen - existed)
          put(key, xs, col)

          Array(value)
        }
      } else {
        NO_VALUE
      }
    }
  }

  def replace(key: Int, toRemove: V, toPut: V, col: Int): Array[V] = {
    val oldValues = get(key, col)
    if (oldValues == NO_VALUE) {
      put(key, Array(toPut), col)
    } else {
      val (toRemoveExisted, toPutExisted) = findValues(oldValues, toRemove, toPut)
      if (toRemoveExisted != -1) {
        if (toPutExisted != 1) {
          if (toRemoveExisted == toPutExisted) { // do nothing, do not remove already existed toPut
            Array(toPut)
          } else { // remove existed only, toPut already there
            val newLen = oldValues.length - 1
            val newValues = Array.ofDim[V](newLen)
            System.arraycopy(oldValues, 0, newValues, 0, toRemoveExisted)
            System.arraycopy(oldValues, toRemoveExisted + 1, newValues, toRemoveExisted, newLen - toRemoveExisted)
            put(key, newValues, col)

            Array(toPut)
          }
        } else { // remove existed and put no-existed
          val newLen = oldValues.length
          val newValues = Array.ofDim[V](newLen)
          System.arraycopy(oldValues, 0, newValues, 0, toRemoveExisted)
          System.arraycopy(oldValues, toRemoveExisted + 1, newValues, toRemoveExisted, newLen - 1 - toRemoveExisted)
          newValues(newLen - 1) = toPut
          put(key, newValues, col)

          Array(toPut)
        }
      } else {
        if (toPutExisted != -1) { // none toRemove and toPut already there, do nothing
          Array(toPut)
        } else { // none toRemove and put no-existed
          val newValues = Array.ofDim[V](oldValues.length + 1)
          System.arraycopy(oldValues, 0, newValues, 0, oldValues.length)
          newValues(newValues.length - 1) = toPut
          put(key, newValues, col)

          Array(toPut)
        }
      }
    }
  }

  private def findValue(values: Array[V], value: V): Int = {
    val len = values.length
    var idx = -1
    var i = 0
    while (i < len && idx == -1) {
      if (values(i) == value) {
        idx = i
      } else {
        i += 1
      }
    }

    idx
  }

  private def findValues(values: Array[V], value1: V, value2: V): (Int, Int) = {
    val len = values.length
    var idx1 = -1
    var idx2 = -1
    var i = 0
    while (i < len && idx1 == -1 && idx2 == -1) {
      val value = values(i)
      if (value == value1) {
        idx1 = i
      }
      if (value == value2) {
        idx2 = i
      }
      i += 1
    }

    (idx1, idx2)
  }

  def remove(key: Int, col: Int): Array[V] = {
    if (key == FREE_KEY) {
      if (!m_hasFreeKey(col)) {
        NO_VALUE
      } else {
        m_hasFreeKey(col) = false
        val ret = m_freeValue(col)
        m_freeValue(col) = NO_VALUE
        m_size -= 1
        ret
      }
    } else {
      val idx = getReadIndex(key)
      if (idx == -1) {
        NO_VALUE
      } else {
        val res = m_values(col)(idx)
        m_values(col)(idx) = NO_VALUE
        shiftKeys(idx, col)
        m_size -= 1
        res
      }
    }
  }

  def size = m_size

  private def shiftKeys(_pos: Int, col: Int): Int = {
    var pos = _pos
    // shift entries with the same hash.
    var last = 0
    var slot = 0
    var k = 0
    val keys = this.m_keys
    while (true) {
      last = pos
      pos = getNextIndex(pos)
      var break = false
      while (!break) {
        k = keys(pos)
        if (k == FREE_KEY) {
          keys(last) = FREE_KEY
          m_values(col)(last) = NO_VALUE
          return last
        }
        slot = getStartIndex(k) // calculate the starting slot for the current key
        if (if (last <= pos) last >= slot || slot > pos else last >= slot && slot > pos) {
          break = true
        } else {
          pos = getNextIndex(pos)
        }
      }
      keys(last) = k
      m_values(col)(last) = m_values(col)(pos)
    }

    // should not be here
    last
  }

  /**
   * Find key position in the map.
   * @param key Key to look for
   * @return Key position or -1 if not found
   */
  private def getReadIndex(key: Int): Int = {
    var idx = getStartIndex(key)
    if (m_keys(idx) == key) { // we check FREE prior to this call
      return idx
    }

    if (m_keys(idx) == FREE_KEY) { // end of chain already
      return -1
    }

    val startIdx = idx

    idx = getNextIndex(idx)
    while (idx != startIdx) {
      if (m_keys(idx) == FREE_KEY) {
        return -1
      } else if (m_keys(idx) == key) {
        return idx
      } else {
        idx = getNextIndex(idx)
      }
    }

    -1
  }

  /**
   * Find an index of a cell which should be updated by 'put' operation.
   * It can be:
   * 1) a cell with a given key
   * 2) first free cell in the chain
   * @param key Key to look for
   * @return Index of a cell to be updated by a 'put' operation
   */
  private def getPutIndex(key: Int): Int = {
    val readIdx = getReadIndex(key)
    if (readIdx >= 0) {
      return readIdx
    }

    // key not found, find insertion point
    val startIdx = getStartIndex(key)
    if (m_keys(startIdx) == FREE_KEY) {
      return startIdx
    }

    var idx = startIdx
    while (m_keys(idx) != FREE_KEY) {
      idx = getNextIndex(idx)
      if (idx == startIdx) {
        return -1
      }
    }

    idx
  }

  private def rehash() {
    val oldLength = m_keys.length
    val oldKeys = m_keys
    val oldValues = m_values

    expendCapacity(isInit = false)

    m_size = if (m_hasFreeKey(0)) 1 else 0
    var i = 0
    while (i < oldLength) {
      val oldKey = oldKeys(i)
      if (oldKey != FREE_KEY) {
        var n = 0
        while (n < nValues) {
          put(oldKey, oldValues(n)(i), n)
          n += 1
        }
      }
      i += 1
    }
  }

  private def expendCapacity(isInit: Boolean) {
    m_capacity = if (isInit) {
      IntIntMap.arraySize(initSize, fillFactor)
    } else {
      if (isCapacityByPowTwo) {
        (m_capacity * 2)
      } else {
        (m_capacity * 1.2).toInt
      }
    }

    m_mask = if (isCapacityByPowTwo) m_capacity - 1 else m_capacity

    m_threshold = (m_capacity * fillFactor).toInt

    m_keys = Array.ofDim[Int](m_capacity)
    m_values = Array.ofDim[Array[Array[V]]](nValues)
    // should filled m_values(n) with NO_VALUE, otherwise, for example, m_value(0)
    // filled by key1 may left m_value(1), m_value(2) to be null
    var n = 0
    while (n < nValues) {
      m_values(n) = Array.fill(m_capacity)(NO_VALUE)
      n += 1
    }
  }

  private def getStartIndex(key: Int): Int = {
    if (isCapacityByPowTwo) {
      IntIntMap.phiMix(key) & m_mask
    } else {
      IntIntMap.phiMix(key) % m_mask
    }
  }

  private def getNextIndex(currentIndex: Int): Int = {
    if (isCapacityByPowTwo) {
      (currentIndex + 1) & m_mask
    } else {
      (currentIndex + 1) % m_mask
    }
  }
}
