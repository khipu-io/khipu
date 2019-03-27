package kesque

import java.util.Arrays
import java.util.concurrent.locks.ReentrantReadWriteLock

object HashOffsets {
  type V = Int // unsigned int could be 2^32 = 4,294,967,296

  // --- simple test
  def main(args: Array[String]) {
    val max = 1000000
    val map = new HashOffsets(200, 3)

    var col = 0
    while (col < 3) {
      println(s"n is $col")
      // put i and -i
      var i = -max
      while (i <= max) {
        map.put(i, i, col)
        map.put(i, -i, col)
        i += 1
      }

      i = -max
      while (i <= max) {
        if (i == -i) {
          if (!Arrays.equals(map.get(i, col), Array(i))) {
            println(s"err at $i - ${map.get(i, col).mkString("[", ",", "]")}")
          }
        } else {
          if (!Arrays.equals(map.get(i, col), Array(i, -i))) {
            println(s"err at $i - ${map.get(i, col).mkString("[", ",", "]")}")
          }
        }
        i += 1
      }
      println(map.get(max, col).mkString(","))
      println(map.get(1, col).mkString(","))
      println(map.get(max - 1, col).mkString(","))

      // remove value -i from map
      i = -max
      while (i <= max) {
        map.removeValue(i, -i, col)
        i += 1
      }
      i = -max
      while (i <= max) {
        if (i == -i) {
          if (map.get(i, col) != IntIntsMap.NO_VALUE) {
            println(s"Remove value -$i from map: err at $i - ${map.get(i, col).mkString("[", ",", "]")}")
          }
        } else {
          if (!Arrays.equals(map.get(i, col), Array(i))) {
            println(s"Remove value -$i from map: err at $i - ${map.get(i, col).mkString("[", ",", "]")}")
          }
        }
        i += 1
      }

      // remove all value
      i = -max
      while (i <= max) {
        map.remove(i, col)
        if (map.get(i, col) != IntIntsMap.NO_VALUE) {
          println(s"Remove all value: err at $i - ${map.get(i, col).mkString("[", ",", "]")}")
        }
        i += 1
      }

      col += 1
    }
  }
}

/**
 * It's actually an int -> ints hashmap by combining
 * an int -> int map and an int -> ints map.
 */
final class HashOffsets(initSize: Int, nValues: Int = 1, fillFactor: Float = 0.75f) {
  import HashOffsets._

  private val singleValueMap = new IntIntMap(initSize, nValues, fillFactor)
  private val multipleValuesMap = new IntIntsMap(initSize, nValues, fillFactor)

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  def get(key: Int, col: Int): Array[V] = {
    try {
      readLock.lock()

      multipleValuesMap.get(key, col) match {
        case IntIntsMap.NO_VALUE =>
          singleValueMap.get(key, col) match {
            case IntIntMap.NO_VALUE => IntIntsMap.NO_VALUE
            case value              => Array(value)
          }
        case values => values
      }
    } finally {
      readLock.unlock()
    }
  }

  def put(key: Int, value: V, col: Int): Array[V] = {
    try {
      writeLock.lock()

      multipleValuesMap.get(key, col) match {
        case IntIntsMap.NO_VALUE =>
          singleValueMap.get(key, col) match {
            case IntIntMap.NO_VALUE => Array(singleValueMap.put(key, value, col))
            case existed =>
              singleValueMap.remove(key, col)
              multipleValuesMap.put(key, existed, col)
              multipleValuesMap.put(key, value, col)
          }
        case _ =>
          multipleValuesMap.put(key, value, col)
      }
    } finally {
      writeLock.unlock()
    }
  }

  def removeValue(key: Int, value: V, col: Int): Array[V] = {
    try {
      writeLock.lock()

      multipleValuesMap.get(key, col) match {
        case IntIntsMap.NO_VALUE =>
          singleValueMap.get(key, col) match {
            case IntIntMap.NO_VALUE => IntIntsMap.NO_VALUE
            case existedValue       => Array(singleValueMap.remove(key, col))
          }
        case _ => multipleValuesMap.removeValue(key, value, col)
      }
    } finally {
      writeLock.unlock()
    }
  }

  def replace(key: Int, toRemove: V, toPut: V, col: Int): Array[V] = {
    try {
      writeLock.lock()

      if (toRemove == toPut) {
        Array(toPut)
      } else {
        multipleValuesMap.get(key, col) match {
          case IntIntsMap.NO_VALUE =>
            singleValueMap.get(key, col) match {
              case IntIntMap.NO_VALUE => Array(singleValueMap.put(key, toPut, col))
              case existed =>
                singleValueMap.remove(key, col)
                multipleValuesMap.put(key, existed, col)
                multipleValuesMap.replace(key, toRemove, toPut, col)
            }
          case _ =>
            multipleValuesMap.replace(key, toRemove, toPut, col)
        }
      }
    } finally {
      writeLock.unlock()
    }
  }

  def remove(key: Int, col: Int): Array[V] = {
    try {
      writeLock.lock()

      multipleValuesMap.remove(key, col) match {
        case IntIntsMap.NO_VALUE =>
          singleValueMap.remove(key, col) match {
            case IntIntMap.NO_VALUE => IntIntsMap.NO_VALUE
            case existedValue       => Array(existedValue)
          }
        case x => x
      }
    } finally {
      writeLock.unlock()
    }
  }

  def removeValues(col: Int)(cond: (Int, Int) => Boolean) {
    try {
      readLock.lock()

      multipleValuesMap.removeValues(col)(cond)
      singleValueMap.removeValues(col)(cond)
    } finally {
      readLock.unlock()
    }
  }

  def size = singleValueMap.size + multipleValuesMap.size
}
