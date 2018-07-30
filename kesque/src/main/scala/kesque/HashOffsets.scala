package kesque

import java.util.concurrent.locks.ReentrantReadWriteLock

object HashOffsets {
  type V = Int // unsigned int could be 2^32 = 4,294,967,296

  // --- simple test
  def main(args: Array[String]) {
    val max = 1000000
    val map = new HashOffsets(200, 3)

    var n = 0
    while (n < 3) {
      println(s"n is $n")
      // put i and -i
      var i = -max
      while (i <= max) {
        map.put(i, i, n)
        map.put(i, -i, n)
        i += 1
      }

      i = -max
      while (i <= max) {
        if (i == -i) {
          if (!java.util.Arrays.equals(map.get(i, n), Array(i))) {
            println(s"err at $i - ${map.get(i, n).mkString("[", ",", "]")}")
          }
        } else {
          if (!java.util.Arrays.equals(map.get(i, n), Array(i, -i))) {
            println(s"err at $i - ${map.get(i, n).mkString("[", ",", "]")}")
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
          if (map.get(i, n) != IntIntsMap.NO_VALUE) {
            println(s"Remove value -$i from map: err at $i - ${map.get(i, n).mkString("[", ",", "]")}")
          }
        } else {
          if (!java.util.Arrays.equals(map.get(i, n), Array(i))) {
            println(s"Remove value -$i from map: err at $i - ${map.get(i, n).mkString("[", ",", "]")}")
          }
        }
        i += 1
      }

      // remove all value
      i = -max
      while (i <= max) {
        map.remove(i, n)
        i += 1
      }
      i = -max
      while (i <= max) {
        if (map.get(i, n) != IntIntsMap.NO_VALUE) {
          println(s"Remove all value: err at $i - ${map.get(i, n).mkString("[", ",", "]")}")
        }
        i += 1
      }

      n += 1
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

  def get(key: Int, valueIndex: Int): Array[V] = {
    try {
      readLock.lock()

      multipleValuesMap.get(key, valueIndex) match {
        case IntIntsMap.NO_VALUE =>
          singleValueMap.get(key, valueIndex) match {
            case IntIntMap.NO_VALUE => IntIntsMap.NO_VALUE
            case value              => Array(value)
          }
        case values => values
      }
    } finally {
      readLock.unlock()
    }
  }

  def put(key: Int, value: V, valueIndex: Int): Array[V] = {
    try {
      writeLock.lock()

      multipleValuesMap.get(key, valueIndex) match {
        case IntIntsMap.NO_VALUE =>
          singleValueMap.get(key, valueIndex) match {
            case IntIntMap.NO_VALUE => Array(singleValueMap.put(key, value, valueIndex))
            case existed =>
              singleValueMap.remove(key, valueIndex)
              multipleValuesMap.put(key, existed, valueIndex)
              multipleValuesMap.put(key, value, valueIndex)
          }
        case _ =>
          multipleValuesMap.put(key, value, valueIndex)
      }
    } finally {
      writeLock.unlock()
    }
  }

  def removeValue(key: Int, value: V, valueIndex: Int): Array[V] = {
    try {
      writeLock.lock()

      multipleValuesMap.get(key, valueIndex) match {
        case IntIntsMap.NO_VALUE =>
          singleValueMap.get(key, valueIndex) match {
            case IntIntMap.NO_VALUE => IntIntsMap.NO_VALUE
            case existedValue       => Array(singleValueMap.remove(key, valueIndex))
          }
        case _ => multipleValuesMap.removeValue(key, value, valueIndex)
      }
    } finally {
      writeLock.unlock()
    }
  }

  def replace(key: Int, toRemove: V, toPut: V, valueIndex: Int): Array[V] = {
    try {
      writeLock.lock()

      if (toRemove == toPut) {
        Array(toPut)
      } else {
        multipleValuesMap.get(key, valueIndex) match {
          case IntIntsMap.NO_VALUE =>
            singleValueMap.get(key, valueIndex) match {
              case IntIntMap.NO_VALUE => Array(singleValueMap.put(key, toPut, valueIndex))
              case existed =>
                singleValueMap.remove(key, valueIndex)
                multipleValuesMap.put(key, existed, valueIndex)
                multipleValuesMap.replace(key, toRemove, toPut, valueIndex)
            }
          case _ =>
            multipleValuesMap.replace(key, toRemove, toPut, valueIndex)
        }
      }
    } finally {
      writeLock.unlock()
    }
  }

  def remove(key: Int, valueIndex: Int): Array[V] = {
    try {
      writeLock.lock()

      multipleValuesMap.remove(key, valueIndex) match {
        case IntIntsMap.NO_VALUE =>
          singleValueMap.remove(key, valueIndex) match {
            case IntIntMap.NO_VALUE => IntIntsMap.NO_VALUE
            case existedValue       => Array(existedValue)
          }
        case _ => multipleValuesMap.remove(key, valueIndex)
      }
    } finally {
      writeLock.unlock()
    }
  }

  def size = singleValueMap.size + multipleValuesMap.size
}
