package kesque

import java.nio.ByteBuffer
import kafka.utils.Logging
import org.rocksdb.OptimisticTransactionDB
import org.rocksdb.ReadOptions
import org.rocksdb.Transaction
import org.rocksdb.WriteOptions

final class KesqueIndexRocksdb(table: OptimisticTransactionDB, topic: String) extends KesqueIndex with Logging {
  import KesqueIndex._

  private val writeOptions = new WriteOptions()
  private val readOptions = new ReadOptions()

  def get(key: Array[Byte]): List[Long] = {
    val sKey = toShortKey(key)
    var offsets: List[Long] = Nil
    try {
      val offsets = table.get(readOptions, sKey) match {
        case null => Nil
        case x =>
          var data: List[Long] = Nil
          val bytes = ByteBuffer.wrap(x)
          while (bytes.remaining >= 8) {
            data ::= bytes.getLong()
          }
          data
      }

      //if (offsets.size > 1) {
      //  println(s"key: ${khipu.Hash(key)}, offsets: $offsets")
      //}

      offsets
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        offsets
    }
  }

  def put(key: Array[Byte], offset: Long) {
    val sKey = toShortKey(key)
    val data = table.get(readOptions, sKey) match {
      case null => ByteBuffer.allocate(8).putLong(offset).array
      case x =>
        val buf = ByteBuffer.allocate(x.length + 8).put(x).putLong(offset)
        buf.flip()
        buf.array
    }

    var txn: Transaction = null
    try {
      txn = table.beginTransaction(writeOptions)

      table.put(sKey, data)

      txn.commit()
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (txn ne null) {
          txn.rollback()
        }
    } finally {
      if (txn ne null) {
        txn.close()
      }
    }
  }

  def put(kvs: Iterable[(Array[Byte], Long)]) {
    var txn: Transaction = null
    try {
      txn = table.beginTransaction(writeOptions)

      kvs foreach {
        case (key, offset) =>
          val sKey = toShortKey(key)
          val data = table.get(readOptions, sKey) match {
            case null =>
              ByteBuffer.allocate(8).putLong(offset).array
            case x =>
              val buf = ByteBuffer.allocate(x.length + 8).put(x).putLong(offset)
              buf.flip()
              buf.array
          }

          table.put(sKey, data)
      }

      txn.commit()
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (txn ne null) {
          txn.rollback()
        }
    } finally {
      if (txn ne null) {
        txn.close()
      }
    }
  }

  def remove(key: Array[Byte], offset: Long) {
    val sKey = toShortKey(key)
    val data = table.get(readOptions, sKey) match {
      case null =>
      case x =>
        val buf = ByteBuffer.allocate(x.length)
        val offsets = ByteBuffer.wrap(x)
        while (offsets.remaining >= 8) {
          val offsetx = offsets.getLong()
          if (offsetx != offset) {
            buf.putLong(offsetx)
          }
        }
        buf.flip()
        val bytes = buf.array

        var txn: Transaction = null
        try {
          txn = table.beginTransaction(writeOptions)

          if (bytes.length >= 8) {
            table.put(sKey, bytes)
          } else {
            table.delete(sKey)
          }

          txn.commit()
        } catch {
          case ex: Throwable =>
            error(ex.getMessage, ex)
            if (txn ne null) {
              txn.rollback()
            }
        } finally {
          if (txn ne null) {
            txn.close()
          }
        }

    }

  }

  def count = {
    // TODO
  }
}
