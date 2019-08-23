package kesque

import java.nio.ByteBuffer
import kafka.utils.Logging
import org.lmdbjava.Cursor
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.GetOp
import org.lmdbjava.Txn
import org.lmdbjava.SeekOp

final class KesqueIndexLmdb(env: Env[ByteBuffer], topic: String) extends KesqueIndex with Logging {
  import KesqueIndex._

  val table = env.openDbi(
    topic,
    DbiFlags.MDB_CREATE,
    DbiFlags.MDB_INTEGERKEY,
    DbiFlags.MDB_INTEGERDUP,
    DbiFlags.MDB_DUPSORT,
    DbiFlags.MDB_DUPFIXED
  )

  def get(key: Array[Byte]): List[Long] = {
    val kBuf = ByteBuffer.allocateDirect(KEY_SIZE)

    val sKey = toShortKey(key)
    kBuf.put(sKey).flip()

    var txn: Txn[ByteBuffer] = null
    var cursor: Cursor[ByteBuffer] = null
    var offsets: List[Long] = Nil
    try {
      txn = env.txnRead()

      cursor = table.openCursor(txn)
      if (cursor.get(kBuf, GetOp.MDB_SET_KEY)) {
        val offset = cursor.`val`.getLong()
        offsets ::= offset

        while (cursor.seek(SeekOp.MDB_NEXT_DUP)) {
          val offset = cursor.`val`.getLong()
          offsets ::= offset
        }
      }

      txn.commit()

      kBuf.clear()

      //if (offsets.size > 1) {
      //  println(s"key: ${khipu.Hash(key)}, offsets: $offsets")
      //}

      offsets
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (txn ne null) {
          txn.abort()
        }
        offsets
    } finally {
      if (cursor ne null) {
        cursor.close()
      }
      if (txn ne null) {
        txn.close()
      }
    }
  }

  def put(key: Array[Byte], offset: Long) {
    var txn: Txn[ByteBuffer] = null
    try {
      txn = env.txnWrite()
      val kBuf = ByteBuffer.allocateDirect(KEY_SIZE)
      val vBuf = ByteBuffer.allocateDirect(VAL_SIZE)

      val sKey = toShortKey(key)
      kBuf.put(sKey).flip()
      vBuf.putLong(offset).flip()

      table.put(txn, kBuf, vBuf)

      txn.commit()

      kBuf.clear()
      vBuf.clear()
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (txn ne null) {
          txn.abort()
        }
    } finally {
      if (txn ne null) {
        txn.close()
      }
    }
  }

  def put(kvs: Iterable[(Array[Byte], Long)]) {
    var txn: Txn[ByteBuffer] = null
    try {
      txn = env.txnWrite()
      var bufs: List[ByteBuffer] = Nil
      kvs foreach {
        case (key, offset) =>
          val kBuf = ByteBuffer.allocateDirect(KEY_SIZE)
          val vBuf = ByteBuffer.allocateDirect(VAL_SIZE)
          bufs ::= kBuf
          bufs ::= vBuf

          val sKey = toShortKey(key)
          kBuf.put(sKey).flip()
          vBuf.putLong(offset).flip()

          table.put(txn, kBuf, vBuf)
      }
      txn.commit()

      bufs foreach (_.clear)
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (txn ne null) {
          txn.abort()
        }
    } finally {
      if (txn ne null) {
        txn.close()
      }
    }
  }

  def remove(key: Array[Byte], offset: Long) {
    var txn: Txn[ByteBuffer] = null
    try {
      txn = env.txnWrite()
      val kBuf = ByteBuffer.allocateDirect(KEY_SIZE)
      val vBuf = ByteBuffer.allocateDirect(VAL_SIZE)

      val sKey = toShortKey(key)
      kBuf.put(sKey).flip()
      vBuf.putLong(offset).flip()

      table.delete(txn, kBuf, vBuf)

      kBuf.clear()
      vBuf.clear()
      txn.commit()
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (txn ne null) {
          txn.abort()
        }
    } finally {
      if (txn ne null) {
        txn.close()
      }
    }
  }

  def count = {
    val rtx = env.txnRead()
    val stat = table.stat(rtx)
    val ret = stat.entries
    rtx.commit()
    rtx.close()
    ret
  }
}
