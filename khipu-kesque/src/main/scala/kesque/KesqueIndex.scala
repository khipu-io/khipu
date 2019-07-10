package kesque

import java.nio.ByteBuffer
import kafka.utils.Logging
import org.lmdbjava.Cursor
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.GetOp
import org.lmdbjava.Txn
import org.lmdbjava.SeekOp

object KesqueIndex {
  private val KEY_SIZE = 8
  private val VAL_SIZE = 8 // long, offset
}
final class KesqueIndex(env: Env[ByteBuffer], topic: String) extends Logging {
  import KesqueIndex._

  val table = env.openDbi(
    topic,
    DbiFlags.MDB_CREATE,
    DbiFlags.MDB_INTEGERKEY,
    DbiFlags.MDB_DUPSORT,
    DbiFlags.MDB_INTEGERDUP,
    DbiFlags.MDB_DUPFIXED
  )

  def get(key: Array[Byte]): List[Long] = {
    val kBuf = ByteBuffer.allocateDirect(KEY_SIZE)

    val start = System.nanoTime
    val sKey = toShortKey(key)
    kBuf.put(sKey).flip()

    var txn: Txn[ByteBuffer] = null
    var cursor: Cursor[ByteBuffer] = null
    var offsets: List[Long] = Nil
    try {
      txn = env.txnRead()

      cursor = table.openCursor(txn)
      if (cursor.get(kBuf, GetOp.MDB_SET_KEY)) {
        val data = Array.ofDim[Byte](cursor.`val`.remaining)
        val offset = cursor.`val`.getLong()
        offsets ::= offset

        while (cursor.seek(SeekOp.MDB_NEXT_DUP)) {
          val data = Array.ofDim[Byte](cursor.`val`.remaining)
          val offset = cursor.`val`.getLong()
          offsets ::= offset
        }
      }

      kBuf.clear()

      txn.commit()
      offsets
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (txn ne null) txn.abort()
        offsets
    } finally {
      if (cursor ne null) cursor.close()
      if (txn ne null) txn.close()
    }
  }

  def put(key: Array[Byte], offset: Long) = {
    val kBuf = ByteBuffer.allocateDirect(KEY_SIZE)
    val vBuf = ByteBuffer.allocateDirect(VAL_SIZE)

    var txn: Txn[ByteBuffer] = null
    try {
      txn = env.txnWrite()
      val sKey = toShortKey(key)
      kBuf.put(sKey).flip()
      vBuf.putLong(offset).flip()

      table.put(txn, kBuf, vBuf)
      kBuf.clear()
      vBuf.clear()
      txn.commit()
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (txn ne null) txn.abort()
    } finally {
      if (txn ne null) txn.close()
    }
  }

  def put(kvs: Iterable[(Array[Byte], Long)]) {
    val kBuf = ByteBuffer.allocateDirect(KEY_SIZE)
    val vBuf = ByteBuffer.allocateDirect(VAL_SIZE)

    var txn: Txn[ByteBuffer] = null
    try {
      txn = env.txnWrite()
      kvs foreach {
        case (key, offset) =>
          val sKey = toShortKey(key)
          kBuf.put(sKey).flip()
          vBuf.putLong(offset).flip()

          table.put(txn, kBuf, vBuf)
          kBuf.clear()
          vBuf.clear()
      }
      txn.commit()
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (txn ne null) txn.abort()
    } finally {
      if (txn ne null) txn.close()
    }
  }

  def remove(key: Array[Byte], offset: Long) {
    val kBuf = ByteBuffer.allocateDirect(KEY_SIZE)
    val vBuf = ByteBuffer.allocateDirect(VAL_SIZE)

    var txn: Txn[ByteBuffer] = null
    try {
      txn = env.txnWrite()
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
        if (txn ne null) txn.abort()
    } finally {
      if (txn ne null) txn.close()
    }
  }

  private def toShortKey(bytes: Array[Byte]) = {
    val slice = Array.ofDim[Byte](KEY_SIZE)
    System.arraycopy(bytes, 0, slice, 0, KEY_SIZE)
    slice
  }

}
