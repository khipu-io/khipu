package khipu.store.datasource

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import khipu.util.BytesUtil
import org.lmdbjava.Env
import org.lmdbjava.Dbi
import org.lmdbjava.DbiFlags
import org.lmdbjava.Txn

trait LmdbConfig {
  val path: String
  val mapSize: Long
  val maxDbs: Int
  val maxReaders: Int
}

final case class LmdbDataSource(env: Env[ByteBuffer], lmdbConfig: LmdbConfig)(implicit system: ActorSystem) extends DataSource {
  private val log = Logging(system, this.getClass)

  private var db = createDb()

  private def createDb(): Dbi[ByteBuffer] = env.openDbi("shared", DbiFlags.MDB_CREATE)

  /**
   * This function obtains the associated value to a key, if there exists one.
   *
   * @param namespace which will be searched for the key.
   * @param key
   * @return the value associated with the passed key.
   */
  override def get(namespace: Array[Byte], key: Array[Byte]): Option[Array[Byte]] = {
    val start = System.nanoTime

    // TODO fetch keyBuf from a pool?
    val keyBuf = ByteBuffer.allocateDirect(env.getMaxKeySize)

    var txn: Txn[ByteBuffer] = null
    var ret: Option[Array[Byte]] = None
    try {
      txn = env.txnRead()

      keyBuf.put(BytesUtil.concat(namespace, key)).flip()
      val data = db.get(txn, keyBuf)
      val ret = if (data ne null) {
        val value = Array.ofDim[Byte](data.remaining)
        data.get(value)
        Some(value)
      } else {
        None
      }
      keyBuf.clear()

      txn.commit()
    } catch {
      case ex: Throwable =>
        if (txn ne null) txn.abort()
        log.error(ex, ex.getMessage)
    } finally {
      if (txn ne null) txn.close()
    }

    clock.elapse(System.nanoTime - start)

    ret
  }

  /**
   * This function updates the DataSource by deleting, updating and inserting new (key-value) pairs.
   *
   * @param namespace from which the (key-value) pairs will be removed and inserted.
   * @param toRemove  which includes all the keys to be removed from the DataSource.
   * @param toUpsert  which includes all the (key-value) pairs to be inserted into the DataSource.
   *                  If a key is already in the DataSource its value will be updated.
   * @return the new DataSource after the removals and insertions were done.
   */
  override def update(namespace: Array[Byte], toRemove: Iterable[Array[Byte]], toUpsert: Iterable[(Array[Byte], Array[Byte])]): DataSource = {
    // TODO fetch keyBuf from a pool?
    val keyWriteBuf = ByteBuffer.allocateDirect(env.getMaxKeySize)
    var dataWriteBuf = ByteBuffer.allocateDirect(100 * 1024) // will grow when needed

    var txn: Txn[ByteBuffer] = null
    try {
      txn = env.txnWrite()

      toRemove foreach { key =>
        keyWriteBuf.put(BytesUtil.concat(namespace, key)).flip()
        db.delete(txn, keyWriteBuf)
        keyWriteBuf.clear()
      }

      toUpsert foreach {
        case (key, value) =>
          keyWriteBuf.put(BytesUtil.concat(namespace, key)).flip()
          dataWriteBuf = ensureValueBufferSize(dataWriteBuf, value.length)
          dataWriteBuf.put(value).flip()
          db.put(txn, keyWriteBuf, dataWriteBuf)
          keyWriteBuf.clear()
          dataWriteBuf.clear()
      }

      txn.commit()
    } catch {
      case ex: Throwable =>
        if (txn ne null) txn.abort()
        log.error(ex, ex.getMessage)
    } finally {
      if (txn ne null) txn.close()
    }

    this
  }

  /**
   * This function updates the DataSource by deleting all the (key-value) pairs in it.
   *
   * @return the new DataSource after all the data was removed.
   */
  override def clear(): DataSource = {
    destroy()
    this.db = createDb()
    this
  }

  /**
   * This function closes the DataSource, without deleting the files used by it.
   */
  override def close() = db.close()

  /**
   * This function closes the DataSource, if it is not yet closed, and deletes all the files used by it.
   */
  override def destroy() {
    try {
      close()
    } finally {
      //
    }
  }

  private def ensureValueBufferSize(buf: ByteBuffer, size: Int): ByteBuffer = {
    if (buf.remaining < size) {
      ByteBuffer.allocateDirect(size * 2)
    } else {
      buf
    }
  }
}

