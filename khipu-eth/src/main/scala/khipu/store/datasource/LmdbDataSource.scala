package khipu.store.datasource

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import kesque.FIFOCache
import khipu.Hash
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

final case class LmdbDataSource(topic: String, env: Env[ByteBuffer], cacheSize: Int = 10000)(implicit system: ActorSystem) extends DataSource {
  private val log = Logging(system, this.getClass)

  private val cache = new FIFOCache[Hash, Array[Byte]](cacheSize)

  var table = createTable()

  private def createTable(): Dbi[ByteBuffer] = env.openDbi(topic, DbiFlags.MDB_CREATE)

  /**
   * This function obtains the associated value to a key, if there exists one.
   *
   * @param namespace which will be searched for the key.
   * @param key
   * @return the value associated with the passed key.
   */
  override def get(namespace: Array[Byte], key: Array[Byte]): Option[Array[Byte]] = {
    val start = System.nanoTime

    val combKey = BytesUtil.concat(namespace, key)

    cache.get(Hash(combKey)) match {
      case None =>
        // TODO fetch keyBuf from a pool?

        var ret: Option[Array[Byte]] = None
        var rtx: Txn[ByteBuffer] = null
        try {
          rtx = env.txnRead()

          val tableKey = ByteBuffer.allocateDirect(combKey.length)
          tableKey.put(combKey).flip()
          val data = table.get(rtx, tableKey)
          ret = if (data ne null) {
            val value = Array.ofDim[Byte](data.remaining)
            data.get(value)
            Some(value)
          } else {
            None
          }

          rtx.commit()
        } catch {
          case ex: Throwable =>
            if (rtx ne null) rtx.abort()
            log.error(ex, ex.getMessage)
        } finally {
          if (rtx ne null) rtx.close()
        }

        clock.elapse(System.nanoTime - start)

        ret

      case x => x
    }
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

    var wtx: Txn[ByteBuffer] = null
    try {
      wtx = env.txnWrite()

      toRemove foreach { key =>
        val combKey = BytesUtil.concat(namespace, key)

        val tableKey = ByteBuffer.allocateDirect(combKey.length)
        tableKey.put(combKey).flip()
        table.delete(wtx, tableKey)
      }

      toUpsert foreach {
        case (key, value) =>
          val combKey = BytesUtil.concat(namespace, key)

          val tableKey = ByteBuffer.allocateDirect(combKey.length)
          val tableVal = ByteBuffer.allocateDirect(value.length)

          tableKey.put(combKey).flip()
          tableVal.put(value).flip()
          table.put(wtx, tableKey, tableVal)
      }

      wtx.commit()

      toRemove foreach {
        key => cache.remove(Hash(BytesUtil.concat(namespace, key)))
      }

      toUpsert foreach {
        case (key, value) => cache.put(Hash(BytesUtil.concat(namespace, key)), value)
      }
    } catch {
      case ex: Throwable =>
        if (wtx ne null) {
          wtx.abort()
        }
        log.error(ex, ex.getMessage)
    } finally {
      if (wtx ne null) {
        wtx.close()
      }
    }

    this
  }

  def count = {
    val rtx = env.txnRead()
    val stat = table.stat(rtx)
    val ret = stat.entries
    rtx.commit()
    rtx.close()
    ret
  }

  /**
   * This function updates the DataSource by deleting all the (key-value) pairs in it.
   *
   * @return the new DataSource after all the data was removed.
   */
  override def clear(): DataSource = {
    destroy()
    this.table = createTable()
    this
  }

  /**
   * This function closes the DataSource, without deleting the files used by it.
   */
  override def close() = table.close()

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

  def printTable(namespace: Array[Byte]) {
    val txn = env.txnRead()
    val itr = table.iterate(txn)
    while (itr.hasNext) {
      val entry = itr.next()

      val key = new Array[Byte](entry.key.remaining)
      entry.key.get(key)
      val (ns, k) = BytesUtil.split(key, namespace.length)
      if (java.util.Arrays.equals(ns, namespace)) {
        val data = new Array[Byte](entry.`val`.remaining)
        entry.`val`.get(data)

        println(s"${new String(k)} -> ${data.mkString(",")}")
      }
    }
    itr.close()
    txn.commit()
    txn.close()
  }
}

