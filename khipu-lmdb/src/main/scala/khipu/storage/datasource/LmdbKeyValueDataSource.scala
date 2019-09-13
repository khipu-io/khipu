package khipu.storage.datasource

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import khipu.Hash
import khipu.util.BytesUtil
import khipu.util.FIFOCache
import org.lmdbjava.Env
import org.lmdbjava.Dbi
import org.lmdbjava.DbiFlags
import org.lmdbjava.Txn

final class LmdbKeyValueDataSource(
    val topic: String,
    env:       Env[ByteBuffer],
    cacheSize: Int
)(implicit system: ActorSystem) extends KeyValueDataSource {
  type This = LmdbKeyValueDataSource

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
  override def get(key: Array[Byte]): Option[Array[Byte]] = {
    val start = System.nanoTime

    cache.get(Hash(key)) match {
      case None =>
        // TODO fetch keyBuf from a pool?

        var ret: Option[Array[Byte]] = None
        var rtx: Txn[ByteBuffer] = null
        try {
          rtx = env.txnRead()

          val tableKey = ByteBuffer.allocateDirect(key.length)
          tableKey.put(key).flip()
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

        ret foreach { data => cache.put(Hash(key), data) }
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
  override def update(toRemove: Iterable[Array[Byte]], toUpsert: Iterable[(Array[Byte], Array[Byte])]): This = {
    // TODO fetch keyBuf from a pool?

    var wtx: Txn[ByteBuffer] = null
    try {
      wtx = env.txnWrite()

      toRemove foreach {
        key =>
          val tableKey = ByteBuffer.allocateDirect(key.length)
          tableKey.put(key).flip()
          table.delete(wtx, tableKey)
      }

      toUpsert foreach {
        case (key, value) =>
          val tableKey = ByteBuffer.allocateDirect(key.length)
          val tableVal = ByteBuffer.allocateDirect(value.length)

          tableKey.put(key).flip()
          tableVal.put(value).flip()
          table.put(wtx, tableKey, tableVal)
      }

      wtx.commit()

      toRemove foreach {
        key => cache.remove(Hash(key))
      }

      toUpsert foreach {
        case (key, value) => cache.put(Hash(key), value)
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

  def cacheHitRate = cache.hitRate
  def cacheReadCount = cache.readCount
  def resetCacheHitRate() = cache.resetHitRate()

  /**
   * This function closes the DataSource, without deleting the files used by it.
   */
  override def stop() {
    // not necessary to close db, we'll call env.sync(true) to force sync 
  }

  private def ensureValueBufferSize(buf: ByteBuffer, size: Int): ByteBuffer = {
    if (buf.remaining < size) {
      ByteBuffer.allocateDirect(size * 2)
    } else {
      buf
    }
  }

  def printTable(namespace: Array[Byte], key: Option[Array[Byte]] = None) {
    println(s"table '$topic' content of namespace '${new String(namespace)}'")
    println("====================================")
    try {
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
    } catch {
      case ex: Throwable => ex.printStackTrace()
    }
    println("====================================")
  }
}

