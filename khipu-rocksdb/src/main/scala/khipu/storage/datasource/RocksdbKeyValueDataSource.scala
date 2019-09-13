package khipu.storage.datasource

import akka.actor.ActorSystem
import akka.event.Logging
import java.io.File
import khipu.Hash
import khipu.config.RocksdbConfig
import khipu.util.BytesUtil
import khipu.util.FIFOCache
import org.rocksdb.BlockBasedTableConfig
import org.rocksdb.BloomFilter
import org.rocksdb.OptimisticTransactionDB
import org.rocksdb.Options
import org.rocksdb.ReadOptions
import org.rocksdb.RocksDB
import org.rocksdb.RocksDBException
import org.rocksdb.Transaction
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions

final class RocksdbKeyValueDataSource(
    val topic:     String,
    rocksdbConfig: RocksdbConfig,
    cacheSize:     Int
)(implicit system: ActorSystem) extends KeyValueDataSource {
  type This = RocksdbKeyValueDataSource
  RocksDB.loadLibrary()

  private val log = Logging(system, this.getClass)

  private val cache = new FIFOCache[Hash, Array[Byte]](cacheSize)

  private val table = createDB()

  private def createDB(): OptimisticTransactionDB = {
    val home = {
      val h = new File(rocksdbConfig.path)
      if (!h.exists) {
        h.mkdirs()
      }
      h
    }

    val path = new File(home, topic)
    if (!path.exists) {
      path.mkdirs()
    }

    val parallelism = math.max(Runtime.getRuntime.availableProcessors, 2)

    val tableOptions = new BlockBasedTableConfig()
      .setFilterPolicy(new BloomFilter(10))

    val options = new Options()
      .setCreateIfMissing(true)
      .setMaxOpenFiles(-1)
      .setTableFormatConfig(tableOptions)
      .setAllowMmapReads(false) // not necessary for syncState and transactions data
      .setAllowMmapWrites(false)
      .setIncreaseParallelism(parallelism)
      .setMaxBackgroundJobs(parallelism)
      .setWriteBufferSize(rocksdbConfig.writeBufferSize * 1024 * 1024)
      .setMaxWriteBufferNumber(rocksdbConfig.maxWriteBufferNumber)
      .setMinWriteBufferNumberToMerge(2)

    OptimisticTransactionDB.open(options, path.getAbsolutePath)
  }

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
        var readOptions: ReadOptions = null
        val ret = try {
          readOptions = new ReadOptions()
          table.get(readOptions, key) match {
            case null => None
            case data => Some(data)
          }
        } catch {
          case ex: Throwable =>
            log.error(ex, ex.getMessage)
            None
        } finally {
          if (readOptions ne null) {
            readOptions.close()
          }
        }

        clock.elapse(System.nanoTime - start)

        ret foreach { data => cache.put(Hash(key), data) }
        ret

      case some => some
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
    var writeOptions: WriteOptions = null
    var batch: WriteBatch = null
    var txn: Transaction = null
    try {
      writeOptions = new WriteOptions()
      txn = table.beginTransaction(writeOptions)
      batch = new WriteBatch()

      toRemove foreach {
        key => batch.delete(key)
      }

      toUpsert foreach {
        case (key, value) => batch.put(key, value)
      }

      table.write(writeOptions, batch)
      txn.commit()

      toRemove foreach {
        key => cache.remove(Hash(key))
      }

      toUpsert foreach {
        case (key, value) => cache.put(Hash(key), value)
      }
    } catch {
      case ex: Throwable =>
        if (txn ne null) {
          txn.rollback()
        }
        log.error(ex, ex.getMessage)
    } finally {
      if (txn ne null) {
        txn.close()
      }
      if (batch ne null) {
        batch.close()
      }
      if (writeOptions ne null) {
        writeOptions.close()
      }
    }

    this
  }

  def count = {
    try {
      table.getLongProperty("rocksdb.estimate-num-keys")
    } catch {
      case ex: RocksDBException =>
        log.error(ex, ex.getMessage)
        0
    }
  }

  /**
   * This function closes the DataSource, without deleting the files used by it.
   */
  override def stop() {
    table.flushWal(true)
  }

  def cacheHitRate = cache.hitRate
  def cacheReadCount = cache.readCount
  def resetCacheHitRate() = cache.resetHitRate()

  def printTable(namespace: Array[Byte], keyOption: Option[Array[Byte]] = None) {
    println(s"table '$topic' content of namespace '${new String(namespace)}', ${keyOption.map(new String(_))}")
    println("====================================")
    try {
      val itr = table.newIterator()
      keyOption match {
        case Some(k) => itr.seek(BytesUtil.concat(namespace, k))
        case None    => itr.seekToFirst
      }
      var done = false
      while (itr.isValid && !done) {
        val key = itr.key
        val (ns, k) = BytesUtil.split(key, namespace.length)
        if (java.util.Arrays.equals(ns, namespace)) {
          keyOption match {
            case Some(x) =>
              if (java.util.Arrays.equals(x, k)) {
                val data = itr.value
                println(s"${new String(k)} -> ${data.mkString(",")}")
              }
              done = true
            case None =>
              val data = itr.value
              println(s"${new String(k)} -> ${data.mkString(",")}")
              itr.next()
          }
        } else {
          done = true
        }
      }
      itr.close()
    } catch {
      case ex: Throwable => ex.printStackTrace()
    }
    println("====================================")
  }
}

