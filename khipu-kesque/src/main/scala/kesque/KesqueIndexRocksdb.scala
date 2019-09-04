package kesque

import java.io.File
import java.nio.ByteBuffer
import kafka.utils.Logging
import khipu.config.RocksdbConfig
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

final class KesqueIndexRocksdb(rocksdbConfig: RocksdbConfig, topic: String, useShortKey: Boolean = true) extends KesqueIndex with Logging {
  RocksDB.loadLibrary()
  import KesqueIndex._

  private val table = {
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
      .setAllowMmapReads(true)
      .setAllowMmapWrites(false)
      .setIncreaseParallelism(parallelism) // The total number of threads to be used by RocksDB. A good value is the number of cores.
      .setMaxBackgroundJobs(parallelism)
      .setWriteBufferSize(rocksdbConfig.writeBufferSize * 1024 * 1024)
      .setMaxWriteBufferNumber(rocksdbConfig.maxWriteBufferNumber)
      .setMinWriteBufferNumberToMerge(rocksdbConfig.minWriteBufferNumberToMerge)

    OptimisticTransactionDB.open(options, path.getAbsolutePath)
  }

  def get(key: Array[Byte]): List[Long] = {
    val sKey = if (useShortKey) toShortKey(key) else key
    var readOptions: ReadOptions = null
    try {
      readOptions = new ReadOptions()
      val offsets = table.get(readOptions, sKey) match {
        case null => Nil
        case x =>
          val bytes = ByteBuffer.wrap(x)
          if (useShortKey) {
            var data: List[Long] = Nil
            while (bytes.remaining >= 8) {
              data ::= bytes.getLong()
            }
            data
          } else {
            List(ByteBuffer.wrap(x).getLong())
          }
      }

      //if (offsets.size > 1) {
      //  println(s"key: ${khipu.Hash(key)}, offsets: $offsets")
      //}

      offsets
    } catch {
      case ex: Throwable =>
        error(ex.getMessage, ex)
        if (readOptions ne null) {
          readOptions.close()
        }
        Nil
    }
  }

  def put(key: Array[Byte], offset: Long) {
    val sKey = if (useShortKey) toShortKey(key) else key
    var readOptions: ReadOptions = null
    var writeOptions: WriteOptions = null
    var txn: Transaction = null
    try {
      val data = if (useShortKey) {
        readOptions = new ReadOptions()
        table.get(readOptions, sKey) match {
          case null =>
            ByteBuffer.allocate(8).putLong(offset).array
          case x =>
            val buf = ByteBuffer.allocate(x.length + 8).put(x).putLong(offset)
            buf.flip()
            buf.array
        }
      } else {
        ByteBuffer.allocate(8).putLong(offset).array
      }

      writeOptions = new WriteOptions()
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
      if (readOptions ne null) {
        readOptions.close()
      }
      if (writeOptions ne null) {
        writeOptions.close()
      }
    }
  }

  def put(kvs: Iterable[(Array[Byte], Long)]) {
    var readOptions: ReadOptions = null
    var writeOptions: WriteOptions = null
    var txn: Transaction = null
    var batch: WriteBatch = null
    try {
      writeOptions = new WriteOptions()
      batch = new WriteBatch()
      txn = table.beginTransaction(writeOptions)

      kvs foreach {
        case (key, offset) =>
          val sKey = if (useShortKey) toShortKey(key) else key
          val data =
            if (useShortKey) {
              readOptions = new ReadOptions()
              table.get(readOptions, sKey) match {
                case null =>
                  ByteBuffer.allocate(8).putLong(offset).array
                case x =>
                  val buf = ByteBuffer.allocate(x.length + 8).put(x).putLong(offset)
                  buf.flip()
                  buf.array
              }
            } else {
              ByteBuffer.allocate(8).putLong(offset).array
            }

          batch.put(sKey, data)
      }

      table.write(writeOptions, batch)
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
      if (batch ne null) {
        batch.close()
      }
      if (writeOptions ne null) {
        writeOptions.close()
      }
      if (readOptions ne null) {
        readOptions.close()
      }
    }
  }

  def remove(key: Array[Byte], offset: Long) {
    val sKey = if (useShortKey) toShortKey(key) else key

    val action = if (useShortKey) {
      var readOptions: ReadOptions = null
      try {
        readOptions = new ReadOptions()
        table.get(readOptions, sKey) match {
          case null => (false, None)
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

            if (bytes.length >= 8) {
              (true, Some(bytes))
            } else {
              (true, None)
            }
        }
      } catch {
        case ex: Throwable =>
          error(ex.getMessage, ex)
          (false, None)
      } finally {
        if (readOptions ne null) {
          readOptions.close()
        }
      }
    } else {
      (true, None)
    }

    action match {
      case (false, _) =>
      case (true, putBytes) =>
        var writeOptions: WriteOptions = null
        var txn: Transaction = null
        try {
          writeOptions = new WriteOptions()
          txn = table.beginTransaction(writeOptions)

          putBytes match {
            case Some(bytes) => table.put(sKey, bytes)
            case None        => table.delete(sKey)
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
          if (writeOptions ne null) {
            writeOptions.close()
          }
        }
    }
  }

  def count: Long = {
    try {
      table.getLongProperty("rocksdb.estimate-num-keys")
    } catch {
      case ex: RocksDBException =>
        error(ex.getMessage, ex)
        0
    }
  }

  def terminate() {
    table.flushWal(true)
  }
}
