package khipu.storage.datasource

import java.io.File
import org.iq80.leveldb.{ DB, Options, WriteOptions }
import khipu.config.LeveldbConfig
import khipu.util.FIFOCache
import org.iq80.leveldb.impl.Iq80DBFactory

object LeveldbKeyValueDataSource {

  private def createDB(levelDbConfig: LeveldbConfig): DB = {
    import levelDbConfig._

    val options = new Options()
      .compressionType(org.iq80.leveldb.CompressionType.SNAPPY)
      .createIfMissing(createIfMissing)
      .paranoidChecks(paranoidChecks) // raise an error as soon as it detects an internal corruption
      .verifyChecksums(verifyChecksums) // force checksum verification of all data that is read from the file system on behalf of a particular read
      .cacheSize(cacheSize)

    Iq80DBFactory.factory.open(new File(path), options)
  }

  def apply(levelDbConfig: LeveldbConfig): LeveldbKeyValueDataSource = {
    new LeveldbKeyValueDataSource(createDB(levelDbConfig), levelDbConfig, 1000)
  }
}

final class LeveldbKeyValueDataSource(
    private var db:            DB,
    private val levelDbConfig: LeveldbConfig,
    cacheSize:                 Int
) extends KeyValueDataSource {
  type This = LeveldbKeyValueDataSource

  def topic = levelDbConfig.path

  private val cache = new FIFOCache[Array[Byte], Array[Byte]](cacheSize)

  /**
   * This function obtains the associated value to a key, if there exists one.
   *
   * @param namespace which will be searched for the key.
   * @param key
   * @return the value associated with the passed key.
   */
  override def get(key: Array[Byte]): Option[Array[Byte]] = {
    val start = System.nanoTime
    val value = db.get(key)
    clock.elapse(System.nanoTime - start)
    Option(value)
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
    val batch = db.createWriteBatch()
    toRemove.foreach { key => batch.delete(key) }
    toUpsert.foreach { case (key, value) => batch.put(key, value) }
    db.write(batch, new WriteOptions())
    this
  }

  /**
   * This function closes the DataSource, without deleting the files used by it.
   */
  override def stop() {
    // TODO sync db
  }

  // TODO
  def count = -1

  def cacheHitRate = cache.hitRate
  def cacheReadCount = cache.readCount
  def resetCacheHitRate() = cache.resetHitRate()
}

