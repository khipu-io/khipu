package khipu.storage.datasource

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import com.typesafe.config.Config
import java.io.File
import khipu.config.CacheConfig
import khipu.config.DbConfig
import khipu.config.LmdbConfig
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags

trait LmdbDataSources extends SharedLmdbDataSources with DataSources {
  implicit protected val system: ActorSystem

  protected val config: Config
  protected val log: LoggingAdapter
  protected val datadir: String

  private lazy val lmdbConfig = new LmdbConfig(datadir, config.getConfig("db").getConfig("lmdb"))

  private lazy val cacheConf = CacheConfig(config)

  private lazy val home = {
    val h = new File(lmdbConfig.path)
    if (!h.exists) {
      h.mkdirs()
    }
    h
  }

  lazy val lmdbEnv = Env.create()
    .setMapSize(lmdbConfig.mapSize)
    .setMaxDbs(lmdbConfig.maxDbs)
    .setMaxReaders(lmdbConfig.maxReaders)
    .open(home, EnvFlags.MDB_NOTLS, EnvFlags.MDB_NORDAHEAD, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC)

  lazy val accountNodeDataSource = new LmdbNodeDataSource(DbConfig.account, lmdbEnv, cacheConf.cacheSize)
  lazy val storageNodeDataSource = new LmdbNodeDataSource(DbConfig.storage, lmdbEnv, cacheConf.cacheSize)
  lazy val evmcodeDataSource = new LmdbNodeDataSource(DbConfig.evmcode, lmdbEnv, cacheSize = 10000)

  lazy val blockNumberDataSource = new LmdbKeyValueDataSource(DbConfig.blocknum, lmdbEnv, cacheSize = 1000)

  lazy val blockHeaderDataSource = new LmdbBlockDataSource(DbConfig.header, lmdbEnv, cacheSize = 1000)
  lazy val blockBodyDataSource = new LmdbBlockDataSource(DbConfig.body, lmdbEnv, cacheSize = 1000)
  lazy val receiptsDataSource = new LmdbBlockDataSource(DbConfig.receipts, lmdbEnv, cacheSize = 1000)
  lazy val totalDifficultyDataSource = new LmdbBlockDataSource(DbConfig.td, lmdbEnv, cacheSize = 1000)

  def stop() {
    log.info("db syncing...")

    // --- Don't close resouces here, since the futures during sync may not been finished yet
    // --- and we don't care about the resources releasing, since when closeAll() is called,
    // --- we are shutting down this application.

    //accountNodeDataSource.close()
    //storageNodeDataSource.close()
    //evmcodeDataSource.close()
    //blockNumberDataSource.close()
    //blockHeaderDataSource.close()
    //blockBodyDataSource.close()
    //receiptsDataSource.close()
    //totalDifficultyDataSource.close()

    //dataSource.close()

    lmdbEnv.sync(true)
    //env.close()

    log.info("db synced")
  }
}
