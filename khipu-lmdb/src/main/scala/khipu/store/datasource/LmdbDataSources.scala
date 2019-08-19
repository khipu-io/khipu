package khipu.store.datasource

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import com.typesafe.config.Config
import java.io.File
import khipu.config.CacheConfig
import khipu.config.DbConfig
import khipu.config.LmdbConfig
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags

trait LmdbDataSources extends SharedLmdbDataSources {
  implicit protected val system: ActorSystem

  protected val config: Config
  protected val log: LoggingAdapter
  protected val lmdbConfig: LmdbConfig

  private lazy val cacheConf = CacheConfig(config)

  private lazy val home = {
    val h = new File(lmdbConfig.path)
    if (!h.exists) {
      h.mkdirs()
    }
    h
  }

  lazy val env = Env.create()
    .setMapSize(lmdbConfig.mapSize)
    .setMaxDbs(lmdbConfig.maxDbs)
    .setMaxReaders(lmdbConfig.maxReaders)
    .open(home, EnvFlags.MDB_NOTLS, EnvFlags.MDB_NORDAHEAD, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC)

  lazy val accountNodeDataSource = new LmdbNodeDataSource(DbConfig.account, env, cacheConf.cacheSize)
  lazy val storageNodeDataSource = new LmdbNodeDataSource(DbConfig.storage, env, cacheConf.cacheSize)
  lazy val evmcodeDataSource = new LmdbNodeDataSource(DbConfig.evmcode, env)

  lazy val blockNumberDataSource = new LmdbDataSource(DbConfig.blocknum, env)

  lazy val blockHeaderDataSource = new LmdbBlockDataSource(DbConfig.header, env)
  lazy val blockBodyDataSource = new LmdbBlockDataSource(DbConfig.body, env)
  lazy val receiptsDataSource = new LmdbBlockDataSource(DbConfig.receipts, env)
  lazy val totalDifficultyDataSource = new LmdbBlockDataSource(DbConfig.td, env)

  def closeAll() {
    log.info("db syncing...")

    // --- Don't close resouces here, since the futures during sync may not been finished yet
    // --- and we don't care about the resources releasing, since when closeAll() is called,
    // --- we are shutting down this application.

    //accountNodeDataSource.close()
    //storageNodeDataSource.close()
    //evmCodeDataSource.close()
    //blockNumberDataSource.close()
    //blockHeaderDataSource.close()
    //blockBodyDataSource.close()
    //receiptsDataSource.close()
    //totalDifficultyDataSource.close()

    //dataSource.close()

    env.sync(true)
    //env.close()

    log.info("db synced")
  }
}
