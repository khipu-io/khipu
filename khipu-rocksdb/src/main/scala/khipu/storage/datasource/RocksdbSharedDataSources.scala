package khipu.storage.datasource

import akka.actor.ActorSystem
import khipu.config.DbConfig
import khipu.config.RocksdbConfig

trait RocksdbSharedDataSources extends DataSources {
  implicit protected val system: ActorSystem

  val rocksdbConfig: RocksdbConfig

  lazy val sharedDataSource = new RocksdbKeyValueDataSource(DbConfig.shared, rocksdbConfig, cacheSize = 1000)

  lazy val fastSyncStateDataSource = sharedDataSource
  lazy val appStateDataSource = sharedDataSource

  lazy val blockHeightsHashesDataSource = sharedDataSource
  lazy val knownNodesDataSource = sharedDataSource
}
