package khipu.storage.datasource

import akka.actor.ActorSystem
import khipu.config.RocksdbConfig

trait SharedRocksdbDataSources extends DataSources {
  implicit protected val system: ActorSystem

  val rocksdbConfig: RocksdbConfig

  lazy val dataSource = new RocksdbDataSource("shared", rocksdbConfig, cacheSize = 1000)

  lazy val transactionDataSource = dataSource

  lazy val fastSyncStateDataSource = dataSource
  lazy val appStateDataSource = dataSource

  lazy val blockHeightsHashesDataSource = dataSource
  lazy val knownNodesDataSource = dataSource

}
