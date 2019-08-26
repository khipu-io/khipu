package khipu.storage.datasource

import akka.actor.ActorSystem
import java.io.File

trait SharedRocksdbDataSources extends DataSources {
  implicit protected val system: ActorSystem

  val rocksdbHome: File

  lazy val dataSource = new RocksdbDataSource("shared", rocksdbHome, cacheSize = 1000)

  lazy val transactionDataSource = dataSource

  lazy val fastSyncStateDataSource = dataSource
  lazy val appStateDataSource = dataSource

  lazy val blockHeightsHashesDataSource = dataSource
  lazy val knownNodesDataSource = dataSource

}
