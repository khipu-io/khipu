package khipu.store.datasource

import kesque.Kesque
import khipu.util.Config

trait SharedLeveldbDataSources extends DataSources {
  val kesque: Kesque

  val dataSource = LeveldbDataSource(Config.Db.LeveldbConfig)

  val transactionDataSource = dataSource

  val fastSyncStateDataSource = dataSource
  val appStateDataSource = dataSource

  val blockHeightsHashesDataSource = dataSource
  val knownNodesDataSource = dataSource

  def closeAll() {
    dataSource.close()
    kesque.shutdown()
  }
}
