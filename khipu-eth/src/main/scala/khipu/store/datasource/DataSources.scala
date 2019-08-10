package khipu.store.datasource

import khipu.util.PruningConfig.PruningMode

trait DataSources {
  val pruningMode: PruningMode

  val dataSource: DataSource

  val transactionDataSource: DataSource

  val blockHeightsHashesDataSource: DataSource

  val appStateDataSource: DataSource
  val fastSyncStateDataSource: DataSource
  val knownNodesDataSource: DataSource

  def closeAll(): Unit
}
