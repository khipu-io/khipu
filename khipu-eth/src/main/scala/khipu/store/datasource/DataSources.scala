package khipu.store.datasource

import khipu.store.trienode.PruningMode

trait DataSources {
  val pruningMode: PruningMode

  val dataSource: DataSource

  val transactionMappingDataSource: DataSource

  val blockHeightsHashesDataSource: DataSource

  val appStateDataSource: DataSource
  val fastSyncStateDataSource: DataSource
  val knownNodesDataSource: DataSource

  def closeAll(): Unit
}
