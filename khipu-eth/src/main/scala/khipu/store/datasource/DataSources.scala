package khipu.store.datasource

import khipu.store.trienode.PruningMode

trait DataSources {
  val pruningMode: PruningMode

  //val blockHeadersDataSource: DataSource
  //val blockBodiesDataSource: DataSource
  //val receiptsDataSource: DataSource
  //val totalDifficultyDataSource: DataSource
  val transactionMappingDataSource: DataSource

  val blockHeightsHashesDataSource: DataSource

  val appStateDataSource: DataSource
  val fastSyncStateDataSource: DataSource
  val knownNodesDataSource: DataSource

  def closeAll(): Unit
}
