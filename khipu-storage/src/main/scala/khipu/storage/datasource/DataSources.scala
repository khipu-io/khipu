package khipu.storage.datasource

trait DataSources {
  val dataSource: DataSource

  val blockHeightsHashesDataSource: DataSource

  val appStateDataSource: DataSource
  val fastSyncStateDataSource: DataSource
  val knownNodesDataSource: DataSource

  def closeAll(): Unit
}
