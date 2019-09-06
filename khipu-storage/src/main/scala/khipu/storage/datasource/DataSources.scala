package khipu.storage.datasource

trait DataSources {
  val dataSource: KeyValueDataSource

  val blockHeightsHashesDataSource: KeyValueDataSource

  val appStateDataSource: KeyValueDataSource
  val fastSyncStateDataSource: KeyValueDataSource
  val knownNodesDataSource: KeyValueDataSource

  def stop(): Unit
}
