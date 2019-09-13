package khipu.storage.datasource

trait DataSources {
  val sharedDataSource: KeyValueDataSource

  val blockHeightsHashesDataSource: KeyValueDataSource

  val appStateDataSource: KeyValueDataSource
  val fastSyncStateDataSource: KeyValueDataSource
  val knownNodesDataSource: KeyValueDataSource

  def stop(): Unit
}
