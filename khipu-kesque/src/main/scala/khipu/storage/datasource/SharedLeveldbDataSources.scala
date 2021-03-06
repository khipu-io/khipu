package khipu.storage.datasource

import kesque.Kesque
import khipu.config.LeveldbConfig

trait SharedLeveldbDataSources extends DataSources {
  val kesque: Kesque
  val leveldbConfig: LeveldbConfig

  val dataSource = LeveldbKeyValueDataSource(leveldbConfig)

  val fastSyncStateDataSource = dataSource
  val appStateDataSource = dataSource

  val blockHeightsHashesDataSource = dataSource
  val knownNodesDataSource = dataSource

  def stop() {
    dataSource.stop()
  }
}
