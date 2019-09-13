package khipu.storage.datasource

import akka.actor.ActorSystem
import java.nio.ByteBuffer
import khipu.config.DbConfig
import org.lmdbjava.Env

trait LmdbSharedDataSources extends DataSources {
  implicit protected val system: ActorSystem

  val lmdbEnv: Env[ByteBuffer]

  lazy val sharedDataSource = new LmdbKeyValueDataSource(DbConfig.shared, lmdbEnv, cacheSize = 1000)

  lazy val fastSyncStateDataSource = sharedDataSource
  lazy val appStateDataSource = sharedDataSource

  lazy val blockHeightsHashesDataSource = sharedDataSource
  lazy val knownNodesDataSource = sharedDataSource
}
