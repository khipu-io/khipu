package khipu.store.datasource

import akka.actor.ActorSystem
import java.nio.ByteBuffer
import khipu.util.Config
import org.lmdbjava.Env

trait SharedLmdbDataSources extends DataSources {
  implicit protected val system: ActorSystem

  val env: Env[ByteBuffer]

  lazy val dataSource = LmdbDataSource(env, Config.Db.LmdbConfig)

  lazy val transactionMappingDataSource = dataSource

  lazy val fastSyncStateDataSource = dataSource
  lazy val appStateDataSource = dataSource

  lazy val blockHeightsHashesDataSource = dataSource
  lazy val knownNodesDataSource = dataSource

  def closeAll() {
    dataSource.close()
    env.close()
  }
}
