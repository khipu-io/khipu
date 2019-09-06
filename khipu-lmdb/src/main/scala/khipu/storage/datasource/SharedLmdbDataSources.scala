package khipu.storage.datasource

import akka.actor.ActorSystem
import java.nio.ByteBuffer
import org.lmdbjava.Env

trait SharedLmdbDataSources {
  implicit protected val system: ActorSystem

  val lmdbEnv: Env[ByteBuffer]

  lazy val dataSource = new LmdbDataSource("shared", lmdbEnv, cacheSize = 1000)

  lazy val transactionDataSource = dataSource

  lazy val fastSyncStateDataSource = dataSource
  lazy val appStateDataSource = dataSource

  lazy val blockHeightsHashesDataSource = dataSource
  lazy val knownNodesDataSource = dataSource

}
