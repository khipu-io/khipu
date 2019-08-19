package khipu.store.datasource

import akka.actor.ActorSystem
import java.nio.ByteBuffer
import org.lmdbjava.Env

trait SharedLmdbDataSources extends DataSources {
  implicit protected val system: ActorSystem

  val env: Env[ByteBuffer]

  lazy val dataSource = LmdbDataSource("shared", env)

  lazy val transactionDataSource = dataSource

  lazy val fastSyncStateDataSource = dataSource
  lazy val appStateDataSource = dataSource

  lazy val blockHeightsHashesDataSource = dataSource
  lazy val knownNodesDataSource = dataSource

}
