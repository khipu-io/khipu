package khipu.store

import java.nio.ByteBuffer
import khipu.store.datasource.DataSource

object AppStateStorage {
  final case class Key private (name: String)

  object Keys {
    val BestBlockNumber = Key("BestBlockNumber")
    val FastSyncDone = Key("FastSyncDone")
    val EstimatedHighestBlock = Key("EstimatedHighestBlock")
    val SyncStartingBlock = Key("SyncStartingBlock")
    val LastPrunedBlock = Key("LastPrunedBlock")
  }

  def longToBytes(v: Long) = ByteBuffer.allocate(8).putLong(v).array
  def bytesToLong(v: Array[Byte]) = ByteBuffer.wrap(v).getLong
}
/**
 * This class is used to store app state variables
 *   Key: see AppStateStorage.Keys
 *   Value: stored string value
 */
import AppStateStorage._
final class AppStateStorage(val source: DataSource) extends KeyValueStorage[Key, Long, AppStateStorage] {

  val namespace: Array[Byte] = Namespaces.AppStateNamespace
  def keySerializer: Key => Array[Byte] = _.name.getBytes
  def valueSerializer: Long => Array[Byte] = longToBytes
  def valueDeserializer: Array[Byte] => Long = bytesToLong

  protected def apply(dataSource: DataSource): AppStateStorage = new AppStateStorage(dataSource)

  def getBestBlockNumber(): Long =
    get(Keys.BestBlockNumber).getOrElse(0)

  def putBestBlockNumber(bestBlockNumber: Long): AppStateStorage = {
    // FIXME We need to decouple pruning from best block number storing in this fn
    //val result = pruneFn(getLastPrunedBlock(), bestBlockNumber)
    //putLastPrunedBlock(result.lastPrunedBlockNumber)

    put(Keys.BestBlockNumber, bestBlockNumber)
  }

  def isFastSyncDone(): Boolean =
    get(Keys.FastSyncDone).isDefined

  def fastSyncDone(): AppStateStorage =
    put(Keys.FastSyncDone, 1L)

  def getEstimatedHighestBlock(): Long =
    get(Keys.EstimatedHighestBlock).getOrElse(0)

  def putEstimatedHighestBlock(n: Long): AppStateStorage =
    put(Keys.EstimatedHighestBlock, n)

  def getSyncStartingBlock(): Long =
    get(Keys.SyncStartingBlock).getOrElse(0)

  def putSyncStartingBlock(n: Long): AppStateStorage =
    put(Keys.SyncStartingBlock, n)

  def putLastPrunedBlock(n: Long): AppStateStorage =
    put(Keys.LastPrunedBlock, n)

  def getLastPrunedBlock(): Long =
    get(Keys.LastPrunedBlock).getOrElse(0)
}

