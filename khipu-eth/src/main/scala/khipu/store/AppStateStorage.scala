package khipu.store

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
}
/**
 * This class is used to store app state variables
 *   Key: see AppStateStorage.Keys
 *   Value: stored string value
 */
import AppStateStorage._
final class AppStateStorage(val source: DataSource) extends KeyValueStorage[Key, String, AppStateStorage] {

  val namespace: Array[Byte] = Namespaces.AppStateNamespace
  def keySerializer: Key => Array[Byte] = _.name.getBytes
  def valueSerializer: String => Array[Byte] = _.getBytes
  def valueDeserializer: Array[Byte] => String = (valueBytes: Array[Byte]) => new String(valueBytes)

  protected def apply(dataSource: DataSource): AppStateStorage = new AppStateStorage(dataSource)

  def getBestBlockNumber(): Long =
    get(Keys.BestBlockNumber).getOrElse("0").toLong

  def putBestBlockNumber(bestBlockNumber: Long): AppStateStorage = {
    // FIXME We need to decouple pruning from best block number storing in this fn
    //val result = pruneFn(getLastPrunedBlock(), bestBlockNumber)
    //putLastPrunedBlock(result.lastPrunedBlockNumber)

    put(Keys.BestBlockNumber, bestBlockNumber.toString)
  }

  def isFastSyncDone(): Boolean =
    get(Keys.FastSyncDone).exists(_.toBoolean)

  def fastSyncDone(): AppStateStorage =
    put(Keys.FastSyncDone, true.toString)

  def getEstimatedHighestBlock(): Long =
    get(Keys.EstimatedHighestBlock).getOrElse("0").toLong

  def putEstimatedHighestBlock(n: Long): AppStateStorage =
    put(Keys.EstimatedHighestBlock, n.toString)

  def getSyncStartingBlock(): Long =
    get(Keys.SyncStartingBlock).getOrElse("0").toLong

  def putSyncStartingBlock(n: Long): AppStateStorage =
    put(Keys.SyncStartingBlock, n.toString)

  def putLastPrunedBlock(n: Long): AppStateStorage =
    put(Keys.LastPrunedBlock, n.toString())

  def getLastPrunedBlock(): Long =
    get(Keys.LastPrunedBlock).getOrElse("0").toLong
}

