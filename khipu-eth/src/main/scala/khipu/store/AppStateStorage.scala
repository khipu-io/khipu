package khipu.store

import java.nio.ByteBuffer
import khipu.store.datasource.DataSource
import khipu.util.CircularArrayQueue

object AppStateStorage {
  final case class Key(name: String) { val bytes = name.getBytes }

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
final class AppStateStorage(val source: DataSource, unconfirmedDepth: Int) extends KeyValueStorage[Key, Long] {
  type This = AppStateStorage

  def topic = source.topic

  private val unconfirmed = new CircularArrayQueue[Long](unconfirmedDepth)

  private var _withUnconfirmed = false
  def withUnconfirmed = _withUnconfirmed
  def withUnconfirmed_=(b: Boolean) = _withUnconfirmed = b
  def swithToWithUnconfirmed() {
    _withUnconfirmed = true
  }

  def getBestBlockNumber: Long = (unconfirmed.lastOption orElse get(Keys.BestBlockNumber)).getOrElse(0)

  def putBestBlockNumber(bestBlockNumber: Long): AppStateStorage = {
    val toFlush = if (withUnconfirmed) {
      if (unconfirmed.isFull) Some(unconfirmed.dequeue) else None
    } else {
      Some(bestBlockNumber)
    }

    if (withUnconfirmed) {
      unconfirmed.enqueue(bestBlockNumber)
    }

    toFlush foreach { number => put(Keys.BestBlockNumber, number) }

    this
  }

  def clearUnconfirmed() {
    unconfirmed.clear()
  }

  val namespace: Array[Byte] = Namespaces.AppState
  def keyToBytes(k: Key): Array[Byte] = k.bytes
  def valueToBytes(v: Long): Array[Byte] = longToBytes(v)
  def valueFromBytes(bytes: Array[Byte]): Long = bytesToLong(bytes)

  private def longToBytes(v: Long) = ByteBuffer.allocate(8).putLong(v).array
  private def bytesToLong(v: Array[Byte]) = ByteBuffer.wrap(v).getLong

  protected def apply(dataSource: DataSource): AppStateStorage = new AppStateStorage(dataSource, unconfirmedDepth)

  def isFastSyncDone(): Boolean = get(Keys.FastSyncDone).isDefined

  def fastSyncDone(): AppStateStorage = put(Keys.FastSyncDone, 1L)

  def getEstimatedHighestBlock(): Long = get(Keys.EstimatedHighestBlock).getOrElse(0)

  def putEstimatedHighestBlock(n: Long): AppStateStorage = put(Keys.EstimatedHighestBlock, n)

  def getSyncStartingBlock(): Long = get(Keys.SyncStartingBlock).getOrElse(0)

  def putSyncStartingBlock(n: Long): AppStateStorage = put(Keys.SyncStartingBlock, n)

  def putLastPrunedBlock(n: Long): AppStateStorage = put(Keys.LastPrunedBlock, n)

  def getLastPrunedBlock(): Long = get(Keys.LastPrunedBlock).getOrElse(0)
}

