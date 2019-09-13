package khipu.storage

import java.nio.ByteBuffer
import khipu.storage.datasource.KeyValueDataSource
import khipu.util.BytesUtil
import khipu.util.CircularArrayQueue

object AppStateStorage {
  private val namespace = Namespaces.AppState
  val BestBlockNumber = BytesUtil.concat(namespace, "BestBlockNumber".getBytes)
  val FastSyncDone = BytesUtil.concat(namespace, "FastSyncDone".getBytes)
  val EstimatedHighestBlock = BytesUtil.concat(namespace, "EstimatedHighestBlock".getBytes)
  val SyncStartingBlock = BytesUtil.concat(namespace, "SyncStartingBlock".getBytes)
  val LastPrunedBlock = BytesUtil.concat(namespace, "LastPrunedBlock".getBytes)
}
/**
 * This class is used to store app state variables
 *   Key: see AppStateStorage.Keys
 *   Value: stored string value
 */
import AppStateStorage._
final class AppStateStorage(val source: KeyValueDataSource, unconfirmedDepth: Int) extends KeyValueStorage[Array[Byte], Long] {
  type This = AppStateStorage

  def topic = source.topic

  private val unconfirmed = new CircularArrayQueue[Long](unconfirmedDepth)

  private var _withUnconfirmed = false
  def withUnconfirmed = _withUnconfirmed
  def withUnconfirmed_=(b: Boolean) = _withUnconfirmed = b
  def swithToWithUnconfirmed() {
    _withUnconfirmed = true
  }

  def getBestBlockNumber: Long = (unconfirmed.lastOption orElse get(BestBlockNumber)).getOrElse(0)

  def putBestBlockNumber(bestBlockNumber: Long): AppStateStorage = {
    val toFlush = if (withUnconfirmed) {
      if (unconfirmed.isFull) Some(unconfirmed.dequeue) else None
    } else {
      Some(bestBlockNumber)
    }

    if (withUnconfirmed) {
      unconfirmed.enqueue(bestBlockNumber)
    }

    toFlush foreach { number => put(BestBlockNumber, number) }

    this
  }

  def clearUnconfirmed() {
    unconfirmed.clear()
  }

  def keyToBytes(k: Array[Byte]): Array[Byte] = k
  def valueToBytes(v: Long): Array[Byte] = longToBytes(v)
  def valueFromBytes(bytes: Array[Byte]): Long = bytesToLong(bytes)

  private def longToBytes(v: Long) = ByteBuffer.allocate(8).putLong(v).array
  private def bytesToLong(v: Array[Byte]) = ByteBuffer.wrap(v).getLong

  protected def apply(source: KeyValueDataSource): AppStateStorage = new AppStateStorage(source, unconfirmedDepth)

  def isFastSyncDone(): Boolean = get(FastSyncDone).isDefined

  def fastSyncDone(): AppStateStorage = put(FastSyncDone, 1L)

  def getEstimatedHighestBlock(): Long = get(EstimatedHighestBlock).getOrElse(0)

  def putEstimatedHighestBlock(n: Long): AppStateStorage = put(EstimatedHighestBlock, n)

  def getSyncStartingBlock(): Long = get(SyncStartingBlock).getOrElse(0)

  def putSyncStartingBlock(n: Long): AppStateStorage = put(SyncStartingBlock, n)

  def putLastPrunedBlock(n: Long): AppStateStorage = put(LastPrunedBlock, n)

  def getLastPrunedBlock(): Long = get(LastPrunedBlock).getOrElse(0)
}

