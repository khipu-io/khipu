package khipu.storage

import akka.util.ByteString
import java.nio.ByteBuffer
import java.nio.ByteOrder
import khipu.blockchain.sync.FastSyncService.SyncState
import khipu.blockchain.sync.NodeHash
import khipu.blockchain.sync.StateMptNodeHash
import khipu.blockchain.sync.StorageRootHash
import khipu.blockchain.sync.ContractStorageMptNodeHash
import khipu.blockchain.sync.EvmcodeHash
import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
import khipu.storage.datasource.KeyValueDataSource

object FastSyncStateStorage {
  val namespace = Namespaces.FastSyncState
  val T = Array[Byte]('T'.toByte)
  val H = Array[Byte]('H'.toByte)
  val B = Array[Byte]('B'.toByte)
  val R = Array[Byte]('R'.toByte)
  val N = Array[Byte]('N'.toByte)
}
final class FastSyncStateStorage(val source: KeyValueDataSource) {
  import FastSyncStateStorage._
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  def putTargetBlockHeader(syncState: SyncState) {
    source.update(namespace, Nil, List(T -> syncState.targetBlockHeader.toBytes))
  }

  def putBestHeaderNumber(syncState: SyncState) {
    source.update(namespace, Nil, List(H -> longToBytes(syncState.bestHeaderNumber)))
  }

  def putBestBodyNumber(syncState: SyncState) {
    source.update(namespace, Nil, List(B -> longToBytes(syncState.bestBodyNumber)))
  }

  def putBestReceiptsNumber(syncState: SyncState) {
    source.update(namespace, Nil, List(R -> longToBytes(syncState.bestReceiptsNumber)))
  }

  def putNodesData(syncState: SyncState) {
    val builder = ByteString.newBuilder

    builder.putLong(syncState.downloadedNodesCount)

    val mptNodes = syncState.workingMptNodes.map(_._1) ++ syncState.pendingMptNodes
    builder.putInt(mptNodes.size)
    mptNodes foreach { x =>
      builder.putByte(x.tpe)
      builder.putInt(x.bytes.length)
      builder.putBytes(x.bytes)
    }

    val nonMptNodes = syncState.workingNonMptNodes.map(_._1) ++ syncState.pendingNonMptNodes
    builder.putInt(nonMptNodes.size)
    nonMptNodes foreach { x =>
      builder.putByte(x.tpe)
      builder.putInt(x.bytes.length)
      builder.putBytes(x.bytes)
    }

    source.update(namespace, Nil, List(N -> builder.result.toArray))
  }

  private def longToBytes(long: Long): Array[Byte] = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(long).array
  private def bytesToLong(bytes: Array[Byte]): Long = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.put(bytes)
    buffer.flip()
    buffer.getLong()
  }

  def putSyncState(syncState: SyncState) = {
    putTargetBlockHeader(syncState)
    putBestHeaderNumber(syncState)
    putBestBodyNumber(syncState)
    putBestReceiptsNumber(syncState)
    putNodesData(syncState)
  }

  def getSyncState(): Option[SyncState] = {
    source.get(namespace, T).map(_.toBlockHeader).map { targetBlockHeader =>

      val bestHeaderNumber = source.get(namespace, H).map(bytesToLong).getOrElse(0L)
      val bestBodyNumber = source.get(namespace, B).map(bytesToLong).getOrElse(0L)
      val bestReceiptsNumber = source.get(namespace, R).map(bytesToLong).getOrElse(0L)

      val (downloadedNodesCount, mptNodes, nonMptNodes) = source.get(namespace, N) match {
        case Some(bytes) if bytes.length != 0 =>
          val data = ByteString(bytes).iterator
          val downloadedNodesCount = data.getLong

          var queueSize = data.getInt
          var i = 0
          var mptNodes = List[NodeHash]()
          while (i < queueSize) {
            val tpe = data.getByte
            val length = data.getInt
            val bytes = data.getBytes(length)
            val hash = tpe match {
              case NodeHash.StateMptNode           => StateMptNodeHash(bytes)
              case NodeHash.StorageRoot            => StorageRootHash(bytes)
              case NodeHash.ContractStorageMptNode => ContractStorageMptNodeHash(bytes)
              case NodeHash.Evmcode                => EvmcodeHash(bytes)
            }
            mptNodes ::= hash
            i += 1
          }

          queueSize = data.getInt
          i = 0
          var nonMptNodes = List[NodeHash]()
          while (i < queueSize) {
            val tpe = data.getByte
            val length = data.getInt
            val bytes = data.getBytes(length)
            val hash = tpe match {
              case NodeHash.StateMptNode           => StateMptNodeHash(bytes)
              case NodeHash.StorageRoot            => StorageRootHash(bytes)
              case NodeHash.ContractStorageMptNode => ContractStorageMptNodeHash(bytes)
              case NodeHash.Evmcode                => EvmcodeHash(bytes)
            }
            nonMptNodes ::= hash
            i += 1
          }

          (downloadedNodesCount, mptNodes.reverse, nonMptNodes.reverse)

        case _ =>
          (0L, Nil, Nil)
      }

      SyncState(targetBlockHeader, bestHeaderNumber, bestBodyNumber, bestReceiptsNumber, downloadedNodesCount, mptNodes, nonMptNodes, bestBodyNumber, bestReceiptsNumber)
    }
  }

  def purge() {
    source.update(namespace, List(T, H, B, R, N), Nil)
  }

}
