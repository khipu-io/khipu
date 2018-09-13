package khipu.store

import akka.util.ByteString
import java.nio.ByteOrder
import khipu.Hash
import khipu.blockchain.sync.FastSyncService.SyncState
import khipu.blockchain.sync.NodeHash
import khipu.blockchain.sync.StateMptNodeHash
import khipu.blockchain.sync.StorageRootHash
import khipu.blockchain.sync.ContractStorageMptNodeHash
import khipu.blockchain.sync.EvmcodeHash
import khipu.store.datasource.DataSource

object FastSyncStateStorage {
  val syncStateKey: String = "fast-sync-state"
}
final class FastSyncStateStorage(val source: DataSource) extends KeyValueStorage[String, SyncState, FastSyncStateStorage] {
  import FastSyncStateStorage._
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override val namespace: Array[Byte] = Namespaces.FastSyncStateNamespace

  override def keySerializer: String => Array[Byte] = _.getBytes
  override def valueSerializer: SyncState => Array[Byte] = syncState => {
    val builder = ByteString.newBuilder
    builder.putLong(syncState.targetBlockNumber)
    builder.putInt(syncState.downloadedNodesCount)
    builder.putLong(syncState.bestBlockHeaderNumber)

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

    val blockBodies = syncState.workingBlockBodies.map(_._1) ++ syncState.pendingBlockBodies
    builder.putInt(blockBodies.size)
    blockBodies foreach { x =>
      builder.putInt(x.bytes.length)
      builder.putBytes(x.bytes)
    }

    val receipts = syncState.workingReceipts.map(_._1) ++ syncState.pendingReceipts
    builder.putInt(receipts.size)
    receipts foreach { x =>
      builder.putInt(x.bytes.length)
      builder.putBytes(x.bytes)
    }

    builder.result.toArray
  }

  override def valueDeserializer: Array[Byte] => SyncState = bytes => {
    if ((bytes eq null) || bytes.length == 0) {
      SyncState(0)
    } else {
      val data = ByteString(bytes).iterator

      val targetBlockNumber = data.getLong
      val downloadedNodesCount = data.getInt
      val bestBlockHeaderNumber = data.getLong

      var queueSize = data.getInt
      var i = 0
      var mptNodes = List[NodeHash]()
      while (i < queueSize) {
        val tpe = data.getByte
        val length = data.getInt
        val bs = data.getBytes(length)
        val hash = tpe match {
          case NodeHash.StateMptNode           => StateMptNodeHash(bs)
          case NodeHash.StorageRoot            => StorageRootHash(bs)
          case NodeHash.ContractStorageMptNode => ContractStorageMptNodeHash(bs)
          case NodeHash.Evmcode                => EvmcodeHash(bs)
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
        val bs = data.getBytes(length)
        val hash = tpe match {
          case NodeHash.StateMptNode           => StateMptNodeHash(bs)
          case NodeHash.StorageRoot            => StorageRootHash(bs)
          case NodeHash.ContractStorageMptNode => ContractStorageMptNodeHash(bs)
          case NodeHash.Evmcode                => EvmcodeHash(bs)
        }
        nonMptNodes ::= hash
        i += 1
      }

      queueSize = data.getInt
      i = 0
      var blockBodies = List[Hash]()
      while (i < queueSize) {
        val length = data.getInt
        val bs = data.getBytes(length)
        blockBodies ::= Hash(bs)
        i += 1
      }

      queueSize = data.getInt
      i = 0
      var receipts = List[Hash]()
      while (i < queueSize) {
        val length = data.getInt
        val bs = data.getBytes(length)
        receipts ::= Hash(bs)
        i += 1
      }

      SyncState(targetBlockNumber, downloadedNodesCount, bestBlockHeaderNumber, mptNodes.reverse, nonMptNodes.reverse, blockBodies.reverse, receipts.reverse)
    }
  }

  protected def apply(dataSource: DataSource): FastSyncStateStorage = new FastSyncStateStorage(dataSource)

  def putSyncState(syncState: SyncState): FastSyncStateStorage = put(syncStateKey, syncState)

  def getSyncState(): Option[SyncState] = get(syncStateKey)

  def purge(): FastSyncStateStorage = remove(syncStateKey)

}
