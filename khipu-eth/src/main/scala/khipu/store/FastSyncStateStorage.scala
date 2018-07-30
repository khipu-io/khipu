package khipu.store

import akka.util.ByteString
import java.nio.ByteOrder
import khipu.Hash
import khipu.blockchain.sync
import khipu.store.datasource.DataSource

object FastSyncStateStorage {
  val syncStateKey: String = "fast-sync-state"
}
final class FastSyncStateStorage(val source: DataSource) extends KeyValueStorage[String, sync.FastSyncService.SyncState, FastSyncStateStorage] {
  import FastSyncStateStorage._
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  override val namespace: Array[Byte] = Namespaces.FastSyncStateNamespace

  override def keySerializer: String => Array[Byte] = _.getBytes
  override def valueSerializer: sync.FastSyncService.SyncState => Array[Byte] =
    syncState => {
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

  override def valueDeserializer: Array[Byte] => sync.FastSyncService.SyncState =
    bytes => {
      if ((bytes eq null) || bytes.length == 0) {
        sync.FastSyncService.SyncState(0)
      } else {
        val data = ByteString(bytes).iterator

        val targetBlockNumber = data.getLong
        val downloadedNodesCount = data.getInt
        val bestBlockHeaderNumber = data.getLong

        var queueSize = data.getInt
        var i = 0
        var mptNodes = List[sync.NodeHash]()
        while (i < queueSize) {
          val tpe = data.getByte
          val length = data.getInt
          val bs = data.getBytes(length)
          val hash = tpe match {
            case sync.NodeHash.StateMptNode           => sync.StateMptNodeHash(bs)
            case sync.NodeHash.StorageRoot            => sync.StorageRootHash(bs)
            case sync.NodeHash.ContractStorageMptNode => sync.ContractStorageMptNodeHash(bs)
            case sync.NodeHash.Evmcode                => sync.EvmcodeHash(bs)
          }
          mptNodes ::= hash
          i += 1
        }

        queueSize = data.getInt
        i = 0
        var nonMptNodes = List[sync.NodeHash]()
        while (i < queueSize) {
          val tpe = data.getByte
          val length = data.getInt
          val bs = data.getBytes(length)
          val hash = tpe match {
            case sync.NodeHash.StateMptNode           => sync.StateMptNodeHash(bs)
            case sync.NodeHash.StorageRoot            => sync.StorageRootHash(bs)
            case sync.NodeHash.ContractStorageMptNode => sync.ContractStorageMptNodeHash(bs)
            case sync.NodeHash.Evmcode                => sync.EvmcodeHash(bs)
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

        sync.FastSyncService.SyncState(targetBlockNumber, downloadedNodesCount, bestBlockHeaderNumber, mptNodes.reverse, nonMptNodes.reverse, blockBodies.reverse, receipts.reverse)

      }
    }

  protected def apply(dataSource: DataSource): FastSyncStateStorage = new FastSyncStateStorage(dataSource)

  def putSyncState(syncState: sync.FastSyncService.SyncState): FastSyncStateStorage = put(syncStateKey, syncState)

  def getSyncState(): Option[sync.FastSyncService.SyncState] = get(syncStateKey)

  def purge(): FastSyncStateStorage = remove(syncStateKey)

}
