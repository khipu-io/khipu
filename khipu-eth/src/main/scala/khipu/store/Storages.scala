package khipu.store

import akka.actor.ActorSystem
import khipu.Hash
import khipu.store.datasource.DataSources
import khipu.store.trienode.NodeTableStorage
import khipu.store.trienode.PruningMode

object Storages {

  trait DefaultStorages extends Storages with DataSources {
    // use 'lazy' val to wait for other fields (cache pruningMode etc) to be set in last implementation, 

    lazy val accountNodeStorageFor: (Option[Long]) => NodeTableStorage = bn => new NodeTableStorage(accountNodeDataSource)
    lazy val storageNodeStorageFor: (Option[Long]) => NodeTableStorage = bn => new NodeTableStorage(storageNodeDataSource)
    lazy val evmcodeStorage = new NodeTableStorage(evmcodeDataSource)

    lazy val blockNumberMappingStorage = new BlockNumberMappingStorage(this, blockNumberMappingDataSource)

    lazy val blockHeaderStorage = new BlockHeaderStorage(this, blockHeaderDataSource)
    lazy val blockBodyStorage = new BlockBodyStorage(this, blockBodyDataSource)
    lazy val receiptsStorage = new ReceiptsStorage(this, receiptsDataSource)
    lazy val totalDifficultyStorage = new TotalDifficultyStorage(this, totalDifficultyDataSource)

    lazy val transactionMappingStorage = new TransactionMappingStorage(transactionMappingDataSource)

    lazy val fastSyncStateStorage = new FastSyncStateStorage(fastSyncStateDataSource)
    lazy val appStateStorage = new AppStateStorage(appStateDataSource)
    lazy val knownNodesStorage = new KnownNodesStorage(knownNodesDataSource)

    lazy val blockNumberMapping = new BlockNumberMapping(blockNumberMappingDataSource, blockHeaderDataSource)
  }
}

trait Storages extends BlockchainStorages {
  implicit protected val system: ActorSystem

  def appStateStorage: AppStateStorage
  def fastSyncStateStorage: FastSyncStateStorage
  def knownNodesStorage: KnownNodesStorage
  def pruningMode: PruningMode
}

