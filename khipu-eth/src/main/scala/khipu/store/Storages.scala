package khipu.store

import akka.actor.ActorSystem
import khipu.store.datasource.DataSources
import khipu.store.trienode.NodeTableStorage
import khipu.store.trienode.PruningMode

object Storages {

  trait DefaultStorages extends Storages with DataSources {
    // lazy val -- to wait for other fields (cache pruningMode etc) to be set in last implementation, 

    lazy val evmCodeStorage = new EvmCodeStorage(evmCodeDataSource)
    lazy val accountNodeStorageFor: (Option[Long]) => NodeTableStorage = bn => new NodeTableStorage(accountNodeDataSource)
    lazy val storageNodeStorageFor: (Option[Long]) => NodeTableStorage = bn => new NodeTableStorage(storageNodeDataSource)

    lazy val blockHeadersStorage = new BlockHeadersStorage(blockHeadersDataSource)
    lazy val blockBodiesStorage = new BlockBodiesStorage(blockBodiesDataSource)
    lazy val blockNumberMappingStorage = new BlockNumberMappingStorage(blockHeightsHashesDataSource, blockNumberCache)
    lazy val receiptsStorage = new ReceiptsStorage(receiptsDataSource)

    lazy val totalDifficultyStorage = new TotalDifficultyStorage(totalDifficultyDataSource)
    lazy val transactionMappingStorage = new TransactionMappingStorage(transactionMappingDataSource)

    lazy val fastSyncStateStorage = new FastSyncStateStorage(fastSyncStateDataSource)
    lazy val appStateStorage = new AppStateStorage(appStateDataSource)
    lazy val knownNodesStorage = new KnownNodesStorage(knownNodesDataSource)
  }
}

trait Storages extends BlockchainStorages {
  implicit protected val system: ActorSystem

  def appStateStorage: AppStateStorage
  def fastSyncStateStorage: FastSyncStateStorage
  def knownNodesStorage: KnownNodesStorage
  def pruningMode: PruningMode
}

