package khipu.store

import akka.actor.ActorSystem
import khipu.store.datasource.DataSources
import khipu.store.trienode.NodeKeyValueStorage
import khipu.store.trienode.PruningMode

object Storages {

  trait DefaultStorages extends Storages with DataSources {
    // use 'lazy' val to wait for other fields (cache pruningMode etc) to be set in last implementation, 

    lazy val accountNodeStorage = new NodeKeyValueStorage(accountNodeDataSource)
    lazy val storageNodeStorage = new NodeKeyValueStorage(storageNodeDataSource)
    lazy val evmcodeStorage = new NodeKeyValueStorage(evmcodeDataSource)

    lazy val blockNumberStorage = new BlockNumberStorage(this, blockNumberDataSource)

    lazy val blockHeaderStorage = new BlockHeaderStorage(this, blockHeaderDataSource)
    lazy val blockBodyStorage = new BlockBodyStorage(this, blockBodyDataSource)
    lazy val receiptsStorage = new ReceiptsStorage(this, receiptsDataSource)
    lazy val totalDifficultyStorage = new TotalDifficultyStorage(this, totalDifficultyDataSource)

    lazy val transactionStorage = new TransactionStorage(transactionDataSource)

    lazy val fastSyncStateStorage = new FastSyncStateStorage(fastSyncStateDataSource)
    lazy val appStateStorage = new AppStateStorage(appStateDataSource)
    lazy val knownNodesStorage = new KnownNodesStorage(knownNodesDataSource)

    lazy val blockNumbers = new BlockNumbers(blockNumberDataSource, blockHeaderDataSource)
  }
}

trait Storages extends BlockchainStorages {
  implicit protected val system: ActorSystem

  def appStateStorage: AppStateStorage
  def fastSyncStateStorage: FastSyncStateStorage
  def knownNodesStorage: KnownNodesStorage
  def pruningMode: PruningMode
}

