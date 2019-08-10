package khipu.store

import akka.actor.ActorSystem
import khipu.store.datasource.DataSources
import khipu.util.PruningConfig.PruningMode

object Storages {

  trait DefaultStorages extends Storages with DataSources {
    // use 'lazy' val to wait for other fields to be set in last implementation 

    lazy val accountNodeStorage = new NodeStorage(accountNodeDataSource, unconfirmedDepth)
    lazy val storageNodeStorage = new NodeStorage(storageNodeDataSource, unconfirmedDepth)
    lazy val evmcodeStorage = new NodeStorage(evmcodeDataSource, unconfirmedDepth)

    lazy val blockNumberStorage = new BlockNumberStorage(blockNumberDataSource, unconfirmedDepth)

    lazy val blockHeaderStorage = new BlockHeaderStorage(blockHeaderDataSource, unconfirmedDepth)
    lazy val blockBodyStorage = new BlockBodyStorage(blockBodyDataSource, unconfirmedDepth)
    lazy val receiptsStorage = new ReceiptsStorage(receiptsDataSource, unconfirmedDepth)
    lazy val totalDifficultyStorage = new TotalDifficultyStorage(totalDifficultyDataSource, unconfirmedDepth)

    lazy val transactionStorage = new TransactionStorage(transactionDataSource, unconfirmedDepth)

    lazy val blockNumbers = new BlockNumbers(blockNumberStorage, blockHeaderStorage, unconfirmedDepth)

    lazy val appStateStorage = new AppStateStorage(appStateDataSource, unconfirmedDepth)
    lazy val fastSyncStateStorage = new FastSyncStateStorage(fastSyncStateDataSource)
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

