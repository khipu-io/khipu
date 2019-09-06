package khipu.storage

import akka.actor.ActorSystem
import khipu.storage.datasource.DataSources

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

  def bestBlockNumber = math.min(bestBodyNumber, bestReceiptsNumber)

  def bestHeaderNumber = blockHeaderStorage.bestBlockNumber
  def bestBodyNumber = blockBodyStorage.bestBlockNumber
  def bestReceiptsNumber = receiptsStorage.bestBlockNumber

  def swithToWithUnconfirmed() {
    accountNodeStorage.swithToWithUnconfirmed()
    storageNodeStorage.swithToWithUnconfirmed()
    evmcodeStorage.swithToWithUnconfirmed()

    blockNumberStorage.swithToWithUnconfirmed()

    blockHeaderStorage.swithToWithUnconfirmed()
    blockBodyStorage.swithToWithUnconfirmed()
    receiptsStorage.swithToWithUnconfirmed()

    totalDifficultyStorage.swithToWithUnconfirmed()
    transactionStorage.swithToWithUnconfirmed()

    appStateStorage.swithToWithUnconfirmed()
  }

  def clearUnconfirmed() {
    accountNodeStorage.clearUnconfirmed()
    storageNodeStorage.clearUnconfirmed()
    evmcodeStorage.clearUnconfirmed()

    blockNumberStorage.clearUnconfirmed()

    blockHeaderStorage.clearUnconfirmed()
    blockBodyStorage.clearUnconfirmed()
    receiptsStorage.clearUnconfirmed()

    totalDifficultyStorage.clearUnconfirmed()
    transactionStorage.clearUnconfirmed()

    appStateStorage.clearUnconfirmed()

    blockNumbers.clearUnconfirmed()
  }
}

