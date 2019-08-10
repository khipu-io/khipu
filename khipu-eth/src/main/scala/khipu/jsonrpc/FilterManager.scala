package khipu.jsonrpc

import akka.actor.{ Actor, Cancellable, Props, Scheduler }
import akka.pattern.{ ask, pipe }
import akka.util.ByteString
import akka.util.Timeout
import khipu.Hash
import khipu.DataWord
import khipu.domain.Address
import khipu.domain.Block
import khipu.domain.Blockchain
import khipu.domain.Receipt
import khipu.jsonrpc.EthService.BlockParam
import khipu.keystore.KeyStore
import khipu.ledger.BloomFilter
import khipu.mining.BlockGenerator
import khipu.store.AppStateStorage
import khipu.transactions.PendingTransactionsService
import khipu.util.FilterConfig
import khipu.util.TxPoolConfig
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

object FilterManager {
  def props(
    blockchain:      Blockchain,
    blockGenerator:  BlockGenerator,
    appStateStorage: AppStateStorage,
    keyStore:        KeyStore.I,
    filterConfig:    FilterConfig,
    txPoolConfig:    TxPoolConfig
  ): Props =
    Props(new FilterManager(blockchain, blockGenerator, appStateStorage, keyStore, filterConfig, txPoolConfig))

  sealed trait Filter {
    def id: DataWord
  }
  final case class LogFilter(
    override val id: DataWord,
    fromBlock:       Option[BlockParam],
    toBlock:         Option[BlockParam],
    address:         Option[Address],
    topics:          List[List[ByteString]]
  ) extends Filter
  final case class BlockFilter(override val id: DataWord) extends Filter
  final case class PendingTransactionFilter(override val id: DataWord) extends Filter

  final case class NewLogFilter(fromBlock: Option[BlockParam], toBlock: Option[BlockParam], address: Option[Address], topics: List[List[ByteString]])
  final case class NewBlockFilter()
  final case class NewPendingTransactionFilter()
  final case class NewFilterResponse(id: DataWord)

  final case class UninstallFilter(id: DataWord)
  final case class UninstallFilterResponse()

  final case class GetFilterLogs(id: DataWord)
  final case class GetFilterChanges(id: DataWord)

  final case class GetLogs(fromBlock: Option[BlockParam], toBlock: Option[BlockParam], address: Option[Address], topics: List[List[ByteString]])

  final case class TxLog(
    logIndex:         Long,
    transactionIndex: Long,
    transactionHash:  Hash,
    blockHash:        Hash,
    blockNumber:      Long,
    address:          Address,
    data:             ByteString,
    topics:           Seq[ByteString]
  )

  sealed trait FilterChanges
  final case class LogFilterChanges(logs: Seq[TxLog]) extends FilterChanges
  final case class BlockFilterChanges(blockHashes: Seq[Hash]) extends FilterChanges
  final case class PendingTransactionFilterChanges(txHashes: Seq[Hash]) extends FilterChanges

  sealed trait FilterLogs
  final case class LogFilterLogs(logs: Seq[TxLog]) extends FilterLogs
  final case class BlockFilterLogs(blockHashes: Seq[Hash]) extends FilterLogs
  final case class PendingTransactionFilterLogs(txHashes: Seq[Hash]) extends FilterLogs

  private case class FilterTimeout(id: DataWord)
}
class FilterManager(
    blockchain:      Blockchain,
    blockGenerator:  BlockGenerator,
    appStateStorage: AppStateStorage,
    keyStore:        KeyStore.I,
    filterConfig:    FilterConfig,
    txPoolConfig:    TxPoolConfig
) extends Actor {

  import FilterManager._
  import context.system

  def pendingTransactionsService = PendingTransactionsService.proxy(system)

  def scheduler: Scheduler = system.scheduler

  val maxBlockHashesChanges = 256

  var filters: Map[DataWord, Filter] = Map.empty

  var lastCheckBlocks: Map[DataWord, Long] = Map.empty

  var lastCheckTimestamps: Map[DataWord, Long] = Map.empty

  var filterTimeouts: Map[DataWord, Cancellable] = Map.empty

  implicit val timeout = Timeout(txPoolConfig.pendingTxManagerQueryTimeout)

  override def receive: Receive = {
    case NewLogFilter(fromBlock, toBlock, address, topics) => addFilterAndSendResponse(LogFilter(generateId(), fromBlock, toBlock, address, topics))
    case NewBlockFilter()                                  => addFilterAndSendResponse(BlockFilter(generateId()))
    case NewPendingTransactionFilter()                     => addFilterAndSendResponse(PendingTransactionFilter(generateId()))
    case UninstallFilter(id)                               => uninstallFilter(id)
    case GetFilterLogs(id)                                 => getFilterLogs(id)
    case GetFilterChanges(id)                              => getFilterChanges(id)
    case FilterTimeout(id)                                 => uninstallFilter(id)
    case gl: GetLogs =>
      val filter = LogFilter(DataWord.Zero, gl.fromBlock, gl.toBlock, gl.address, gl.topics)
      sender() ! LogFilterLogs(getLogs(filter, None))
  }

  private def resetTimeout(id: DataWord) {
    filterTimeouts.get(id).foreach(_.cancel())
    val timeoutCancellable = scheduler.scheduleOnce(filterConfig.filterTimeout, self, FilterTimeout(id))
    filterTimeouts += (id -> timeoutCancellable)
  }

  private def addFilterAndSendResponse(filter: Filter): Unit = {
    filters += (filter.id -> filter)
    lastCheckBlocks += (filter.id -> appStateStorage.getBestBlockNumber)
    lastCheckTimestamps += (filter.id -> System.currentTimeMillis())
    resetTimeout(filter.id)
    sender() ! NewFilterResponse(filter.id)
  }

  private def uninstallFilter(id: DataWord) {
    filters -= id
    lastCheckBlocks -= id
    lastCheckTimestamps -= id
    filterTimeouts.get(id).foreach(_.cancel())
    filterTimeouts -= id
    sender() ! UninstallFilterResponse()
  }

  private def getFilterLogs(id: DataWord) {
    val filterOpt = filters.get(id)
    filterOpt.foreach { _ =>
      lastCheckBlocks += (id -> appStateStorage.getBestBlockNumber)
      lastCheckTimestamps += (id -> System.currentTimeMillis())
    }
    resetTimeout(id)

    filterOpt match {
      case Some(logFilter: LogFilter) =>
        sender() ! LogFilterLogs(getLogs(logFilter))

      case Some(_: BlockFilter) =>
        sender() ! BlockFilterLogs(Nil) // same as geth, returns empty array (otherwise it would have to return hashes of all blocks in the blockchain)

      case Some(_: PendingTransactionFilter) =>
        getPendingTransactions().map { pendingTransactions =>
          PendingTransactionFilterLogs(pendingTransactions.map(_.stx.hash))
        }.pipeTo(sender())

      case None =>
        sender() ! LogFilterLogs(Nil)
    }
  }

  private def getLogs(filter: LogFilter, startingBlockNumber: Option[Long] = None): Seq[TxLog] = {
    val bytesToCheckInBloomFilter = filter.address.map(a => List(a.bytes)).getOrElse(Nil) ::: filter.topics.flatten

    @tailrec
    def recur(currentBlockNumber: Long, toBlockNumber: Long, logsSoFar: Seq[TxLog]): Seq[TxLog] = {
      if (currentBlockNumber > toBlockNumber) {
        logsSoFar
      } else {
        blockchain.getBlockHeaderByNumber(currentBlockNumber) match {
          case Some(header) if bytesToCheckInBloomFilter.isEmpty || BloomFilter.containsAnyOf(header.logsBloom, bytesToCheckInBloomFilter) =>
            blockchain.getReceiptsByHash(header.hash) match {
              case Some(receipts) => recur(
                currentBlockNumber + 1,
                toBlockNumber,
                logsSoFar ++ getLogsFromBlock(filter, Block(header, blockchain.getBlockBodyByHash(header.hash).get), receipts)
              )
              case None => logsSoFar
            }
          case Some(_) => recur(currentBlockNumber + 1, toBlockNumber, logsSoFar)
          case None    => logsSoFar
        }
      }
    }

    val bestBlockNumber = appStateStorage.getBestBlockNumber

    val fromBlockNumber =
      startingBlockNumber.getOrElse(resolveBlockNumber(filter.fromBlock.getOrElse(BlockParam.Latest), bestBlockNumber))

    val toBlockNumber =
      resolveBlockNumber(filter.toBlock.getOrElse(BlockParam.Latest), bestBlockNumber)

    val logs = recur(fromBlockNumber, toBlockNumber, Nil)

    if (filter.toBlock.contains(BlockParam.Pending))
      logs ++ blockGenerator.getPending.map(p => getLogsFromBlock(filter, p.block, p.receipts)).getOrElse(Nil)
    else logs
  }

  private def getFilterChanges(id: DataWord) {
    val bestBlockNumber = appStateStorage.getBestBlockNumber
    val lastCheckBlock = lastCheckBlocks.getOrElse(id, bestBlockNumber)
    val lastCheckTimestamp = lastCheckTimestamps.getOrElse(id, System.currentTimeMillis())

    val filterOpt = filters.get(id)
    filterOpt.foreach { _ =>
      lastCheckBlocks += (id -> bestBlockNumber)
      lastCheckTimestamps += (id -> System.currentTimeMillis())
    }
    resetTimeout(id)

    filterOpt match {
      case Some(logFilter: LogFilter) =>
        sender() ! LogFilterChanges(getLogs(logFilter, Some(lastCheckBlock + 1)))

      case Some(_: BlockFilter) =>
        sender() ! BlockFilterChanges(getBlockHashesAfter(lastCheckBlock).takeRight(maxBlockHashesChanges))

      case Some(_: PendingTransactionFilter) =>
        getPendingTransactions().map { pendingTransactions =>
          val filtered = pendingTransactions.filter(_.addTimestamp > lastCheckTimestamp)
          PendingTransactionFilterChanges(filtered.map(_.stx.hash))
        }.pipeTo(sender())

      case None =>
        sender() ! LogFilterChanges(Nil)
    }
  }

  private def getLogsFromBlock(filter: LogFilter, block: Block, receipts: Seq[Receipt]): Seq[TxLog] = {
    val bytesToCheckInBloomFilter = filter.address.map(a => List(a.bytes)).getOrElse(Nil) ++ filter.topics.flatten

    receipts.zipWithIndex.foldLeft(Nil: Seq[TxLog]) {
      case (logsSoFar, (receipt, txIndex)) =>
        if (bytesToCheckInBloomFilter.isEmpty || BloomFilter.containsAnyOf(receipt.logsBloomFilter, bytesToCheckInBloomFilter)) {
          logsSoFar ++ receipt.logs.zipWithIndex
            .filter { case (log, _) => filter.address.forall(_ == log.loggerAddress) && topicsMatch(log.logTopics, filter.topics) }
            .map {
              case (log, logIndex) =>
                val tx = block.body.transactionList(txIndex)
                TxLog(
                  logIndex = logIndex,
                  transactionIndex = txIndex,
                  transactionHash = tx.hash,
                  blockHash = block.header.hash,
                  blockNumber = block.header.number,
                  address = log.loggerAddress,
                  data = log.data,
                  topics = log.logTopics
                )
            }
        } else logsSoFar
    }
  }

  private def topicsMatch(logTopics: Seq[ByteString], filterTopics: Seq[Seq[ByteString]]): Boolean = {
    logTopics.size >= filterTopics.size &&
      (filterTopics zip logTopics).forall { case (filter, log) => filter.isEmpty || filter.contains(log) }
  }

  private def getBlockHashesAfter(blockNumber: Long): Seq[Hash] = {
    val bestBlock = appStateStorage.getBestBlockNumber

    @tailrec
    def recur(currentBlockNumber: Long, hashesSoFar: Seq[Hash]): Seq[Hash] = {
      if (currentBlockNumber > bestBlock) {
        hashesSoFar
      } else blockchain.getBlockHeaderByNumber(currentBlockNumber) match {
        case Some(header) => recur(currentBlockNumber + 1, hashesSoFar :+ header.hash)
        case None         => hashesSoFar
      }
    }

    recur(blockNumber + 1, Nil)
  }

  private def getPendingTransactions(): Future[Seq[PendingTransactionsService.PendingTransaction]] = {
    (pendingTransactionsService ? PendingTransactionsService.GetPendingTransactions)
      .mapTo[PendingTransactionsService.PendingTransactionsResponse]
      .flatMap {
        case PendingTransactionsService.PendingTransactionsResponse(pendingTransactions) =>
          keyStore.listAccounts() match {
            case Right(accounts) =>
              Future.successful(pendingTransactions.filter(pt => accounts.contains(pt.stx.sender)))
            case Left(_) => Future.failed(new RuntimeException("Cannot get account list"))
          }
      }
  }

  private def generateId(): DataWord = DataWord.safe(Random.nextLong().abs)

  private def resolveBlockNumber(blockParam: BlockParam, bestBlockNumber: Long): Long = {
    blockParam match {
      case BlockParam.WithNumber(blockNumber) => blockNumber.longValue
      case BlockParam.Earliest                => 0
      case BlockParam.Latest                  => bestBlockNumber
      case BlockParam.Pending                 => bestBlockNumber
    }
  }
}

