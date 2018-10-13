package khipu.jsonrpc

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.ByteString
import akka.util.Timeout
import java.math.BigInteger
import java.time.Duration
import java.util.function.UnaryOperator
import java.util.Date
import java.util.concurrent.atomic.AtomicReference
import khipu.Hash
import khipu.domain.Account
import khipu.domain.Address
import khipu.domain.Block
import khipu.domain.BlockHeader
import khipu.domain.Blockchain
import khipu.domain.Receipt
import khipu.domain.SignedTransaction
import khipu.blockchain.sync.SyncService
import khipu.blockchain.sync.SyncService.MinedBlock
import khipu.crypto
import khipu.crypto.ECDSASignature
import khipu.domain.Transaction
import khipu.jsonrpc.FilterManager.{ FilterChanges, FilterLogs, LogFilterLogs, TxLog }
import khipu.keystore.KeyStore
import khipu.ledger.Ledger
import khipu.mining.BlockGenerator
import khipu.util
import khipu.store.AppStateStorage
import khipu.store.TransactionMappingStorage.TransactionLocation
import khipu.transactions.PendingTransactionsService
import khipu.ommers.OmmersPool
import khipu.rlp
import khipu.rlp.RLPList
import khipu.rlp.RLPImplicitConversions._
import khipu.vm.UInt256
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success, Try }

// scalastyle:off number.of.methods number.of.types
object EthService {

  val CurrentProtocolVersion = 63

  final case class ProtocolVersionRequest()
  final case class ProtocolVersionResponse(value: String)

  final case class BestBlockNumberRequest()
  final case class BestBlockNumberResponse(bestBlockNumber: Long)

  final case class TxCountByBlockHashRequest(blockHash: Hash)
  final case class TxCountByBlockHashResponse(txsQuantity: Option[Int])

  final case class BlockByBlockHashRequest(blockHash: Hash, fullTxs: Boolean)
  final case class BlockByBlockHashResponse(blockResponse: Option[BlockResponse])

  final case class BlockByNumberRequest(block: BlockParam, fullTxs: Boolean)
  final case class BlockByNumberResponse(blockResponse: Option[BlockResponse])

  final case class GetTransactionByBlockHashAndIndexRequest(blockHash: Hash, transactionIndex: Long)
  final case class GetTransactionByBlockHashAndIndexResponse(transactionResponse: Option[TransactionResponse])

  final case class UncleByBlockHashAndIndexRequest(blockHash: Hash, uncleIndex: Long)
  final case class UncleByBlockHashAndIndexResponse(uncleBlockResponse: Option[BlockResponse])

  final case class UncleByBlockNumberAndIndexRequest(block: BlockParam, uncleIndex: Long)
  final case class UncleByBlockNumberAndIndexResponse(uncleBlockResponse: Option[BlockResponse])

  final case class SubmitHashRateRequest(hashRate: UInt256, id: Hash)
  final case class SubmitHashRateResponse(success: Boolean)

  final case class GetMiningRequest()
  final case class GetMiningResponse(isMining: Boolean)

  final case class GetTransactionByHashRequest(txHash: Hash)
  final case class GetTransactionByHashResponse(txResponse: Option[TransactionResponse])

  final case class GetTransactionReceiptRequest(txHash: Hash)
  final case class GetTransactionReceiptResponse(txResponse: Option[TransactionReceiptResponse])

  final case class GetTransactionByBlockNumberAndIndexRequest(block: BlockParam, transactionIndex: Long)
  final case class GetTransactionByBlockNumberAndIndexResponse(transactionResponse: Option[TransactionResponse])

  final case class GetHashRateRequest()
  final case class GetHashRateResponse(hashRate: UInt256)

  final case class GetGasPriceRequest()
  final case class GetGasPriceResponse(price: UInt256)

  final case class GetWorkRequest()
  final case class GetWorkResponse(powHeaderHash: Hash, dagSeed: ByteString, target: ByteString)

  final case class SubmitWorkRequest(nonce: ByteString, powHeaderHash: Hash, mixHash: Hash)
  final case class SubmitWorkResponse(success: Boolean)

  final case class SyncingRequest()
  final case class SyncingStatus(startingBlock: Long, currentBlock: Long, highestBlock: Long)
  final case class SyncingResponse(syncStatus: Option[SyncingStatus])

  final case class SendRawTransactionRequest(data: ByteString)
  final case class SendRawTransactionResponse(transactionHash: Hash)

  sealed trait BlockParam

  object BlockParam {
    final case class WithNumber(n: UInt256) extends BlockParam
    case object Latest extends BlockParam
    case object Pending extends BlockParam
    case object Earliest extends BlockParam
  }

  final case class CallTx(
    from:     Option[ByteString],
    to:       Option[ByteString],
    gas:      Option[Long],
    gasPrice: UInt256,
    value:    UInt256,
    data:     ByteString
  )
  final case class CallRequest(tx: CallTx, block: BlockParam)
  final case class CallResponse(returnData: ByteString)
  final case class EstimateGasResponse(gas: Long)

  final case class GetCodeRequest(address: Address, block: BlockParam)
  final case class GetCodeResponse(result: ByteString)

  final case class GetUncleCountByBlockNumberRequest(block: BlockParam)
  final case class GetUncleCountByBlockNumberResponse(result: Long)

  final case class GetUncleCountByBlockHashRequest(blockHash: Hash)
  final case class GetUncleCountByBlockHashResponse(result: Long)

  final case class GetCoinbaseRequest()
  final case class GetCoinbaseResponse(address: Address)

  final case class GetBlockTransactionCountByNumberRequest(block: BlockParam)
  final case class GetBlockTransactionCountByNumberResponse(result: Long)

  final case class GetBalanceRequest(address: Address, block: BlockParam)
  final case class GetBalanceResponse(value: UInt256)

  final case class GetStorageAtRequest(address: Address, position: UInt256, block: BlockParam)
  final case class GetStorageAtResponse(value: ByteString)

  final case class GetTransactionCountRequest(address: Address, block: BlockParam)
  final case class GetTransactionCountResponse(value: UInt256)

  final case class ResolvedBlock(block: Block, pending: Boolean)

  final case class NewFilterRequest(filter: Filter)
  final case class Filter(
    fromBlock: Option[BlockParam],
    toBlock:   Option[BlockParam],
    address:   Option[Address],
    topics:    Seq[Seq[ByteString]]
  )

  final case class NewBlockFilterRequest()
  final case class NewPendingTransactionFilterRequest()

  final case class NewFilterResponse(filterId: UInt256)

  final case class UninstallFilterRequest(filterId: UInt256)
  final case class UninstallFilterResponse(success: Boolean)

  final case class GetFilterChangesRequest(filterId: UInt256)
  final case class GetFilterChangesResponse(filterChanges: FilterChanges)

  final case class GetFilterLogsRequest(filterId: UInt256)
  final case class GetFilterLogsResponse(filterLogs: FilterLogs)

  final case class GetLogsRequest(filter: Filter)
  final case class GetLogsResponse(filterLogs: LogFilterLogs)
}

class EthService(
    blockchain:       Blockchain,
    blockGenerator:   BlockGenerator,
    appStateStorage:  AppStateStorage,
    miningConfig:     util.MiningConfig,
    ledger:           Ledger.I,
    keyStore:         KeyStore.I,
    filterManager:    ActorRef,
    filterConfig:     util.FilterConfig,
    blockchainConfig: util.BlockchainConfig
)(implicit system: ActorSystem) {
  import EthService._

  def syncService = SyncService.proxy(system)

  val hashRate: AtomicReference[Map[Hash, (UInt256, Date)]] = new AtomicReference[Map[Hash, (UInt256, Date)]](Map())
  val lastActive = new AtomicReference[Option[Date]](None)

  def pendingTransactionsService = PendingTransactionsService.proxy(system)

  def protocolVersion(req: ProtocolVersionRequest): ServiceResponse[ProtocolVersionResponse] =
    Future.successful(Right(ProtocolVersionResponse(f"0x$CurrentProtocolVersion%x")))

  /**
   * eth_blockNumber that returns the number of most recent block.
   *
   * @return Current block number the client is on.
   */
  def bestBlockNumber(req: BestBlockNumberRequest): ServiceResponse[BestBlockNumberResponse] = Future {
    Right(BestBlockNumberResponse(appStateStorage.getBestBlockNumber()))
  }

  /**
   * Implements the eth_getBlockTransactionCountByHash method that fetches the number of txs that a certain block has.
   *
   * @param request with the hash of the block requested
   * @return the number of txs that the block has or None if the client doesn't have the block requested
   */
  def getBlockTransactionCountByHash(request: TxCountByBlockHashRequest): ServiceResponse[TxCountByBlockHashResponse] = Future {
    val txsCount = blockchain.getBlockBodyByHash(request.blockHash).map(_.transactionList.size)
    Right(TxCountByBlockHashResponse(txsCount))
  }

  /**
   * Implements the eth_getBlockByHash method that fetches a requested block.
   *
   * @param request with the hash of the block requested
   * @return the block requested or None if the client doesn't have the block
   */
  def getByBlockHash(request: BlockByBlockHashRequest): ServiceResponse[BlockByBlockHashResponse] = Future {
    val BlockByBlockHashRequest(blockHash, fullTxs) = request
    val blockOpt = blockchain.getBlockByHash(blockHash)
    val totalDifficulty = blockchain.getTotalDifficultyByHash(blockHash)

    val blockResponseOpt = blockOpt.map(block => BlockResponse(block, totalDifficulty, fullTxs = fullTxs))
    Right(BlockByBlockHashResponse(blockResponseOpt))
  }

  /**
   * Implements the eth_getBlockByNumber method that fetches a requested block.
   *
   * @param request with the block requested (by it's number or by tag)
   * @return the block requested or None if the client doesn't have the block
   */
  def getBlockByNumber(request: BlockByNumberRequest): ServiceResponse[BlockByNumberResponse] = Future {
    val BlockByNumberRequest(blockParam, fullTxs) = request
    val blockResponseOpt = resolveBlock(blockParam).toOption.map {
      case ResolvedBlock(block, pending) =>
        val totalDifficulty = blockchain.getTotalDifficultyByHash(block.header.hash)
        BlockResponse(block, totalDifficulty, fullTxs = fullTxs, pendingBlock = pending)
    }
    Right(BlockByNumberResponse(blockResponseOpt))
  }

  /**
   * Implements the eth_getTransactionByHash method that fetches a requested tx.
   * The tx requested will be fetched from the pending tx pool or from the already executed txs (depending on the tx state)
   *
   * @param req with the tx requested (by it's hash)
   * @return the tx requested or None if the client doesn't have the tx
   */
  def getTransactionByHash(req: GetTransactionByHashRequest): ServiceResponse[GetTransactionByHashResponse] = {
    val maybeTxPendingResponse: Future[Option[TransactionResponse]] = getTransactionsFromPool.map {
      _.pendingTransactions.map(_.stx).find(_.hash == req.txHash).map(TransactionResponse(_))
    }

    val maybeTxResponse: Future[Option[TransactionResponse]] = maybeTxPendingResponse.flatMap { txPending =>
      Future {
        txPending.orElse {
          for {
            TransactionLocation(blockHash, txIndex) <- blockchain.getTransactionLocation(req.txHash)
            Block(header, body) <- blockchain.getBlockByHash(blockHash)
            stx <- body.transactionList.lift(txIndex)
          } yield TransactionResponse(stx, Some(header), Some(txIndex))
        }
      }
    }

    maybeTxResponse.map(txResponse => Right(GetTransactionByHashResponse(txResponse)))
  }

  def getTransactionReceipt(req: GetTransactionReceiptRequest): ServiceResponse[GetTransactionReceiptResponse] = Future {
    val result: Option[TransactionReceiptResponse] = for {
      TransactionLocation(blockHash, txIndex) <- blockchain.getTransactionLocation(req.txHash)
      Block(header, body) <- blockchain.getBlockByHash(blockHash)
      stx <- body.transactionList.lift(txIndex)
      receipts <- blockchain.getReceiptsByHash(blockHash)
      receipt: Receipt <- receipts.lift(txIndex)
    } yield {

      val contractAddress = if (stx.tx.isContractCreation) {
        //do not subtract 1 from nonce because in transaction we have nonce of account before transaction execution
        val hash = crypto.kec256(rlp.encode(RLPList(stx.sender.bytes, rlp.toRLPEncodable(stx.tx.nonce))))
        Some(Address(hash))
      } else {
        None
      }

      TransactionReceiptResponse(
        transactionHash = stx.hash,
        transactionIndex = txIndex,
        blockNumber = header.number,
        blockHash = header.hash,
        cumulativeGasUsed = receipt.cumulativeGasUsed,
        gasUsed = if (txIndex == 0) receipt.cumulativeGasUsed else receipt.cumulativeGasUsed - receipts(txIndex - 1).cumulativeGasUsed,
        contractAddress = contractAddress,
        logs = receipt.logs.zipWithIndex.map {
          case (txLog, index) =>
            TxLog(
              logIndex = index,
              transactionIndex = txIndex,
              transactionHash = stx.hash,
              blockHash = header.hash,
              blockNumber = header.number,
              address = txLog.loggerAddress,
              data = txLog.data,
              topics = txLog.logTopics
            )
        }
      )
    }

    Right(GetTransactionReceiptResponse(result))
  }

  /**
   * eth_getTransactionByBlockHashAndIndex that returns information about a transaction by block hash and
   * transaction index position.
   *
   * @return the tx requested or None if the client doesn't have the block or if there's no tx in the that index
   */
  def getTransactionByBlockHashAndIndexRequest(req: GetTransactionByBlockHashAndIndexRequest): ServiceResponse[GetTransactionByBlockHashAndIndexResponse] = Future {
    import req._
    val maybeTransactionResponse = blockchain.getBlockByHash(blockHash).flatMap {
      blockWithTx =>
        val blockTxs = blockWithTx.body.transactionList
        if (transactionIndex >= 0 && transactionIndex < blockTxs.size)
          Some(TransactionResponse(blockTxs(transactionIndex.toInt), Some(blockWithTx.header), Some(transactionIndex.toInt)))
        else None
    }
    Right(GetTransactionByBlockHashAndIndexResponse(maybeTransactionResponse))
  }

  /**
   * Implements the eth_getUncleByBlockHashAndIndex method that fetches an uncle from a certain index in a requested block.
   *
   * @param request with the hash of the block and the index of the uncle requested
   * @return the uncle that the block has at the given index or None if the client doesn't have the block or if there's no uncle in that index
   */
  def getUncleByBlockHashAndIndex(request: UncleByBlockHashAndIndexRequest): ServiceResponse[UncleByBlockHashAndIndexResponse] = Future {
    val UncleByBlockHashAndIndexRequest(blockHash, uncleIndex) = request
    val uncleHeaderOpt = blockchain.getBlockBodyByHash(blockHash)
      .flatMap { body =>
        if (uncleIndex >= 0 && uncleIndex < body.uncleNodesList.size)
          Some(body.uncleNodesList.apply(uncleIndex.toInt))
        else
          None
      }
    val totalDifficulty = uncleHeaderOpt.flatMap(uncleHeader => blockchain.getTotalDifficultyByHash(uncleHeader.hash))

    //The block in the response will not have any txs or uncles
    val uncleBlockResponseOpt = uncleHeaderOpt.map { uncleHeader =>
      BlockResponse(blockHeader = uncleHeader, totalDifficulty = totalDifficulty, pendingBlock = false)
    }
    Right(UncleByBlockHashAndIndexResponse(uncleBlockResponseOpt))
  }

  /**
   * Implements the eth_getUncleByBlockNumberAndIndex method that fetches an uncle from a certain index in a requested block.
   *
   * @param request with the number/tag of the block and the index of the uncle requested
   * @return the uncle that the block has at the given index or None if the client doesn't have the block or if there's no uncle in that index
   */
  def getUncleByBlockNumberAndIndex(request: UncleByBlockNumberAndIndexRequest): ServiceResponse[UncleByBlockNumberAndIndexResponse] = Future {
    val UncleByBlockNumberAndIndexRequest(blockParam, uncleIndex) = request
    val uncleBlockResponseOpt = resolveBlock(blockParam).toOption
      .flatMap {
        case ResolvedBlock(block, pending) =>
          if (uncleIndex >= 0 && uncleIndex < block.body.uncleNodesList.size) {
            val uncleHeader = block.body.uncleNodesList.apply(uncleIndex.toInt)
            val totalDifficulty = blockchain.getTotalDifficultyByHash(uncleHeader.hash)

            //The block in the response will not have any txs or uncles
            Some(BlockResponse(blockHeader = uncleHeader, totalDifficulty = totalDifficulty, pendingBlock = pending))
          } else
            None
      }

    Right(UncleByBlockNumberAndIndexResponse(uncleBlockResponseOpt))
  }

  def submitHashRate(req: SubmitHashRateRequest): ServiceResponse[SubmitHashRateResponse] = {
    reportActive()
    hashRate.updateAndGet(new UnaryOperator[Map[Hash, (UInt256, Date)]] {
      override def apply(t: Map[Hash, (UInt256, Date)]): Map[Hash, (UInt256, Date)] = {
        val now = new Date
        removeObsoleteHashrates(now, t + (req.id -> (req.hashRate, now)))
      }
    })

    Future.successful(Right(SubmitHashRateResponse(true)))
  }

  def getGetGasPrice(req: GetGasPriceRequest): ServiceResponse[GetGasPriceResponse] = {
    val blockDifference = 30
    val bestBlock = appStateStorage.getBestBlockNumber()

    Future {
      val gasPrice = ((bestBlock - blockDifference) to bestBlock)
        .flatMap(blockchain.getBlockByNumber)
        .flatMap(_.body.transactionList)
        .map(_.tx.gasPrice)
      if (gasPrice.nonEmpty) {
        val avgGasPrice = gasPrice.foldLeft(UInt256.Zero)(_ + _) / UInt256(gasPrice.length)
        Right(GetGasPriceResponse(avgGasPrice))
      } else {
        Right(GetGasPriceResponse(UInt256.Zero))
      }
    }
  }

  def getMining(req: GetMiningRequest): ServiceResponse[GetMiningResponse] = {
    val isMining = lastActive.updateAndGet(new UnaryOperator[Option[Date]] {
      override def apply(e: Option[Date]): Option[Date] = {
        e.filter { time => Duration.between(time.toInstant, (new Date).toInstant).toMillis < miningConfig.activeTimeout.toMillis }
      }
    }).isDefined
    Future.successful(Right(GetMiningResponse(isMining)))
  }

  private def reportActive() = {
    val now = new Date()
    lastActive.updateAndGet(new UnaryOperator[Option[Date]] {
      override def apply(e: Option[Date]): Option[Date] = {
        Some(now)
      }
    })
  }

  def getHashRate(req: GetHashRateRequest): ServiceResponse[GetHashRateResponse] = {
    val hashRates: Map[Hash, (UInt256, Date)] = hashRate.updateAndGet(new UnaryOperator[Map[Hash, (UInt256, Date)]] {
      override def apply(t: Map[Hash, (UInt256, Date)]): Map[Hash, (UInt256, Date)] = {
        removeObsoleteHashrates(new Date, t)
      }
    })

    //sum all reported hashRates
    Future.successful(Right(GetHashRateResponse(hashRates.mapValues { case (hr, _) => hr }.values.foldLeft(UInt256.Zero)(_ + _))))
  }

  private def removeObsoleteHashrates(now: Date, rates: Map[Hash, (UInt256, Date)]): Map[Hash, (UInt256, Date)] = {
    rates.filter {
      case (_, (_, reported)) =>
        Duration.between(reported.toInstant, now.toInstant).toMillis < miningConfig.activeTimeout.toMillis
    }
  }

  def getWork(req: GetWorkRequest): ServiceResponse[GetWorkResponse] = {
    reportActive()
    import khipu.mining.pow.PowCache._

    val blockNumber = appStateStorage.getBestBlockNumber() + 1

    getOmmersFromPool(blockNumber).zip(getTransactionsFromPool).map {
      case (ommers, pendingTxs) =>
        blockGenerator.generateBlockForMining(blockNumber, pendingTxs.pendingTransactions.map(_.stx), ommers.headers, miningConfig.coinbase) match {
          case Right(pb) =>
            Right(GetWorkResponse(
              powHeaderHash = Hash(crypto.kec256(BlockHeader.getEncodedWithoutNonce(pb.block.header))),
              dagSeed = seedForBlock(pb.block.header.number),
              target = ByteString((UInt256.Modulus / pb.block.header.difficulty).bigEndianMag)
            ))
          case Left(err) =>
            //log.error(s"unable to prepare block because of $err")
            Left(JsonRpcErrors.InternalError)
        }
    }
  }

  private def getOmmersFromPool(blockNumber: Long) = {
    implicit val timeout = Timeout(miningConfig.ommerPoolQueryTimeout)

    (syncService ? OmmersPool.GetOmmers(blockNumber)).mapTo[OmmersPool.Ommers]
      .recover {
        case ex =>
          //log.error("failed to get ommer, mining block with empty ommers list", ex)
          OmmersPool.Ommers(Nil)
      }
  }

  private def getTransactionsFromPool = {
    implicit val timeout = Timeout(miningConfig.ommerPoolQueryTimeout)

    (pendingTransactionsService ? PendingTransactionsService.GetPendingTransactions).mapTo[PendingTransactionsService.PendingTransactionsResponse]
      .recover {
        case ex =>
          //log.error("failed to get transactions, mining block with empty transactions list", ex)
          PendingTransactionsService.PendingTransactionsResponse(Nil)
      }
  }

  def getCoinbase(req: GetCoinbaseRequest): ServiceResponse[GetCoinbaseResponse] =
    Future.successful(Right(GetCoinbaseResponse(miningConfig.coinbase)))

  def submitWork(req: SubmitWorkRequest): ServiceResponse[SubmitWorkResponse] = {
    reportActive()
    Future {
      blockGenerator.getPrepared(req.powHeaderHash) match {
        case Some(pendingBlock) if appStateStorage.getBestBlockNumber <= pendingBlock.block.header.number =>
          import pendingBlock._
          syncService ! MinedBlock(block.copy(header = block.header.copy(nonce = req.nonce, mixHash = req.mixHash)))
          Right(SubmitWorkResponse(true))
        case _ =>
          Right(SubmitWorkResponse(false))
      }
    }
  }

  /**
   * Implements the eth_syncing method that returns syncing information if the node is syncing.
   *
   * @return The syncing status if the node is syncing or None if not
   */
  def syncing(req: SyncingRequest): ServiceResponse[SyncingResponse] = Future {
    val currentBlock = appStateStorage.getBestBlockNumber()
    val highestBlock = appStateStorage.getEstimatedHighestBlock()

    //The node is syncing if there's any block that other peers have and this peer doesn't
    val maybeSyncStatus =
      if (currentBlock < highestBlock)
        Some(SyncingStatus(
          startingBlock = appStateStorage.getSyncStartingBlock(),
          currentBlock = currentBlock,
          highestBlock = highestBlock
        ))
      else
        None
    Right(SyncingResponse(maybeSyncStatus))
  }

  def sendRawTransaction(req: SendRawTransactionRequest): ServiceResponse[SendRawTransactionResponse] = {
    import khipu.network.p2p.messages.CommonMessages.SignedTransactions.SignedTransactionDec

    Try(req.data.toArray.toSignedTransaction) match {
      case Success(signedTransaction) =>
        pendingTransactionsService ! PendingTransactionsService.AddOrOverrideTransaction(signedTransaction)
        Future.successful(Right(SendRawTransactionResponse(signedTransaction.hash)))
      case Failure(_) =>
        Future.successful(Left(JsonRpcErrors.InvalidRequest))
    }
  }

  def call(req: CallRequest): ServiceResponse[CallResponse] = {
    Future {
      doCall(req).map(r => CallResponse(r.vmReturnData))
    }
  }

  def estimateGas(req: CallRequest): ServiceResponse[EstimateGasResponse] = {
    Future {
      doCall(req).map(r => EstimateGasResponse(r.gasUsed))
    }
  }

  def getCode(req: GetCodeRequest): ServiceResponse[GetCodeResponse] = {
    Future {
      resolveBlock(req.block).map {
        case ResolvedBlock(block, _) =>
          val world = blockchain.getWorldState(block.header.number, blockchainConfig.accountStartNonce, Some(block.header.stateRoot))
          GetCodeResponse(world.getCode(req.address))
      }
    }
  }

  def getUncleCountByBlockNumber(req: GetUncleCountByBlockNumberRequest): ServiceResponse[GetUncleCountByBlockNumberResponse] = {
    Future {
      resolveBlock(req.block).map {
        case ResolvedBlock(block, _) =>
          GetUncleCountByBlockNumberResponse(block.body.uncleNodesList.size)
      }
    }
  }

  def getUncleCountByBlockHash(req: GetUncleCountByBlockHashRequest): ServiceResponse[GetUncleCountByBlockHashResponse] = {
    Future {
      blockchain.getBlockBodyByHash(req.blockHash) match {
        case Some(blockBody) =>
          Right(GetUncleCountByBlockHashResponse(blockBody.uncleNodesList.size))
        case None =>
          Left(JsonRpcErrors.InvalidParams(s"Block with hash ${req.blockHash.hexString} not found"))
      }
    }
  }

  def getBlockTransactionCountByNumber(req: GetBlockTransactionCountByNumberRequest): ServiceResponse[GetBlockTransactionCountByNumberResponse] = {
    Future {
      resolveBlock(req.block).map {
        case ResolvedBlock(block, _) =>
          GetBlockTransactionCountByNumberResponse(block.body.transactionList.size)
      }
    }
  }

  def getTransactionByBlockNumberAndIndexRequest(req: GetTransactionByBlockNumberAndIndexRequest): ServiceResponse[GetTransactionByBlockNumberAndIndexResponse] = Future {
    import req._
    resolveBlock(block).map {
      blockWithTx =>
        val blockTxs = blockWithTx.block.body.transactionList
        if (transactionIndex >= 0 && transactionIndex < blockTxs.size)
          GetTransactionByBlockNumberAndIndexResponse(
            Some(TransactionResponse(
              blockTxs(transactionIndex.toInt),
              Some(blockWithTx.block.header),
              Some(transactionIndex.toInt)
            ))
          )
        else
          GetTransactionByBlockNumberAndIndexResponse(None)
    }.left.flatMap(_ => Right(GetTransactionByBlockNumberAndIndexResponse(None)))
  }

  def getBalance(req: GetBalanceRequest): ServiceResponse[GetBalanceResponse] = {
    Future {
      withAccount(req.address, req.block) { account =>
        GetBalanceResponse(account.balance)
      }
    }
  }

  def getStorageAt(req: GetStorageAtRequest): ServiceResponse[GetStorageAtResponse] = {
    Future {
      withAccount(req.address, req.block) { account =>
        GetStorageAtResponse(blockchain.getAccountStorageAt(account.stateRoot, req.position))
      }
    }
  }

  def getTransactionCount(req: GetTransactionCountRequest): ServiceResponse[GetTransactionCountResponse] = {
    Future {
      withAccount(req.address, req.block) { account =>
        GetTransactionCountResponse(account.nonce)
      }
    }
  }

  def newFilter(req: NewFilterRequest): ServiceResponse[NewFilterResponse] = {
    implicit val timeout = Timeout(filterConfig.filterManagerQueryTimeout)

    import req.filter._
    (filterManager ? FilterManager.NewLogFilter(fromBlock, toBlock, address, topics)).mapTo[FilterManager.NewFilterResponse].map { resp =>
      Right(NewFilterResponse(resp.id))
    }
  }

  def newBlockFilter(req: NewBlockFilterRequest): ServiceResponse[NewFilterResponse] = {
    implicit val timeout = Timeout(filterConfig.filterManagerQueryTimeout)

    (filterManager ? FilterManager.NewBlockFilter()).mapTo[FilterManager.NewFilterResponse].map { resp =>
      Right(NewFilterResponse(resp.id))
    }
  }

  def newPendingTransactionFilter(req: NewPendingTransactionFilterRequest): ServiceResponse[NewFilterResponse] = {
    implicit val timeout = Timeout(filterConfig.filterManagerQueryTimeout)

    (filterManager ? FilterManager.NewPendingTransactionFilter()).mapTo[FilterManager.NewFilterResponse].map { resp =>
      Right(NewFilterResponse(resp.id))
    }
  }

  def uninstallFilter(req: UninstallFilterRequest): ServiceResponse[UninstallFilterResponse] = {
    implicit val timeout = Timeout(filterConfig.filterManagerQueryTimeout)

    (filterManager ? FilterManager.UninstallFilter(req.filterId)).mapTo[FilterManager.UninstallFilterResponse].map { _ =>
      Right(UninstallFilterResponse(success = true))
    }
  }

  def getFilterChanges(req: GetFilterChangesRequest): ServiceResponse[GetFilterChangesResponse] = {
    implicit val timeout = Timeout(filterConfig.filterManagerQueryTimeout)

    (filterManager ? FilterManager.GetFilterChanges(req.filterId)).mapTo[FilterManager.FilterChanges].map { filterChanges =>
      Right(GetFilterChangesResponse(filterChanges))
    }
  }

  def getFilterLogs(req: GetFilterLogsRequest): ServiceResponse[GetFilterLogsResponse] = {
    implicit val timeout = Timeout(filterConfig.filterManagerQueryTimeout)

    (filterManager ? FilterManager.GetFilterLogs(req.filterId)).mapTo[FilterManager.FilterLogs].map { filterLogs =>
      Right(GetFilterLogsResponse(filterLogs))
    }
  }

  def getLogs(req: GetLogsRequest): ServiceResponse[GetLogsResponse] = {
    implicit val timeout = Timeout(filterConfig.filterManagerQueryTimeout)
    import req.filter._

    (filterManager ? FilterManager.GetLogs(fromBlock, toBlock, address, topics)).mapTo[FilterManager.LogFilterLogs].map { filterLogs =>
      Right(GetLogsResponse(filterLogs))
    }
  }

  private def withAccount[T](address: Address, blockParam: BlockParam)(f: Account => T): Either[JsonRpcError, T] = {
    resolveBlock(blockParam).map {
      case ResolvedBlock(block, _) =>
        f(blockchain.getAccount(address, block.header.number).getOrElse(Account.empty(blockchainConfig.accountStartNonce)))
    }
  }

  private def resolveBlock(blockParam: BlockParam): Either[JsonRpcError, ResolvedBlock] = {
    def getBlock(number: Long): Either[JsonRpcError, Block] = {
      blockchain.getBlockByNumber(number)
        .map(Right.apply)
        .getOrElse(Left(JsonRpcErrors.InvalidParams(s"Block $number not found")))
    }

    blockParam match {
      case BlockParam.WithNumber(blockNumber) => getBlock(blockNumber.longValue).map(ResolvedBlock(_, pending = false))
      case BlockParam.Earliest                => getBlock(0).map(ResolvedBlock(_, pending = false))
      case BlockParam.Latest                  => getBlock(appStateStorage.getBestBlockNumber()).map(ResolvedBlock(_, pending = false))
      case BlockParam.Pending =>
        blockGenerator.getPending.map(pb => ResolvedBlock(pb.block, pending = true))
          .map(Right.apply)
          .getOrElse(resolveBlock(BlockParam.Latest)) //Default behavior in other clients
    }
  }

  private def doCall(req: CallRequest): Either[JsonRpcError, Ledger.TxResult] = {
    val fromAddress = req.tx.from
      .map(Address.apply) // `from` param, if specified
      .getOrElse(
        keyStore
          .listAccounts().getOrElse(Nil).headOption // first account, if exists and `from` param not specified
          .getOrElse(Address(0))
      ) // 0x0 default

    val toAddress = req.tx.to.map(Address.apply)

    // TODO improvement analysis is suggested in EC-199
    val gasLimit: Either[JsonRpcError, Long] = {
      if (req.tx.gas.isDefined) Right[JsonRpcError, Long](req.tx.gas.get)
      else resolveBlock(BlockParam.Latest).map(r => r.block.header.gasLimit)
    }

    gasLimit.flatMap { gl =>
      val tx = Transaction(UInt256.Zero, req.tx.gasPrice, gl, toAddress, req.tx.value, req.tx.data)
      val fakeSignature = ECDSASignature(BigInteger.ZERO, BigInteger.ZERO, 0.toByte)
      // TODO chainId
      val stx = SignedTransaction(tx, fakeSignature, None, fromAddress)

      resolveBlock(req.block).map {
        case ResolvedBlock(block, _) =>
          ledger.simulateTransaction(stx, block.header)
      }
    }

  }
}
