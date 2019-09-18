package khipu.ledger

import akka.actor.ActorSystem
import akka.event.Logging
import akka.pattern.ask
import akka.util.ByteString
import khipu.Hash
import khipu.DataWord
import khipu.config.BlockchainConfig
import khipu.domain.Account
import khipu.domain.Address
import khipu.domain.Block
import khipu.domain.Blockchain
import khipu.domain.BlockHeader
import khipu.domain.Receipt
import khipu.domain.SignedTransaction
import khipu.domain.Transaction
import khipu.domain.TxLogEntry
import khipu.validators._
import khipu.trie.MerklePatriciaTrie.MPTNodeMissingException
import khipu.util.SimpleMap
import khipu.validators.BlockValidator
import khipu.validators.SignedTransactionValidator
import khipu.vm.EvmConfig
import khipu.vm.OutOfGas
import khipu.vm.PrecompiledContracts
import khipu.vm.Program
import khipu.vm.ProgramContext
import khipu.vm.ProgramError
import khipu.vm.ProgramResult
import khipu.vm.ProgramState
import khipu.vm.VM
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

/**
 * EIP-161
 * https://github.com/ethereum/ethereumj/commit/7a08a68c9eb1515737739754093d5668864c69cb#diff-1217705a51746d79276a325e55ec22b5
 */
object Ledger {

  trait I {
    def validateBlocksBeforeExecution(blocks: Seq[Block], validators: Validators)(implicit executor: ExecutionContext): Future[(Vector[Block], Option[BlockExecutionError])]
    def executeBlock(block: Block, validators: Validators)(implicit executor: ExecutionContext): Future[Either[BlockExecutionError, BlockResult]]
    def prepareBlock(block: Block, validators: Validators)(implicit executor: ExecutionContext): Future[BlockPreparationResult]
    def simulateTransaction(stx: SignedTransaction, blockHeader: BlockHeader)(implicit executor: ExecutionContext): TxResult
  }

  type PC = ProgramContext[BlockWorldState, TrieStorage]
  type PR = ProgramResult[BlockWorldState, TrieStorage]

  final case class Stats(parallelRate: Double, dbReadTimePerc: Double, cacheHitRates: List[Double], cacheReadCount: Long)

  final case class BlockResult(world: BlockWorldState, gasUsed: Long = 0, receipts: Seq[Receipt] = Nil, stats: Stats)
  final case class BlockPreparationResult(block: Block, blockResult: BlockResult, stateRootHash: Hash)
  final case class TxResult(stx: SignedTransaction, postWorld: BlockWorldState, intermediateRootHash: Hash, gasUsed: Long, txFee: DataWord, logs: Seq[TxLogEntry], touchedAddresses: Set[Address], vmReturnData: ByteString, error: Option[ProgramError], isRevert: Boolean, parallelRaceConditions: Set[ProgramState.ParallelRace])

  sealed trait BlockExecutionError {
    def blockNumber: Long
    def reason: String
  }
  final case class ValidationBeforeExecError(blockNumber: Long, reason: String) extends BlockExecutionError
  final case class TxsExecutionError(blockNumber: Long, stx: SignedTransaction, stateBeforeError: StateBeforeFailure, error: SignedTransactionError) extends BlockExecutionError { def reason = error.toString }
  final case class ValidationAfterExecError(blockNumber: Long, reason: String) extends BlockExecutionError
  final case class MissingNodeExecptionError(blockNumber: Long, hash: Hash, storage: SimpleMap[Hash, Array[Byte]]) extends BlockExecutionError { def reason = s"Missing node $hash in ${storage.topic}, block $blockNumber" }

  final case class StateBeforeFailure(blockNumber: Long, worldState: BlockWorldState, cumGas: Long, cumReceipts: Vector[Receipt])

  trait BlockPreparationError
  final case class TxError(reason: String) extends BlockPreparationError

  /**
   * v0 ≡ Tg (Tx gas limit) * Tp (Tx gas price) + Tv (Tx value). See YP equation number (65)
   *
   * @param tx Target transaction
   * @return Upfront cost
   */
  private def calculateUpfrontCost(tx: Transaction): DataWord =
    calculateUpfrontGas(tx) + tx.value

  /**
   * v0 ≡ Tg (Tx gas limit) * Tp (Tx gas price). See YP equation number (68)
   *
   * @param tx Target transaction
   * @return Upfront cost
   */
  private def calculateUpfrontGas(tx: Transaction): DataWord =
    DataWord(tx.gasLimit) * tx.gasPrice

}
final class Ledger(blockchain: Blockchain, blockchainConfig: BlockchainConfig)(implicit system: ActorSystem) extends Ledger.I {
  import Ledger._

  private val log = Logging(system, this.getClass)

  val txProcessTimeout = 600.seconds
  val blockRewardCalculator = new BlockRewardCalculator(blockchainConfig)

  val txProcessor = system.actorOf(TxProcessor.props(this), "TxProcessor")

  /**
   * called by minning
   */
  override def prepareBlock(
    block:      Block,
    validators: Validators
  )(implicit executor: ExecutionContext): Future[BlockPreparationResult] = {
    val parentStateRoot = blockchain.getBlockHeaderByHash(block.header.parentHash).map(_.stateRoot)
    val initialWorld = blockchain.getReadOnlyWorldState(None, blockchainConfig.accountStartNonce, parentStateRoot)

    executePreparedTransactions(block.body.transactionList, initialWorld, block.header, validators.signedTransactionValidator) map {
      case (execResult @ BlockResult(resultingWorldState, _, _, _), txExecuted) =>
        val worldRewardPaid = payBlockReward(block)(resultingWorldState)
        val worldPersisted = worldRewardPaid.flush().persist()
        BlockPreparationResult(block.copy(body = block.body.copy(transactionList = txExecuted)), execResult, worldPersisted.rootHash)
    }
  }

  /**
   * This method does not actually consume the stack in the way a typical recursive
   * function would. This is because the "recursive" call will happen asynchronously,
   * on some thread from the execution context. So it is very likely that this
   * recursive call won't even reside on the same stack as the first call.
   *
   * The executeTransactions_xxxx method will create the future object which will
   * eventually trigger the "recursive" call asynchronously. After that, it is
   * immediately popped from the stack.
   *
   * So this isn't actually stack recursion and the stack doesn't grow proportional,
   * it stays roughly at a constant size.
   *
   * This can be easily checked by throwing an exception at some point in the
   * executeTransactions_xxxx method and inspecting the stack trace.
   */
  private def executePreparedTransactions(
    signedTransactions:         Seq[SignedTransaction],
    world:                      BlockWorldState,
    blockHeader:                BlockHeader,
    signedTransactionValidator: SignedTransactionValidator,
    accGas:                     Long                       = 0,
    accReceipts:                Vector[Receipt]            = Vector(),
    executed:                   Vector[SignedTransaction]  = Vector()
  )(implicit executor: ExecutionContext): Future[(BlockResult, Vector[SignedTransaction])] = {
    val evmCfg = EvmConfig.forBlock(blockHeader.number, blockchainConfig)

    executeTransactions_sequential(signedTransactions, blockHeader, signedTransactionValidator, evmCfg)(world) flatMap {
      case Right(br) => Future.successful(br, executed ++ signedTransactions)

      case Left(TxsExecutionError(blockHeader.number, stx, StateBeforeFailure(blockHeader.number, worldState, gas, receipts), reason)) =>
        log.debug(s"failure while preparing block because of $reason in transaction with hash ${stx.hash}")
        val txIndex = signedTransactions.indexWhere(tx => tx.hash == stx.hash)
        executePreparedTransactions(
          signedTransactions.drop(txIndex + 1),
          worldState, blockHeader, signedTransactionValidator, gas, receipts, executed ++ signedTransactions.take(txIndex)
        )

      case Left(error) =>
        throw new RuntimeException(s"Error during executePreparedTransactions: $error")
    }
  }

  override def simulateTransaction(stx: SignedTransaction, blockHeader: BlockHeader)(implicit executor: ExecutionContext): TxResult = {
    val start = System.nanoTime

    val gasLimit = stx.tx.gasLimit
    val evmCfg = EvmConfig.forBlock(blockHeader.number, blockchainConfig)

    val world1 = blockchain.getReadOnlyWorldState(None, blockchainConfig.accountStartNonce, Some(blockHeader.stateRoot))
    val world2 = if (world1.getAccount(stx.sender).isEmpty) {
      world1.saveAccount(stx.sender, Account.empty(blockchainConfig.accountStartNonce))
    } else {
      world1
    }

    val (checkpoint, context) = prepareProgramContext(stx, blockHeader, evmCfg)(world2)
    val result = runVM(stx, context, evmCfg)(checkpoint)

    val totalGasToRefund = calcTotalGasToRefund(gasLimit, result)
    val gasUsed = stx.tx.gasLimit - totalGasToRefund
    val txFee = DataWord(gasUsed) * stx.tx.gasPrice

    val elapsed = System.nanoTime - start

    val intermediateRootHash = result.world.rootHash

    TxResult(stx, result.world, intermediateRootHash, gasUsed, txFee, result.txLogs, result.addressesTouched, result.returnData, result.error, result.isRevert, result.parallelRaceConditions)
  }

  def validateBlocksBeforeExecution(blocks: Seq[Block], validators: Validators)(implicit executor: ExecutionContext): Future[(Vector[Block], Option[BlockExecutionError])] = {
    val start = System.nanoTime

    val blocksToValidate = blocks.map(block => block.header.hash -> block).toMap

    val fs = blocks map { block =>
      (txProcessor ? TxProcessor.PreValidateWork(block, blocksToValidate, validators))(txProcessTimeout).mapTo[Either[BlockExecutionError, Unit]] map (block -> _)
    }

    Future.sequence(fs) map { rs =>
      val itr = rs.iterator
      var errorOpt: Option[BlockExecutionError] = None
      var validBlocks = Vector[Block]()
      while (itr.hasNext && errorOpt.isEmpty) {
        itr.next() match {
          case (block, Left(error)) => errorOpt = Some(error)
          case (block, Right(_))    => validBlocks :+= block
        }
      }

      log.debug(s"pre-validated ${validBlocks.size} blocks in parallel in ${(System.nanoTime - start) / 1000000}ms ${errorOpt.fold("")(x => x.toString)}")
      (validBlocks, errorOpt)
    }
  }

  private[ledger] def validateBlockBeforeExecution(block: Block, validatingBlocks: Map[Hash, Block], validators: Validators): Either[BlockExecutionError, Unit] = {
    val result = for {
      _ <- validators.blockHeaderValidator.validate(block.header, blockchain, validatingBlocks)
      _ <- validators.blockValidator.validateHeaderAndBody(block.header, block.body)
      _ <- validators.ommersValidator.validate(block.header.number, block.body.uncleNodesList, blockchain, validatingBlocks)
    } yield ()
    result.left.map(error => ValidationBeforeExecError(block.header.number, error.toString))
  }

  /**
   * Execute and validate on minned block
   */
  override def executeBlock(block: Block, validators: Validators)(implicit executor: ExecutionContext): Future[Either[BlockExecutionError, BlockResult]] = {
    val start1 = System.nanoTime
    val parallelResult = executeBlockTransactions(block, validators.signedTransactionValidator, isParallel = true && !blockchainConfig.isDebugTraceEnabled) map {
      case Right(blockResult) =>
        log.debug(s"${block.header.number} parallel-executed in ${(System.nanoTime - start1) / 1000000}ms")

        val start2 = System.nanoTime
        val worldRewardPaid = payBlockReward(block)(blockResult.world)
        val worldFlushed = worldRewardPaid.flush() // State root hash needs to be up-to-date for validateBlockAfterExecution
        log.debug(s"${block.header.number} flushed in ${(System.nanoTime - start2) / 1000000}ms")

        val start3 = System.nanoTime
        validateBlockAfterExecution(block, worldFlushed.rootHash, blockResult.receipts, blockResult.gasUsed, validators.blockValidator) match {
          case Right(_) =>
            log.debug(s"${block.header.number} post-validated in ${(System.nanoTime - start3) / 1000000}ms")
            Right(blockResult, worldFlushed)

          case Left(error) => Left(error)
        }

      case Left(error) => Left(error)
    }

    parallelResult flatMap {
      case Right((blockResult, worldCommitted)) => Future.successful(Right(blockResult))

      case left @ Left(error) =>
        log.debug(s"in parallel failed with error $error, try sequential ...")

        val start1 = System.nanoTime
        executeBlockTransactions(block, validators.signedTransactionValidator, isParallel = false) map {
          case Right(blockResult) =>
            log.debug(s"${block.header.number} sequential-executed in ${(System.nanoTime - start1) / 1000000}ms")

            val worldRewardPaid = payBlockReward(block)(blockResult.world)
            val worldFlushed = worldRewardPaid.flush() // State root hash needs to be up-to-date for validateBlockAfterExecution

            validateBlockAfterExecution(block, worldFlushed.rootHash, blockResult.receipts, blockResult.gasUsed, validators.blockValidator) match {
              case Right(_)    => Right(blockResult)
              case Left(error) => Left(error)
            }

          case Left(error) => Left(error)
        }
    }
  }

  /**
   * This function runs transaction
   *
   * @param block
   * @param blockchain
   * @param signedTransactionValidator
   */
  private def executeBlockTransactions(
    block:        Block,
    stxValidator: SignedTransactionValidator,
    isParallel:   Boolean
  )(implicit executor: ExecutionContext): Future[Either[BlockExecutionError, BlockResult]] = {
    val parentStateRoot = blockchain.getBlockHeaderByHash(block.header.parentHash).map(_.stateRoot)
    val evmCfg = EvmConfig.forBlock(block.header.number, blockchainConfig)

    def initialWorld = blockchain.getWorldState(block.header.number, blockchainConfig.accountStartNonce, parentStateRoot)

    if (isParallel) {
      executeTransactions_inparallel(block.body.transactionList, block.header, stxValidator, evmCfg)(initialWorld)
    } else {
      executeTransactions_sequential(block.body.transactionList, block.header, stxValidator, evmCfg)(initialWorld)
    }
  }

  /**
   * This functions executes all the signed transactions from a block (till one of those executions fails)
   *
   * @param signedTransactions from the block that are left to execute
   * @param blockHeader of the block we are currently executing
   * @param stxValidator used to validate the signed transactions
   * @param evmCfg evm config
   * @param world that will be updated by the execution of the signedTransactions
   * @return a BlockResult if the execution of all the transactions in the block was successful or a BlockExecutionError
   *         if one of them failed
   */
  private def executeTransactions_sequential(
    signedTransactions: Seq[SignedTransaction],
    blockHeader:        BlockHeader,
    stxValidator:       SignedTransactionValidator,
    evmCfg:             EvmConfig
  )(initialWorld: BlockWorldState): Future[Either[BlockExecutionError, BlockResult]] = {
    var currWorld = initialWorld
    var txError: Option[BlockExecutionError] = None
    var txResults = Vector[TxResult]()

    val itr = signedTransactions.iterator
    while (itr.hasNext && txError.isEmpty) {
      val stx = itr.next()
      validateAndExecuteTransaction(stx, blockHeader, stxValidator, evmCfg)(currWorld.withTx(Some(stx))) match {
        case Right(txResult) =>
          currWorld = txResult.postWorld
          txResults = txResults :+ txResult
        case Left(error) =>
          txError = Some(error)
      }
    }

    txError match {
      case Some(error) => Future.successful(Left(error))
      case None        => Future.successful(postExecuteTransactions(blockHeader, evmCfg, txResults, Stats(0, 0, Nil, 0))(currWorld.withTx(None)))
    }
  }

  private def executeTransactions_inparallel(
    signedTransactions: Seq[SignedTransaction],
    blockHeader:        BlockHeader,
    stxValidator:       SignedTransactionValidator,
    evmCfg:             EvmConfig
  )(initialWorldFun: => BlockWorldState)(implicit executor: ExecutionContext): Future[Either[BlockExecutionError, BlockResult]] = {
    val nTx = signedTransactions.size

    val start = System.nanoTime
    blockchain.storages.accountNodeDataSource.clock.start()
    blockchain.storages.storageNodeDataSource.clock.start()
    blockchain.storages.evmcodeDataSource.clock.start()
    blockchain.storages.blockHeaderDataSource.clock.start()
    blockchain.storages.blockBodyDataSource.clock.start()
    blockchain.storages.accountNodeDataSource.resetCacheHitRate()
    blockchain.storages.storageNodeDataSource.resetCacheHitRate()

    val fs = signedTransactions.map(stx => stx -> initialWorldFun.withTx(Some(stx))) map {
      case (stx, initialWorld) =>
        (txProcessor ? TxProcessor.ExecuteWork(initialWorld, stx, blockHeader, stxValidator, evmCfg))(txProcessTimeout).mapTo[(Either[BlockExecutionError, TxResult], Long)] // recover { case ex => s"$ex.getMessage" }
    }

    Future.sequence(fs) map { rs =>
      val dsGetElapsed1 = blockchain.storages.accountNodeDataSource.clock.elasped + blockchain.storages.storageNodeDataSource.clock.elasped +
        blockchain.storages.evmcodeDataSource.clock.elasped + blockchain.storages.blockHeaderDataSource.clock.elasped + blockchain.storages.blockBodyDataSource.clock.elasped

      blockchain.storages.accountNodeDataSource.clock.start()
      blockchain.storages.storageNodeDataSource.clock.start()
      blockchain.storages.evmcodeDataSource.clock.start()
      blockchain.storages.blockHeaderDataSource.clock.start()
      blockchain.storages.blockBodyDataSource.clock.start()

      val (results, elapses) = rs.unzip
      val elapsed = elapses.sum
      log.debug(s"${blockHeader.number} executed parallel in ${(System.nanoTime - start) / 1000000}ms, db get ${100.0 * dsGetElapsed1 / elapsed}%")

      var currWorld: Option[BlockWorldState] = None
      var txError: Option[BlockExecutionError] = None
      var txResults = Vector[TxResult]()
      var parallelCount = 0

      // re-execute tx under prevWorld, commit prevWorld to get all nodes exist, see BlockWorldState.getStorage and getStateRoott
      var reExecutedElapsed = 0L
      def reExecute(stx: SignedTransaction, prevWorld: BlockWorldState) = {
        var start = System.nanoTime
        log.debug(s"${stx.hash} re-executing")
        // should commit prevWorld's state, since we may need to get newest account/storage/code by new state's hash
        validateAndExecuteTransaction(stx, blockHeader, stxValidator, evmCfg)(prevWorld.flush().withTx(Some(stx))) match {
          case Left(error) => txError = Some(error)
          case Right(newTxResult) =>
            currWorld = Some(newTxResult.postWorld)
            txResults = txResults :+ newTxResult
        }
        reExecutedElapsed += System.nanoTime - start
      }

      val itr = results.iterator
      while (itr.hasNext && txError.isEmpty) {
        val r = itr.next()
        r match {
          case Right(txResult) =>
            currWorld match {
              case None => // first tx
                parallelCount += 1
                currWorld = Some(txResult.postWorld)
                txResults = txResults :+ txResult

              case Some(prevWorld) =>
                if (txResult.parallelRaceConditions.nonEmpty) {
                  log.debug(s"tx ${txResult.stx.hash} potential parallel race conditions ${txResult.parallelRaceConditions} occurred during executing")
                  // when potential parallel race conditions occurred during executing, it's difficult to judge if it was caused by conflict, so, just re-execute
                  reExecute(txResult.stx, prevWorld)
                } else {
                  prevWorld.merge(txResult.postWorld) match {
                    case Left(raceCondiftions) =>
                      log.debug(s"tx ${txResult.stx.hash} has race conditions with prev world state:\n$raceCondiftions")
                      reExecute(txResult.stx, prevWorld)

                    case Right(mergedWorld) =>
                      parallelCount += 1
                      currWorld = Some(mergedWorld)
                      txResults = txResults :+ txResult
                  }
                }
            }

          case Left(error @ TxsExecutionError(_, stx, _, SignedTransactionError.TransactionSenderCantPayUpfrontCostError(_, _))) =>
            currWorld match {
              case None => txError = Some(error) // first tx
              case Some(prevWorld) =>
                reExecute(stx, prevWorld)
            }

          case Left(error) => txError = Some(error)
        }

        //log.debug(s"${blockHeader.number} touched accounts (${r.fold(_.stx, _.stx).hash}):\n ${currWorld.map(_.touchedAccounts.mkString("\n", "\n", "\n")).getOrElse("")}")
      }

      val dsGetElapsed2 = blockchain.storages.accountNodeDataSource.clock.elasped + blockchain.storages.storageNodeDataSource.clock.elasped +
        blockchain.storages.evmcodeDataSource.clock.elasped + blockchain.storages.blockHeaderDataSource.clock.elasped + blockchain.storages.blockBodyDataSource.clock.elasped

      val totalElasped = elapsed + reExecutedElapsed
      val dbReadTimePerc = if (totalElasped != 0) {
        (dsGetElapsed1 + dsGetElapsed2).toDouble / totalElasped
      } else {
        0.0
      }

      val parallelRate = if (nTx > 0) parallelCount.toDouble / nTx else 1.0
      val cacheHitRates = List(blockchain.storages.accountNodeDataSource.cacheHitRate, blockchain.storages.storageNodeDataSource.cacheHitRate).map(_ * 100.0)
      val cacheReadCount = blockchain.storages.accountNodeDataSource.cacheReadCount + blockchain.storages.storageNodeDataSource.cacheReadCount

      log.debug(s"${blockHeader.number} re-executed in ${reExecutedElapsed}ms, ${100 * (1 - parallelRate)}% with race conditions, db get ${100.0 * dsGetElapsed2 / reExecutedElapsed}%")
      log.debug(s"${blockHeader.number} touched accounts:\n ${currWorld.map(_.touchedAccounts.mkString("\n", "\n", "\n")).getOrElse("")}")

      txError match {
        case Some(error) => Left(error)
        case None        => postExecuteTransactions(blockHeader, evmCfg, txResults, Stats(parallelRate, dbReadTimePerc, cacheHitRates, cacheReadCount))(currWorld.map(_.withTx(None)).getOrElse(initialWorldFun))
      }
    } andThen {
      case Success(_) =>
      case Failure(e) => log.error(e, s"Error on block ${blockHeader.number}: ${e.getMessage}")
    }
  }

  private def postExecuteTransactions(
    blockHeader: BlockHeader,
    evmCfg:      EvmConfig,
    txResults:   Vector[TxResult],
    stats:       Stats
  )(world: BlockWorldState): Either[BlockExecutionError, BlockResult] = {
    try {
      val (accGas, accTxFee, accTouchedAddresses, accReceipts) = txResults.foldLeft(0L, DataWord.Zero, Set[Address](), Vector[Receipt]()) {
        case ((accGas, accTxFee, accTouchedAddresses, accReceipts), TxResult(stx, postWorld, intermediateRootHash, gasUsed, txFee, logs, touchedAddresses, _, error, isRevert, _)) =>

          val postTxState = if (evmCfg.eip658) {
            if (error.isDefined || isRevert) Receipt.Failure else Receipt.Success
          } else {
            intermediateRootHash
          }

          log.debug(s"Tx ${stx.hash} gasLimit: ${stx.tx.gasLimit}, gasUsed: $gasUsed, cumGasUsed: ${accGas + gasUsed}")

          val receipt = Receipt(
            postTxState = postTxState,
            cumulativeGasUsed = accGas + gasUsed,
            logsBloomFilter = BloomFilter.create(logs),
            logs = logs
          )

          if (blockchainConfig.isDebugTraceEnabled) {
            println(s"\nTx ${stx.hash} ${receipt}")
          }

          (accGas + gasUsed, accTxFee + txFee, accTouchedAddresses ++ touchedAddresses, accReceipts :+ receipt)
      }

      val minerAddress = Address(blockHeader.beneficiary)
      val worldPayMinerForGas = world.pay(minerAddress, accTxFee)

      // find empty touched accounts to be deleted
      val deadAccounts = if (evmCfg.eip161) {
        (accTouchedAddresses + minerAddress) filter (worldPayMinerForGas.isAccountDead)
      } else {
        Set[Address]()
      }
      //log.debug(s"touched accounts: ${result.addressesTouched}, miner: $minerAddress")
      log.debug(s"dead accounts accounts: $deadAccounts")
      val worldDeletedDeadAccounts = deleteAccounts(deadAccounts)(worldPayMinerForGas)

      log.debug(s"$blockHeader, accGas $accGas, receipts = $accReceipts")
      Right(BlockResult(worldDeletedDeadAccounts, accGas, accReceipts, stats))
    } catch {
      case MPTNodeMissingException(_, hash, storage) => Left(MissingNodeExecptionError(blockHeader.number, hash, storage))
      case e: Throwable                              => throw e
    }
  }

  // TODO see TODO at lines
  private[ledger] def validateAndExecuteTransaction(
    stx:          SignedTransaction,
    blockHeader:  BlockHeader,
    stxValidator: SignedTransactionValidator,
    evmCfg:       EvmConfig
  )(world: BlockWorldState): Either[BlockExecutionError, TxResult] = {
    try {
      // TODO put OnAccount(senderAccount) as race condition? It should to be added during executeTransaction  
      val (senderAccount, worldForTx) = world.getAccount(stx.sender) match {
        case Some(account) => (account, world)
        case None =>
          val emptyAccount = world.emptyAccount
          (emptyAccount, world.saveAccount(stx.sender, emptyAccount))
      }

      val upfrontCost = calculateUpfrontCost(stx.tx)

      stxValidator.validate(stx, senderAccount, blockHeader, upfrontCost, accumGasUsed = 0L) match { // TODO validate accumGasUsed lazily for asyn execution
        case Right(_) | Left(SignedTransactionError.TransactionNonceError(_, _)) => // TODO validate TransactionNonceError lazily for async execution
          Right(executeTransaction(stx, blockHeader, evmCfg)(worldForTx))

        case Left(error) =>
          Left(TxsExecutionError(blockHeader.number, stx, StateBeforeFailure(blockHeader.number, world, 0L, Vector()), error)) // TODO content of StateBeforeFailure
      }
    } catch {
      case MPTNodeMissingException(_, hash, storage) => Left(MissingNodeExecptionError(blockHeader.number, hash, storage))
      case e: Throwable                              => throw e
    }
  }

  private def executeTransaction(
    stx:         SignedTransaction,
    blockHeader: BlockHeader,
    evmCfg:      EvmConfig
  )(world: BlockWorldState): TxResult = {
    val start = System.nanoTime

    // TODO catch prepareProgramContext's throwable (MPTException etc from mtp) here
    val (checkpoint, context) = prepareProgramContext(stx, blockHeader, evmCfg)(world)

    if (blockchainConfig.isDebugTraceEnabled) {
      println(s"\nTx 0x${stx.hash} ========>\n${stx.tx}")
    }
    val result = runVM(stx, context, evmCfg)(checkpoint)

    val gasLimit = stx.tx.gasLimit
    val totalGasToRefund = calcTotalGasToRefund(gasLimit, result)
    val gasUsed = stx.tx.gasLimit - totalGasToRefund
    val gasPrice = stx.tx.gasPrice
    val txFee = gasPrice * gasUsed
    val refund = gasPrice * totalGasToRefund

    if (blockchainConfig.isDebugTraceEnabled) {
      println(s"\nTx 0x${stx.hash} gasLimit: ${stx.tx.gasLimit} gasUsed $gasUsed, isRevert: ${result.isRevert}, error: ${result.error}")
    }

    val worldRefundGasPaid = result.world.pay(stx.sender, refund)
    val worldDeletedAccounts = deleteAccounts(result.addressesToDelete)(worldRefundGasPaid)

    val intermediateRootHash = worldRefundGasPaid.rootHash

    //log.debug(
    //  s"""Transaction 0x${stx.hashAsHexString} execution end. Summary:
    //     | - Value: ${stx.tx.value}
    //     | - Error: ${result.error}.
    //     | - Total Gas to Refund: $totalGasToRefund
    //     | - Execution gas paid to miner: $txFee""".stripMargin
    //)

    val elapsed = System.nanoTime - start
    TxResult(stx, worldDeletedAccounts, intermediateRootHash, gasUsed, txFee, result.txLogs, result.addressesTouched, result.returnData, result.error, result.isRevert, result.parallelRaceConditions)
  }

  /**
   * This function validates that the various results from execution are consistent with the block. This includes:
   *   - Validating the resulting stateRootHash
   *   - Doing BlockValidator.validateBlockReceipts validations involving the receipts
   *   - Validating the resulting gas used
   *
   * @param block to validate
   * @param stateRootHash from the resulting state trie after executing the txs from the block
   * @param receipts associated with the execution of each of the tx from the block
   * @param gasUsed, accumulated gas used for the execution of the txs from the block
   * @param blockValidator used to validate the receipts with the block
   * @return None if valid else a message with what went wrong
   */
  private def validateBlockAfterExecution(
    block:          Block,
    stateRootHash:  Hash,
    receipts:       Seq[Receipt],
    gasUsed:        Long,
    blockValidator: BlockValidator
  ): Either[BlockExecutionError, Unit] = {
    if (block.header.gasUsed != gasUsed) {
      Left(ValidationAfterExecError(block.header.number, s"Block ${block.header.number} has invalid gas used, expected ${block.header.gasUsed} but got $gasUsed"))
    } else if (block.header.stateRoot != stateRootHash) {
      Left(ValidationAfterExecError(block.header.number, s"Block ${block.header.number} has invalid state root hash, expected ${block.header.stateRoot.hexString} but got ${stateRootHash.hexString}"))
    } else {
      blockValidator.validateBlockAndReceipts(block, receipts) match {
        case Left(error) => Left(ValidationAfterExecError(block.header.number, error.toString))
        case right       => Right(())
      }
    }
  }

  /**
   * This function updates state in order to pay rewards based on YP section 11.3
   *
   * @param block
   * @param world
   * @return
   */
  private def payBlockReward(block: Block)(world: BlockWorldState): BlockWorldState = {
    val minerAddress = Address(block.header.beneficiary)
    val minerAccount = getAccountToPay(minerAddress)(world)
    val minerReward = blockRewardCalculator.calcBlockMinerReward(block.header.number, block.body.uncleNodesList.size)
    val afterMinerReward = world.saveAccount(minerAddress, minerAccount.increaseBalance(minerReward))
    log.debug(s"Paying block ${block.header.number} reward of $minerReward to miner with account address $minerAddress")

    block.body.uncleNodesList.foldLeft(afterMinerReward) { (ws, ommer) =>
      val ommerAddress = Address(ommer.beneficiary)
      val account = getAccountToPay(ommerAddress)(ws)
      val ommerReward = blockRewardCalculator.calcOmmerMinerReward(block.header.number, ommer.number)
      log.debug(s"Paying block ${block.header.number} reward of $ommerReward to ommer with account address $ommerAddress")
      ws.saveAccount(ommerAddress, account.increaseBalance(ommerReward))
    }
  }

  private def getAccountToPay(address: Address)(world: BlockWorldState): Account = {
    world.getAccount(address).getOrElse(world.emptyAccount)
  }

  /**
   * Increments account nonce by 1 stated in YP equation (69) and
   * Pays the upfront Tx gas calculated as TxGasPrice * TxGasLimit from balance. YP equation (68)
   * remember the checkpoint world state
   * prepareProgtamContext
   * Note we use one fewer than the sender’s nonce
   * value; we assert that we have incremented the sender account’s
   * nonce prior to this call, and so the value used
   * is the sender’s nonce at the beginning of the responsible
   * transaction or VM operation
   */
  private def prepareProgramContext(stx: SignedTransaction, blockHeader: BlockHeader, evmCfg: EvmConfig)(world: BlockWorldState): (BlockWorldState, PC) = {
    val senderAddress = stx.sender
    val account = world.getGuaranteedAccount(senderAddress)
    val (checkpoint, worldAtCheckpoint) = {
      val worldx = world.increaseNonce(senderAddress).withdraw(senderAddress, calculateUpfrontGas(stx.tx))
      (worldx.copy, worldx)
    }

    // YP eq (91)
    val (worldBeforeTransfer, recipientAddress, program, input) =
      if (stx.tx.isContractCreation) {
        val newContractAddress = worldAtCheckpoint.createAddress(senderAddress)
        val world = if (evmCfg.eip161) {
          worldAtCheckpoint.increaseNonce(newContractAddress)
        } else {
          worldAtCheckpoint
        }
        log.debug(s"newContractAddress: $newContractAddress")
        (world, newContractAddress, Program(stx.tx.payload.toArray), ByteString())
      } else {
        val txReceivingAddress = stx.tx.receivingAddress.get
        log.debug(s"txReceivingAddress: $txReceivingAddress")
        (worldAtCheckpoint, txReceivingAddress, Program(world.getCode(txReceivingAddress).toArray), stx.tx.payload)
      }

    val worldAfterTransfer = worldBeforeTransfer.transfer(senderAddress, recipientAddress, stx.tx.value)
    val initialAddressesToDelete = Set[Address]()
    val initialAddressesTouched = Set(recipientAddress)

    val context: PC = ProgramContext(
      stx,
      recipientAddress,
      program,
      input,
      blockHeader,
      worldAfterTransfer,
      evmCfg,
      initialAddressesToDelete,
      initialAddressesTouched,
      isStaticCall = false
    )

    (checkpoint, context)
  }

  /**
   * @param checkpoint - world will return checkpoint if result error or isRevert
   */
  private def runVM(stx: SignedTransaction, context: PC, evmCfg: EvmConfig)(checkpoint: BlockWorldState): PR = {
    val r = if (stx.tx.isContractCreation) { // create
      VM.run(context, blockchainConfig.isDebugTraceEnabled)
    } else { // call
      PrecompiledContracts.getContractForAddress(context.targetAddress, evmCfg) match {
        case Some(contract) =>
          contract.run(context)
        case None =>
          VM.run(context, blockchainConfig.isDebugTraceEnabled)
      }
    }

    val result = if (stx.tx.isContractCreation && !r.error.isDefined && !r.isRevert) {
      saveCreatedContract(context.env.ownerAddr, r, evmCfg)
    } else {
      r
    }

    if (result.error.isDefined || result.isRevert) {
      // rollback to the world before transfer was done if an error happened
      // the error result may be caused by parallel conflict, so merge all possible modifies
      result.copy(world = checkpoint.mergeRaceConditions(result.world), addressesToDelete = Set(), addressesTouched = Set(), txLogs = Vector(), parallelRaceConditions = Set(ProgramState.OnError))
    } else {
      result
    }
  }

  private def saveCreatedContract(address: Address, result: PR, evmCfg: EvmConfig): PR = {
    val codeDepositCost = evmCfg.calcCodeDepositCost(result.returnData)

    if (result.gasRemaining < codeDepositCost) {
      if (evmCfg.exceptionalFailedCodeDeposit) {
        // TODO set returnData to empty bytes ByteString()?
        result.copy(error = Some(OutOfGas))
      } else {
        result
      }
    } else if (result.returnData.length > evmCfg.maxContractSize) {
      // contract size too large
      log.warning(s"Contract size too large: ${result.returnData.length}")
      // TODO set returnData to empty bytes ByteString()?
      result.copy(error = Some(OutOfGas))
    } else {
      // even result.isRevert? this is a different behavior from CREATE opcode? 
      result.copy(
        gasRemaining = result.gasRemaining - codeDepositCost,
        world = result.world.saveCode(address, result.returnData)
      )
    }
  }

  /**
   * Calculate total gas to be refunded
   * See YP, eq (72)
   */
  private def calcTotalGasToRefund(gasLimit: Long, result: PR): Long = {
    if (result.error.isDefined) {
      0
    } else {
      if (result.isRevert) {
        result.gasRemaining
      } else {
        val gasUsed = gasLimit - result.gasRemaining
        result.gasRemaining + math.min(gasUsed / 2, result.gasRefund)
      }
    }
  }

  /**
   * Delete all accounts (that appear in SUICIDE list). YP eq (78).
   * The contract storage should be cleared during pruning as nodes could be used in other tries.
   * The contract code is also not deleted as there can be contracts with the exact same code, making it risky to delete
   * the code of an account in case it is shared with another one.
   * FIXME: [EC-242]
   *   Should we delete the storage associated with the deleted accounts?
   *   Should we keep track of duplicated contracts for deletion?
   *
   * @param addressesToDelete
   * @param worldState
   * @return a worldState equal worldState except that the accounts from addressesToDelete are deleted
   */
  private def deleteAccounts(addressesToDelete: Set[Address])(worldState: BlockWorldState): BlockWorldState = {
    addressesToDelete.foldLeft(worldState) { case (world, address) => world.deleteAccount(address) }
  }
}

