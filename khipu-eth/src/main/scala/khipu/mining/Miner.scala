package khipu.mining

import akka.actor.{ Actor, ActorLogging, Props }
import akka.pattern.ask
import akka.util.{ ByteString, Timeout }
import java.math.BigInteger
import khipu.DataWord
import khipu.Hash
import khipu.blockchain.sync.SyncService
import khipu.config.MiningConfig
import khipu.consensus.pow.Ethash
import khipu.consensus.pow.Ethash.MiningSuccessful
import khipu.consensus.pow.EthashParams
import khipu.crypto
import khipu.domain.Blockchain
import khipu.jsonrpc.EthService
import khipu.jsonrpc.EthService.SubmitHashRateRequest
import khipu.ommers.OmmersPool
import khipu.service.ServiceBoard
import khipu.transactions.PendingTransactionsService
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object Miner {
  def props(
    blockchain:     Blockchain,
    blockGenerator: BlockGenerator,
    miningConfig:   MiningConfig,
    ethService:     Option[EthService]
  ): Props = Props(new Miner(blockchain, blockGenerator, miningConfig, ethService))

  case object StartMining
  case object StopMining

  private case object ProcessMining

  val MaxNonce = BigInteger.valueOf(2).pow(64).add(BigInteger.ONE.negate)
}
class Miner(
    blockchain:     Blockchain,
    blockGenerator: BlockGenerator,
    miningConfig:   MiningConfig,
    ethService:     Option[EthService]
) extends Actor with ActorLogging {
  import Miner._
  import context.dispatcher

  var currentEpoch: Option[Long] = None
  var currentEpochDagSize: Option[Long] = None
  var currentEpochDag: Option[Array[Array[Int]]] = None

  private val ethashParams = new EthashParams()

  private val serviceBoard = ServiceBoard(context.system)
  private val ethashAlgo = serviceBoard.etashAlgo
  private def ommersPool = serviceBoard.ommersPool
  private def pendingTransactionsService = PendingTransactionsService.proxy(context.system)
  private def syncService = SyncService.proxy(context.system)

  override def receive: Receive = stopped

  def stopped: Receive = {
    case StartMining =>
      context become started
      self ! ProcessMining
    case ProcessMining => // nothing
  }

  def started: Receive = {
    case StopMining    => context become stopped
    case ProcessMining => processMining()
  }

  def processMining(): Unit = {
    val blockNumber = blockchain.storages.bestBlockNumber + 1
    val ethash = Ethash.getForBlock(miningConfig, ethashAlgo, blockNumber)

    getBlockForMining(blockNumber) onComplete {
      case Success(PendingBlock(block, _)) =>
        val headerHash = crypto.kec256(block.header.getEncodedWithoutNonce)
        val startTime = System.currentTimeMillis
        ethash.mine(block)
        val mineResult = ethash.mine(block)
        val time = System.currentTimeMillis - startTime
        val hashRate = (mineResult.nTriedHashes * 1000) / time
        ethService foreach { _.submitHashRate(SubmitHashRateRequest(DataWord(hashRate), ByteString("khipu-miner"))) }
        mineResult match {
          case MiningSuccessful(_, pow, nonce) =>
            syncService ! SyncService.MinedBlock(block.copy(header = block.header.copy(nonce = ByteString(nonce), mixHash = Hash(pow.mixHash))))
          case _ => // nothing
        }
        self ! ProcessMining

      case Failure(ex) =>
        log.error(ex, "Unable to get block for mining")
        context.system.scheduler.scheduleOnce(10.seconds, self, ProcessMining)
    }
  }

  private def toUnsignedByteArrayNonce(nouce: BigInteger) = {
    val asByteArray = nouce.toByteArray
    if (asByteArray.head == 0) {
      asByteArray.tail
    } else {
      asByteArray
    }
  }

  private def getBlockForMining(blockNumber: Long): Future[PendingBlock] = {
    getOmmersFromPool(blockNumber).zip(getTransactionsFromPool).flatMap {
      case (ommers, pendingTxs) =>
        blockGenerator.generateBlockForMining(blockNumber, pendingTxs.pendingTransactions.map(_.stx), ommers.headers, miningConfig.coinbase) match {
          case Right(pb) => Future.successful(pb)
          case Left(err) => Future.failed(new RuntimeException(s"Error while generating block for mining: $err"))
        }
    }
  }

  private def getOmmersFromPool(blockNumber: Long) = {
    implicit val timeout = Timeout(miningConfig.ommerPoolQueryTimeout)

    (ommersPool ? OmmersPool.GetOmmers(blockNumber)).mapTo[OmmersPool.Ommers]
      .recover {
        case ex =>
          log.error(ex, "Failed to get ommers, mining block with empty ommers list")
          OmmersPool.Ommers(Nil)
      }
  }

  private def getTransactionsFromPool = {
    implicit val timeout = Timeout(miningConfig.ommerPoolQueryTimeout)

    (pendingTransactionsService ? PendingTransactionsService.GetPendingTransactions).mapTo[PendingTransactionsService.PendingTransactionsResponse]
      .recover {
        case ex =>
          log.error(ex, "Failed to get transactions, mining block with empty transactions list")
          PendingTransactionsService.PendingTransactionsResponse(Nil)
      }
  }
}

