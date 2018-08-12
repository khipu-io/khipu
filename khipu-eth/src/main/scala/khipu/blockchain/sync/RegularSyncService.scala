package khipu.blockchain.sync

import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.pattern.AskTimeoutException
import akka.pattern.ask
import java.math.BigInteger
import java.util.concurrent.ThreadLocalRandom
import khipu.BroadcastNewBlocks
import khipu.blockchain.sync
import khipu.blockchain.sync.HandshakedPeersService.BlacklistPeer
import khipu.domain.{ Block, BlockHeader }
import khipu.ledger.Ledger.BlockExecutionError
import khipu.ledger.Ledger.BlockResult
import khipu.ledger.Ledger.MissingNodeExecptionError
import khipu.ledger.Ledger.ValidationBeforeExecError
import khipu.network.handshake.EtcHandshake.PeerInfo
import khipu.network.p2p.messages.CommonMessages.NewBlock
import khipu.network.p2p.messages.PV62
import khipu.network.rlpx.Peer
import khipu.network.rlpx.PeerEntity
import khipu.store.datasource.KesqueDataSource
import khipu.transactions.PendingTransactionsService
import khipu.ommers.OmmersPool
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Failure

object RegularSyncService {
  private case object ResumeRegularSyncTask
  private case object ResumeRegularSyncTick

  private case class ProcessBlockHeaders(peer: Peer, headers: Seq[BlockHeader])
  private case class ProcessBlockBodies(peer: Peer, bodies: Seq[PV62.BlockBody])
}
trait RegularSyncService { _: SyncService =>
  import context.dispatcher
  import RegularSyncService._
  import khipu.util.Config.Sync._

  private def tf(n: Int) = "%1$4d".format(n) // tx
  private def xf(n: Double) = "%1$5.1f".format(n) // tps
  private def pf(n: Double) = "%1$5.2f".format(n) // percent
  private def ef(n: Double) = "%1$6.3f".format(n) // elapse time
  private def gf(n: Double) = "%1$6.3f".format(n) // gas
  private def lf(n: Int) = "%1$5d".format(n) // payload

  // Should keep newer block to be at the front
  private var workingHeaders: Seq[BlockHeader] = Vector[BlockHeader]()

  def startRegularSync() {
    log.info("Starting regular block synchronization")
    appStateStorage.fastSyncDone()
    context become (handleRegularSync orElse peerUpdateBehavior orElse ommersBehavior orElse stopBehavior)
    requestHeaders()
  }

  def handleRegularSync: Receive = {
    case ResumeRegularSyncTick =>
      workingHeaders = Vector()
      requestHeaders()

    case ProcessBlockHeaders(peer, headers) =>
      if (workingHeaders.isEmpty) {
        if (headers.nonEmpty) {
          workingHeaders = headers
          processBlockHeaders(peer, headers)
        } else {
          // no new headers to process, schedule to ask again in future, we are at the top of chain
          scheduleResume()
        }
      } else {
        // TODO limit max branch depth? [EC-248]
        if (headers.nonEmpty && headers.last.hash == workingHeaders.head.parentHash) {
          // should insert before pendingHeaders
          workingHeaders = headers ++ workingHeaders
          processBlockHeaders(peer, workingHeaders)
        } else {
          blockPeerAndResumeWithAnotherOne(peer, "Did not get previous blocks, there is no way to resolve, blacklist peer and continue download")
        }
      }

    case ProcessBlockBodies(peer, blockBodies) =>
      processBlockBodies(peer, blockBodies)

    case SyncService.ReceivedMessage(peerId, message) =>
      log.debug(s"Received ${message.getClass.getName} from $peerId")

    //todo improve mined block handling - add info that block was not included because of syncing [EC-250]
    //we allow inclusion of mined block only if we are not syncing / reorganising chain
    case SyncService.MinedBlock(block) =>
      if (workingHeaders.isEmpty && !isRequesting) {
        //we are at the top of chain we can insert new block
        blockchain.getBlockHeaderByHash(block.header.parentHash).flatMap { b =>
          blockchain.getTotalDifficultyByHash(b.hash)
        } match {
          case Some(parentTd) if block.header.number > appStateStorage.getBestBlockNumber =>
            // just insert block and let resolve it with regular download
            executeAndInsertBlock(block, parentTd, isBatch = false) match {
              case Right(newBlock) =>
                // broadcast new block
                handshakedPeers.foreach {
                  case (peerId, (peer, peerInfo)) => peer.entity ! PeerEntity.MessageToPeer(peerId, newBlock)
                }
              case Left(error) =>
            }
          case _ =>
            log.error("Failed to add mined block")
        }
      } else {
        ommersPool ! OmmersPool.AddOmmers(List(block.header))
      }

    case SyncService.ReportStatusTick =>
      log.debug(s"Block: ${appStateStorage.getBestBlockNumber()}. Peers(in/out): ${handshakedPeers.size}(${incomingPeers.size}/${outgoingPeers.size}) black: ${blacklistPeers.size}")
  }

  private var lookbackFromBlock: Option[Long] = if (reimportFromBlockNumber > 0) Some(reimportFromBlockNumber) else None // for debugging/reimporting a specified block
  private var isLookbacked = false
  private def requestHeaders() = {
    bestPeer match {
      case Some(peer) =>
        val nextBlockNumber = lookbackFromBlock match {
          case None => appStateStorage.getBestBlockNumber + 1
          case Some(reimportFromBlockNumber) if isLookbacked => appStateStorage.getBestBlockNumber + 1
          case Some(reimportFromBlockNumber) => reimportFromBlockNumber
        }

        log.debug(s"Request block headers beginning at $nextBlockNumber via best peer $peer")

        requestingHeaders(peer, None, Left(nextBlockNumber), blockHeadersPerRequest, skip = 0, reverse = false)(syncRequestTimeout) map {
          case Some(sync.BlockHeadersResponse(peerId, headers, true)) =>
            log.debug(s"Got block headers from $peer")
            if (lookbackFromBlock.isDefined) {
              isLookbacked = true
            }
            lookbackFromBlock = None
            self ! ProcessBlockHeaders(peer, headers)

          case Some(sync.BlockHeadersResponse(peerId, _, false)) =>
            blockPeerAndResumeWithAnotherOne(peer, s"Got error in block headers response for requested: $nextBlockNumber")

          case None =>
            scheduleResume()

        } andThen {
          case Failure(e: AskTimeoutException) => blockPeerAndResumeWithAnotherOne(peer, s"${e.getMessage}")
          case Failure(e)                      => blockPeerAndResumeWithAnotherOne(peer, s"${e.getMessage}")
          case _                               =>
        }
      case None =>
        log.debug("No peers to download from")
        scheduleResume()
    }
  }

  private def processBlockHeaders(peer: Peer, headers: Seq[BlockHeader]) {
    if (checkHeaders(headers)) {
      blockchain.getBlockHeaderByNumber(headers.head.number - 1) match {
        case Some(parent) =>
          if (parent.hash == headers.head.parentHash) {
            // we have same chain prefix
            val oldBranch = getPrevBlocks(headers)
            val oldBranchTotalDifficulty = oldBranch.map(_.header.difficulty).foldLeft(BigInteger.ZERO)(_ add _)

            val newBranchTotalDifficulty = headers.map(_.difficulty).foldLeft(BigInteger.ZERO)(_ add _)

            if (newBranchTotalDifficulty.compareTo(oldBranchTotalDifficulty) > 0) { // TODO what about == 0 ?
              val transactionsToAdd = oldBranch.flatMap(_.body.transactionList)
              pendingTransactionsService ! PendingTransactionsService.AddTransactions(transactionsToAdd.toList)
              val hashes = headers.take(blockBodiesPerRequest).map(_.hash)

              log.debug(s"Request block bodies from $peer")

              requestingBodies(peer, hashes)(syncRequestTimeout.plus((hashes.size * 100).millis)) map {
                case Some(sync.BlockBodiesResponse(peerId, bodies)) =>
                  log.debug(s"Got block bodies from $peer")
                  self ! ProcessBlockBodies(peer, bodies)

                case None =>
                  blockPeerAndResumeWithAnotherOne(peer, s"Got empty block bodies response for known hashes: ${hashes.map(_.hexString)}")

              } andThen {
                case Failure(e: AskTimeoutException) => blockPeerAndResumeWithAnotherOne(peer, s"${e.getMessage}")
                case Failure(e)                      => blockPeerAndResumeWithAnotherOne(peer, s"${e.getMessage}")
                case _                               =>
              }

              // add first block from branch as ommer
              oldBranch.headOption.foreach { block => ommersPool ! OmmersPool.AddOmmers(List(block.header)) }
            } else {
              // add first block from branch as ommer
              headers.headOption.foreach { header => ommersPool ! OmmersPool.AddOmmers(List(header)) }
              scheduleResume()
            }

          } else {
            log.info(s"[sync] Received branch block ${headers.head.number} from ${peer.id}, resolving fork ...")

            requestingHeaders(peer, None, Right(headers.head.parentHash), blockResolveDepth, skip = 0, reverse = true)(syncRequestTimeout) map {
              case Some(sync.BlockHeadersResponse(peerId, headers, true)) =>
                self ! ProcessBlockHeaders(peer, headers)

              case Some(sync.BlockHeadersResponse(peerId, headers, false)) =>
                blockPeerAndResumeWithAnotherOne(peer, s"Got error in block headers response for requested: ${headers.head.parentHash}")

              case None =>
                scheduleResume()

            } andThen {
              case Failure(e: AskTimeoutException) => blockPeerAndResumeWithAnotherOne(peer, s"timeout, ${e.getMessage}")
              case Failure(e)                      => blockPeerAndResumeWithAnotherOne(peer, s"${e.getMessage}")
              case _                               =>
            }
          }

        case None =>
          log.info(s"[warn] Received block header ${headers.head.number} without parent from ${peer.id}, trying to lookback to ${headers.head.number - 1}")
          lookbackFromBlock = Some(headers.head.number - 1)
          blockPeerAndResumeWithAnotherOne(peer, s"Got block header ${headers.head.number} that does not have parent")
      }
    } else {
      blockPeerAndResumeWithAnotherOne(peer, s"Got block headers begin at ${headers.head.number} to ${headers.last.number} that are not consistent")
    }
  }

  private def getPrevBlocks(headers: Seq[BlockHeader]) = getPrevBlocks_recurse(headers, Vector())
  @tailrec private def getPrevBlocks_recurse(headers: Seq[BlockHeader], blocks: Vector[Block]): Vector[Block] = headers match {
    case Seq(h, tail @ _*) =>
      blockchain.getBlockByNumber(h.number) match {
        case Some(block) => getPrevBlocks_recurse(tail, blocks :+ block)
        case None        => blocks
      }
    case Seq() => blocks
  }

  private def processBlockBodies(peer: Peer, bodies: Seq[PV62.BlockBody]) {
    if (bodies.nonEmpty && workingHeaders.nonEmpty) {
      val blocks = workingHeaders.zip(bodies).map { case (header, body) => Block(header, body) }

      ledger.validateBlocksBeforeExecution(blocks, validators) match {
        case (preValidatedBlocks, None) =>
          log.debug(s"preValidatedBlocks.head.header.parentHash: ${preValidatedBlocks.head.header.parentHash}")
          blockchain.getBlockHeaderByHash(preValidatedBlocks.head.header.parentHash).flatMap {
            parentHeader => blockchain.getTotalDifficultyByHash(parentHeader.hash)
          } match {
            case Some(blockParentTd) =>
              val start = System.currentTimeMillis
              val (newBlocks, errors) = executeAndInsertBlocks(preValidatedBlocks, blockParentTd, preValidatedBlocks.size > 1)
              val elapsed = (System.currentTimeMillis - start) / 1000.0

              if (newBlocks.nonEmpty) {
                val (nTx, _gasUsed, nTxInParallel) = newBlocks.foldLeft((0, 0L, 0)) {
                  case ((accTx, accGasUsed, accParallel), b) =>
                    val nTx = b.block.body.transactionList.size
                    (accTx + nTx, accGasUsed + b.block.header.gasUsed, accParallel + b.txInParallel)
                }
                val gasUsed = _gasUsed / 1048576.0
                val parallel = (100.0 * nTxInParallel / nTx)
                log.debug(s"[sync] Executed ${newBlocks.size} blocks up to #${newBlocks.last.block.header.number} in ${ef(elapsed)}s, block time ${ef(elapsed / newBlocks.size)}s, ${xf(nTx / elapsed)} tx/s, ${gf(gasUsed / elapsed)} Mgas/s, parallel: ${pf(parallel)}%")

                broadcastNewBlocks(newBlocks)
              }

              errors match {
                case Vector() =>
                  workingHeaders = workingHeaders.drop(blocks.length)
                  if (workingHeaders.nonEmpty) {
                    val hashes = workingHeaders.take(blockBodiesPerRequest).map(_.hash)
                    requestingBodies(peer, hashes)(syncRequestTimeout.plus((hashes.size * 100).millis)) map {
                      case Some(sync.BlockBodiesResponse(peerId, bodies)) =>
                        self ! ProcessBlockBodies(peer, bodies)
                      case None =>
                        blockPeerAndResumeWithAnotherOne(peer, s"Got empty block bodies response for known hashes: ${hashes.map(_.hexString)}")
                    } andThen {
                      case Failure(e: AskTimeoutException) => blockPeerAndResumeWithAnotherOne(peer, s"${e.getMessage}")
                      case Failure(e)                      => blockPeerAndResumeWithAnotherOne(peer, s"${e.getMessage}")
                      case _                               =>
                    }
                  } else {
                    scheduleResume()
                  }

                case Vector(error @ MissingNodeExecptionError(number, hash, table), _*) =>
                  log.info(s"[warn] Execution error $error, in block ${error.blockNumber}, try to fetch from ${peer.id}")
                  val future = requestingNodeData(nodeOkPeer.getOrElse(peer), hash)(3.seconds) map {
                    case Some(sync.NodeDataResponse(peerId, value)) =>
                      table match {
                        case KesqueDataSource.account => accountNodeStorage.put(hash, value.toArray)
                        case KesqueDataSource.storage => storageNodeStorage.put(hash, value.toArray)
                      }

                    case None =>
                  } andThen {
                    case Failure(e) => nodeErrorPeers += peer
                    case _          =>
                  } andThen {
                    case _ => self ! ResumeRegularSyncTick
                  }
                  try {
                    scala.concurrent.Await.ready(future, 10.seconds)
                  } catch {
                    case e: Throwable => // Await.ready will throw timeout if future timeout
                  }

                case Vector(error, _*) =>
                  val numberBlockFailed = blocks.head.header.number + newBlocks.length
                  log.error(s"[sync] Execution error $error, in block ${error.blockNumber}")
                  blockPeerAndResumeWithAnotherOne(peer, s"Block execution error: $error, in block ${error.blockNumber} from ${peer.id}, will sync from another peer")
              }

            case None =>
              // TODO: Investigate if we can recover from this error (EC-165)
              throw new IllegalStateException(s"No total difficulty for the latest block with number ${blocks.head.header.number - 1} (and hash ${blocks.head.header.parentHash.hexString})")
          }

        case (_, Some(error)) =>
          log.info(s"[warn] Before execution validate error: $error, in block ${error.blockNumber} from ${peer.id}, will sync from another peer")
          lookbackFromBlock = Some(error.blockNumber - 1)
          blockPeerAndResumeWithAnotherOne(peer, s"Validate blocks before execution error: $error, in block ${error.blockNumber}")
      }

    } else {
      blockPeerAndResumeWithAnotherOne(peer, "Got empty response for bodies from peer but we got block headers earlier")
    }
  }

  /**
   * Inserts and executes all the blocks, up to the point to which one of them fails (or we run out of blocks).
   * If the execution of any block were to fail, newBlocks only contains the NewBlock msgs for all the blocks executed before it,
   * and only the blocks successfully executed are inserted into the blockchain.
   *
   * @param blocks to execute
   * @param blockParentTd, td of the parent of the blocks.head block
   * @param newBlocks which, after adding the corresponding NewBlock msg for blocks, will be broadcasted
   * @return list of NewBlocks to broadcast (one per block successfully executed) and  errors if happened during execution
   */
  private def executeAndInsertBlocks(blocks: Seq[Block], blockParentTd: BigInteger, isBatch: Boolean) = executeAndInsertBlocks_recurse(blocks, blockParentTd, isBatch, Vector(), Vector())
  @tailrec private def executeAndInsertBlocks_recurse(blocks: Seq[Block], blockParentTd: BigInteger, isBatch: Boolean, newBlocks: Vector[NewBlock], errors: Vector[BlockExecutionError]): (Vector[NewBlock], Vector[BlockExecutionError]) = {
    blocks match {
      case Seq(block, tail @ _*) =>
        executeAndInsertBlock(block, blockParentTd, isBatch) match {
          case Right(newBlock) =>
            // check blockHashToDelete
            blockchain.getBlockHeaderByNumber(block.header.number).map(_.hash).filter(_ != block.header.hash) foreach blockchain.removeBlock

            executeAndInsertBlocks_recurse(tail, newBlock.totalDifficulty, isBatch, newBlocks :+ newBlock, errors)

          case Left(error) =>
            (newBlocks, errors :+ error)
        }

      case Seq() => newBlocks -> errors
    }
  }

  private def executeAndInsertBlock(block: Block, parentTd: BigInteger, isBatch: Boolean): Either[BlockExecutionError, NewBlock] = {
    try {
      val start = System.currentTimeMillis
      ledger.executeBlock(block, validators) match {
        case Right(BlockResult(world, _, receipts, parallelCount, dbTimePercent)) =>
          val newTd = parentTd add block.header.difficulty

          val start1 = System.currentTimeMillis
          world.persist()
          blockchain.save(block)
          blockchain.save(block.header.hash, receipts)
          appStateStorage.putBestBlockNumber(block.header.number)
          blockchain.save(block.header.hash, newTd)
          log.debug(s"${block.header.number} persisted in ${System.currentTimeMillis - start1}ms")

          pendingTransactionsService ! PendingTransactionsService.RemoveTransactions(block.body.transactionList)
          ommersPool ! OmmersPool.RemoveOmmers((block.header +: block.body.uncleNodesList).toList)

          val nTx = block.body.transactionList.size
          val gasUsed = block.header.gasUsed / 1048576.0
          val payloadSize = block.body.transactionList.map(_.tx.payload.size).foldLeft(0)(_ + _)
          val elapsed = (System.currentTimeMillis - start) / 1000.0
          val parallel = 100.0 * parallelCount / nTx
          log.info(s"[sync]${if (isBatch) "+" else " "}Executed #${block.header.number} (${tf(nTx)} tx) in ${ef(elapsed)}s, ${xf(nTx / elapsed)} tx/s, ${gf(gasUsed / elapsed)} mgas/s, payload ${lf(payloadSize)}, parallel ${pf(parallel)}%, db ${pf(dbTimePercent)}%")
          Right(NewBlock(block, newTd, parallelCount))

        case Left(err) =>
          log.warning(s"Failed to execute mined block because of $err")
          Left(err)
      }
    } catch { // TODO need detailed here
      case ex: Throwable =>
        log.error(ex, s"Failed to execute mined block because of exception: ${ex.getMessage}")
        Left(ValidationBeforeExecError(block.header.number, ex.getMessage))
    }
  }

  private def scheduleResume() {
    timers.startSingleTimer(ResumeRegularSyncTask, ResumeRegularSyncTick, checkForNewBlockInterval)
  }

  private def blockPeerAndResumeWithAnotherOne(currPeer: Peer, reason: String) {
    self ! BlacklistPeer(currPeer.id, reason)
    self ! ResumeRegularSyncTick
  }

  private def checkHeaders(headers: Seq[BlockHeader]): Boolean = {
    headers.zip(headers.tail).forall { case (parent, child) => parent.hash == child.parentHash && parent.number + 1 == child.number }
  }

  private def bestPeer: Option[Peer] = {
    val peersToUse = peersToDownloadFrom.collect {
      case (peer, PeerInfo(_, totalDifficulty, true, _)) => (peer, totalDifficulty)
    }

    if (peersToUse.nonEmpty) {
      val candicates = peersToUse.toList.sortBy { case (_, td) => td.negate }.take(3).toArray
      Some(candicates(nextCandicate(0, candicates.length))._1)
    } else {
      None
    }
  }

  private var nodeErrorPeers = Set[Peer]()
  private def nodeOkPeer: Option[Peer] = {
    val peersToUse = peersToDownloadFrom.collect {
      case (peer, PeerInfo(_, totalDifficulty, true, _)) => (peer, totalDifficulty)
    } -- nodeErrorPeers

    if (peersToUse.nonEmpty) {
      val candicates = peersToUse.toList.sortBy { case (_, td) => td.negate }.take(3).toArray
      Some(candicates(nextCandicate(0, candicates.length))._1)
    } else {
      None
    }
  }

  private def nextCandicate(low: Int, high: Int) = { // >= low and < high
    val rnd = ThreadLocalRandom.current()
    rnd.nextInt(high - low) + low
  }

  /**
   * Broadcasts various NewBlock's messages to handshaked peers, considering that a block should not be sent to a peer
   * that is thought to know it. In the current implementation we send every block to every peer (that doesn't know
   * this block)
   *
   * @param newBlocks, blocks to broadcast
   * @param handshakedPeers
   */
  //FIXME: Decide block propagation algorithm (for now we send block to every peer) [EC-87]
  def broadcastNewBlocks(newBlocks: Seq[NewBlock]) {
    mediator ! Publish(khipu.NewBlockTopic, BroadcastNewBlocks(newBlocks))
  }
}
