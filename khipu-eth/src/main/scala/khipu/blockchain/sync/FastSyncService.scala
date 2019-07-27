package khipu.blockchain.sync

import akka.pattern.AskTimeoutException
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.ByteString
import java.util.concurrent.ThreadLocalRandom
import khipu.Hash
import khipu.DataWord
import khipu.blockchain.sync
import khipu.blockchain.sync.HandshakedPeersService.BlacklistPeer
import khipu.crypto
import khipu.domain.BlockHeader
import khipu.domain.Receipt
import khipu.network.handshake.EtcHandshake.PeerInfo
import khipu.network.p2p.messages.PV62
import khipu.network.p2p.messages.CommonMessages.Status
import khipu.network.rlpx.Peer
import khipu.network.rlpx.RLPxStage
import khipu.store.trienode.NodeKeyValueStorage
import khipu.util.Config
import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

/**
 * eth/63 fast synchronization algorithm
 * https://github.com/ethereum/go-ethereum/pull/1889
 *
 * An outline of the fast sync algorithm would be:
 * - Similarly to classical sync, download the block headers and bodies that make up the blockchain
 * - Similarly to classical sync, verify the header chain's consistency (POW, total difficulty, etc)
 * - Instead of processing the blocks, download the transaction receipts as defined by the header
 * - Store the downloaded blockchain, along with the receipt chain, enabling all historical queries
 * - When the chain reaches a recent enough state (head - 1024 blocks), pause for state sync:
 *   - Retrieve the entire Merkel Patricia state trie defined by the root hash of the pivot point
 *   - For every account found in the trie, retrieve it's contract code and internal storage state trie
 * - Upon successful trie download, mark the pivot point (head - 1024 blocks) as the current head
 * - Import all remaining blocks (1024) by fully processing them as in the classical sync
 *
 */
object FastSyncService {
  case object RetryStart

  case object BlockHeadersTimeout
  case object TargetBlockTimeout

  case object ProcessSyncingTask
  case object ProcessSyncingTick

  case object PersistSyncStateTask
  case object PersistSyncStateTick

  case object ReportStatusTask
  case object ReportStatusTick

  final case class SyncState(
    targetBlockNumber:         Long,
    targetBlockHeader:         Option[BlockHeader]                     = None,
    var bestBlockHeaderNumber: Long                                    = 0,
    var enqueuedBlockNumber:   Long                                    = 0,
    var downloadedNodesCount:  Int                                     = 0,
    var pendingMptNodes:       List[NodeHash]                          = Nil,
    var pendingNonMptNodes:    List[NodeHash]                          = Nil,
    var pendingBlockBodies:    List[Hash]                              = Nil,
    var pendingReceipts:       List[Hash]                              = Nil,
    workingMptNodes:           mutable.LinkedHashMap[NodeHash, AnyRef] = mutable.LinkedHashMap[NodeHash, AnyRef](),
    workingNonMptNodes:        mutable.LinkedHashMap[NodeHash, AnyRef] = mutable.LinkedHashMap[NodeHash, AnyRef](),
    workingBlockBodies:        mutable.LinkedHashMap[Hash, AnyRef]     = mutable.LinkedHashMap[Hash, AnyRef](),
    workingReceipts:           mutable.LinkedHashMap[Hash, AnyRef]     = mutable.LinkedHashMap[Hash, AnyRef]()
  )

  sealed trait Work
  final case class HeadersWork(enqueueHashs: List[Hash], headers: Map[Hash, BlockHeader], tds: Map[Hash, DataWord], lastNumber: Option[Long]) extends Work
  final case class BodiesWork(workHashes: List[Hash], enqueueHashs: List[Hash], bodies: Map[Hash, PV62.BlockBody], receivedHashes: List[Hash]) extends Work
  final case class ReceiptsWork(workHashes: List[Hash], enqueueHashs: List[Hash], receipts: Map[Hash, Seq[Receipt]]) extends Work
  final case class NodesWork(workHashes: List[NodeHash], enqueueHashs: List[NodeHash], downloadedCount: Int, accountNodes: Map[Hash, Array[Byte]], storageNodes: Map[Hash, Array[Byte]], evmcodes: Map[Hash, ByteString]) extends Work

  final case class PeerWorkDone(peer: Peer, work: Work)

  final case class MarkPeerBlockchainOnly(peer: Peer)

  sealed trait BlockBodyValidationResult
  case object Valid extends BlockBodyValidationResult
  case object Invalid extends BlockBodyValidationResult
  case object DbError extends BlockBodyValidationResult
}
trait FastSyncService { _: SyncService =>
  import context.dispatcher
  import FastSyncService._
  import Config.Sync._

  private implicit val fastSyncTimeout = RLPxStage.decodeTimeout.plus(20.seconds)

  private var syncState: Option[SyncState] = None
  protected def startFastSync() {
    log.info("Trying to start block synchronization (fast mode)")
    fastSyncStateStorage.getSyncState match {
      case Some(syncState) => startFastSyncWithSyncState(syncState, isInitialSyncState = false)
      case None            => startFastSyncFromScratch()
    }
  }

  private def startFastSyncWithSyncState(syncState: SyncState, isInitialSyncState: Boolean) {
    this.syncState = Some(syncState)
    if (isInitialSyncState) {
      saveSyncState()
    }

    log.info("Start fast synchronization")
    context become ((new SyncingHandler(syncState).receive) orElse peerUpdateBehavior orElse ommersBehavior)
    self ! ProcessSyncingTick
  }

  private def fastSyncBehavior: Receive = peerUpdateBehavior orElse ommersBehavior orElse stopBehavior orElse {
    case RetryStart => startFastSync()
  }

  private def scheduleStartRetry(interval: FiniteDuration) = {
    context.system.scheduler.scheduleOnce(interval, self, RetryStart)
  }

  private def startFastSyncFromScratch() = {
    log.info("Start fast synchronization from scratch")

    val peersUsedToChooseTarget = peersToDownloadFrom

    if (peersUsedToChooseTarget.size >= minPeersToChooseTargetBlock) {
      log.debug(s"Asking ${peersUsedToChooseTarget.size} peers for block headers")

      // def f(f: Future[Int]) = f map {x => x + 1} recover {case _ => 0}
      // val fs = Future.sequence(List(f(Future.successful(1)), f(Future.failed(new RuntimeException("")))))
      // res22: scala.concurrent.Future[List[Int]] = Success(List(2, 0))
      Future.sequence(peersUsedToChooseTarget map {
        case (peer, PeerInfo(Status(protocolVersion, networkId, totalDifficulty, bestHash, genesisHash), _, _, _)) =>
          requestingHeaders(peer, None, Right(bestHash), 1, 0, reverse = false) map {
            case Some(BlockHeadersResponse(peerId, Seq(header), true)) =>
              log.debug(s"Got BlockHeadersResponse with 1 header number=${header.number} from ${peer.id}")
              Some(peer -> header)

            case Some(BlockHeadersResponse(peerId, headers, true)) =>
              self ! BlacklistPeer(peerId, s"Got BlockHeadersRespons with more than 1 header ${headers.size}, blacklisting for $blacklistDuration")
              None

            case Some(BlockHeadersResponse(peerId, _, false)) =>
              self ! BlacklistPeer(peerId, s"Got BlockHeadersRespons with non-consistent headers, blacklisting for $blacklistDuration")
              None

            case None =>
              // do not blacklist it, since the targetBlockNumber may be wrong one
              log.debug(s"Got empty block headers response for requested: $bestHash")
              None

          } recover {
            case e: AskTimeoutException =>
              self ! BlacklistPeer(peer.id, s"${e.getMessage}")
              None
            case e =>
              self ! BlacklistPeer(peer.id, s"${e.getMessage}")
              None
          }
      }) map {
        receivedHeaders => tryStartFastSync(receivedHeaders.flatten.toSeq)
      }
    } else {
      log.info(s"Fast synchronization did not start yet. Need at least ${minPeersToChooseTargetBlock} peers, but only ${peersUsedToChooseTarget.size} available at the moment. Retry in ${startRetryInterval.toSeconds} seconds")
      scheduleStartRetry(startRetryInterval)
      context become fastSyncBehavior
    }
  }

  private def tryStartFastSync(receivedHeaders: Seq[(Peer, BlockHeader)]) {
    log.debug(s"Trying to start fast sync. Received ${receivedHeaders.size} block headers")

    if (receivedHeaders.size >= minPeersToChooseTargetBlock) {
      val (chosenPeer, chosenBlockHeader) = chooseTargetBlock(receivedHeaders)
      val targetBlockNumber = chosenBlockHeader.number - targetBlockOffset

      if (targetBlockNumber < 1) {
        log.debug("Target block is less than 1 now, starting regular sync")
        appStateStorage.fastSyncDone()
        context become idle
        self ! SyncService.FastSyncDone
      } else {
        log.info(s"Fetching block headers of target $targetBlockNumber")

        val fs = Future.sequence(receivedHeaders.toSeq.map(_._1) map { peer =>
          requestingHeaders(peer, None, Left(targetBlockNumber), 1, 0, reverse = false) transform {
            case Success(Some(BlockHeadersResponse(peerId, headers, true))) =>
              log.debug(s"Got BlockHeadersResponse with 1 header from ${peer.id}")
              headers.find(_.number == targetBlockNumber) match {
                case Some(targetBlockHeader) =>
                  log.info(s"Pre start fast sync, got one target block ${targetBlockHeader} from ${peer.id}")
                  Success(Some(peer -> targetBlockHeader))
                case None =>
                  self ! BlacklistPeer(peerId, s"did not respond with proper target block header")
                  Success(None)
              }

            case Success(Some(BlockHeadersResponse(peerId, _, false))) =>
              self ! BlacklistPeer(peerId, s"Got BlockHeadersRespons with non-consistent headers")
              Success(None)

            case Success(None) =>
              // do not blacklist it, since the targetBlockNumber may be wrong one
              log.debug(s"Target block header receive empty.")
              Success(None)

            case Failure(e: AskTimeoutException) =>
              self ! BlacklistPeer(peer.id, s"${e.getMessage}")
              Success(None)

            case Failure(e) =>
              self ! BlacklistPeer(peer.id, s"${e.getMessage}")
              Success(None)
          }
        })

        fs map { headers =>
          val (stateRoot, peerToBlockHeader) = headers.flatten.groupBy(_._2.stateRoot).maxBy(_._2.size)
          val nSameHeadersRequired = math.min(minPeersToChooseTargetBlock, 3)
          if (peerToBlockHeader.size >= nSameHeadersRequired) {
            val peers = receivedHeaders.map(_._1).toSet

            val (goodPeers, blockHeaders) = peerToBlockHeader.unzip
            headerWhitePeers ++= goodPeers
            headerBlackPeers = peers -- headerWhitePeers
            headerBlackPeers foreach { peer =>
              self ! BlacklistPeer(peer.id, s"Got uncertain block header", always = true)
            }

            val targetBlockHeader = blockHeaders.head
            log.info(s"Got enough block headers that have the same stateRoot, starting block synchronization (fast mode). Target block ${targetBlockHeader}")
            val initialSyncState = SyncState(
              targetBlockNumber = targetBlockHeader.number,
              targetBlockHeader = Some(targetBlockHeader),
              pendingMptNodes = List(StateMptNodeHash(targetBlockHeader.stateRoot.bytes))
            )
            startFastSyncWithSyncState(initialSyncState, isInitialSyncState = true)
          } else {
            log.info(s"Could not get enough block headers that have the same stateRoot, requires ${nSameHeadersRequired}, but only found ${peerToBlockHeader.size}")
            scheduleStartRetry(startRetryInterval)
            context become fastSyncBehavior
          }
        }
      }

    } else {
      log.info(s"Block synchronization (fast mode) does not started. Need to receive block headers from at least $minPeersToChooseTargetBlock peers, but received only from ${receivedHeaders.size}. Retrying in ${startRetryInterval.toSeconds} seconds")
      scheduleStartRetry(startRetryInterval)
      context become fastSyncBehavior
    }
  }

  private def chooseTargetBlock(receivedHeaders: Iterable[(Peer, BlockHeader)]): (Peer, BlockHeader) = {
    val headers = receivedHeaders.toArray.sortBy(_._2.number)
    assert(headers.length > 0)
    val middlePosition = (headers.length & 1) match {
      case 1 => headers.length / 2
      case 0 => headers.length / 2
    }
    headers(middlePosition)
  }

  protected def saveSyncState() {
    syncState foreach { syncState =>
      val start = System.nanoTime
      fastSyncStateStorage.putSyncState(syncState)
      log.info(s"[fast] Saved sync state in ${(System.nanoTime - start) / 1000000}ms, header/enqueued: ${syncState.bestBlockHeaderNumber}/${syncState.enqueuedBlockNumber}, bodies queue: ${syncState.pendingBlockBodies.size + syncState.workingBlockBodies.size}, receipts queue: ${syncState.pendingReceipts.size + syncState.workingReceipts.size}, nodes queue: ${syncState.pendingMptNodes.size + syncState.pendingNonMptNodes.size + syncState.workingMptNodes.size + syncState.workingNonMptNodes.size}, downloaded nodes: ${syncState.downloadedNodesCount} ")
    }
  }

  private class SyncingHandler(syncState: SyncState) {
    blockHeaderForChecking = syncState.targetBlockHeader

    private var workingPeers = Set[Peer]()
    private var headerWorkingPeer: Option[String] = None

    private var blockchainOnlyPeers = Map[String, Peer]()

    private var currSyncedBlockNumber = appStateStorage.getBestBlockNumber
    private var prevSyncedBlockNumber = currSyncedBlockNumber
    private var prevDownloadeNodes = syncState.downloadedNodesCount
    private var prevReportTime = System.nanoTime

    log.info(s"[fast] sync to target block header: \n${syncState.targetBlockHeader.get}")

    timers.startPeriodicTimer(ProcessSyncingTask, ProcessSyncingTick, syncRetryInterval)
    timers.startPeriodicTimer(PersistSyncStateTask, PersistSyncStateTick, persistStateSnapshotInterval)

    reportStatus()

    def receive: Receive = peerUpdateBehavior orElse ommersBehavior orElse {

      case ProcessSyncingTick =>
        processSyncing()

      case PeerWorkDone(peerId, work) =>
        work match {
          case BodiesWork(workHashes, enqueueHashs, bodies, receivedHashes) =>
            saveBodies(bodies, receivedHashes)

            syncState.pendingBlockBodies = enqueueHashs ::: syncState.pendingBlockBodies
            syncState.workingBlockBodies --= workHashes
            currSyncedBlockNumber = appStateStorage.getBestBlockNumber

          case ReceiptsWork(workHashes, enqueueHashs, receipts) =>
            saveReceipts(receipts)

            syncState.pendingReceipts = enqueueHashs ::: syncState.pendingReceipts
            syncState.workingReceipts --= workHashes

          case NodesWork(workHashes, enqueueHashs, downloadedCount, accountNodes, storageNodes, evmcodes) =>
            saveAccountNodes(accountNodes)
            saveStorageNodes(storageNodes)
            saveEvmcodes(evmcodes)

            log.debug(s"Saved acccount: ${accountNodes.size}, storage: ${storageNodes.size}, evmcode: ${evmcodes.size}. Total ${accountNodes.size + storageNodes.size + evmcodes.size} - downloaded: ${downloadedCount}")

            workHashes foreach {
              case h: EvmcodeHash                => syncState.workingNonMptNodes -= h
              case h: StorageRootHash            => syncState.workingNonMptNodes -= h
              case h: StateMptNodeHash           => syncState.workingMptNodes -= h
              case h: ContractStorageMptNodeHash => syncState.workingMptNodes -= h
            }

            // always enqueue hashes in the front, this will get shorter pending queue !!!
            enqueueHashs foreach {
              case h: EvmcodeHash                => syncState.pendingNonMptNodes = h :: syncState.pendingNonMptNodes
              case h: StorageRootHash            => syncState.pendingNonMptNodes = h :: syncState.pendingNonMptNodes
              case h: StateMptNodeHash           => syncState.pendingMptNodes = h :: syncState.pendingMptNodes
              case h: ContractStorageMptNodeHash => syncState.pendingMptNodes = h :: syncState.pendingMptNodes
            }

            syncState.downloadedNodesCount += downloadedCount

          case HeadersWork(enqueueHashs, headers, tds, lastNumber) =>
            saveHeaders(headers)
            saveTotalDifficulties(tds)

            lastNumber match {
              case Some(n) if (n > syncState.bestBlockHeaderNumber) => syncState.bestBlockHeaderNumber = n
              case None =>
            }

            headerWorkingPeer = None
        }

        workingPeers -= peerId
        processSyncing()

      case MarkPeerBlockchainOnly(peer) =>
        if (!blockchainOnlyPeers.contains(peer.id)) {
          blockchainOnlyPeers = blockchainOnlyPeers.take(blockchainOnlyPeersPoolSize) + (peer.id -> peer)
        }

      case ReportStatusTick =>
        reportStatus()

      case PersistSyncStateTick =>
        saveSyncState()
    }

    def processSyncing() {
      if (isFullySynced) {
        // TODO check isFullySynced is not enough, since saving blockbodies and appStateStorage are async 
        reportStatus()
        val bestBlockNumber = appStateStorage.getBestBlockNumber
        if (bestBlockNumber == syncState.targetBlockNumber) {
          log.info(s"[fast] Block synchronization in fast mode finished, switching to regular mode")
          finishFastSync()
        } else {
          log.info(s"[fast] Waiting for assigning works left $bestBlockNumber/${syncState.targetBlockNumber}")
        }
      } else {
        if (isAnythingToDownload) {
          processDownload()
        } else {
          log.info(s"[fast] No more items to request, waiting for ${workingPeers.size} requests finish")
        }
      }
    }

    private def finishFastSync() {
      blockHeaderForChecking = None
      blockchainOnlyPeers = Map()
      timers.cancel(ReportStatusTask)
      timers.cancel(ProcessSyncingTask)
      timers.cancel(PersistSyncStateTask)
      fastSyncStateStorage.purge()
      appStateStorage.fastSyncDone()
      context become idle
      self ! SyncService.FastSyncDone
    }

    private def processDownload() {
      if (unassignedPeers.isEmpty) {
        if (workingPeers.isEmpty) {
          log.debug("There are no available peers, waiting for ProcessSyncingTick")
        } else {
          log.debug("There are no available peers, waiting for ProcessSyncingTick or working peers done")
        }
      } else {

        val headerWork = if (syncState.pendingBlockBodies.size + syncState.pendingReceipts.size < 10000) {
          if (isThereHeaderToDownload && headerWorkingPeer.isEmpty) {
            val candicates = headerWhitePeers -- workingPeers
            nextPeer(candicates.toArray) map { peer =>
              headerWorkingPeer = Some(peer.id)
              workingPeers += peer
              peer
            }
          } else {
            None
          }
        } else {
          None
        }

        val nodeWorks = if (syncState.pendingNonMptNodes.nonEmpty || syncState.pendingMptNodes.nonEmpty) {
          val blockchainOnlys = blockchainOnlyPeers.values.toSet
          unassignedPeers.filterNot(blockchainOnlys.contains)
            .take(maxConcurrentRequests - workingPeers.size)
            .foldLeft(Vector[(Peer, List[NodeHash])]()) {
              case (acc, peer) =>
                if (syncState.pendingNonMptNodes.nonEmpty || syncState.pendingMptNodes.nonEmpty) {
                  val (requestingNonMptNodes, remainingNonMptNodes) = syncState.pendingNonMptNodes.splitAt(nodesPerRequest)
                  val (requestingMptNodes, remainingMptNodes) = syncState.pendingMptNodes.splitAt(nodesPerRequest - requestingNonMptNodes.size)

                  syncState.pendingNonMptNodes = remainingNonMptNodes
                  syncState.pendingMptNodes = remainingMptNodes
                  syncState.workingNonMptNodes ++= requestingNonMptNodes.map(x => (x -> null))
                  syncState.workingMptNodes ++= requestingMptNodes.map(x => (x -> null))

                  workingPeers += peer

                  acc :+ (peer, requestingNonMptNodes ::: requestingMptNodes)
                } else {
                  acc
                }
            }
        } else {
          Vector()
        }

        if (syncState.pendingBlockBodies.size + syncState.pendingReceipts.size < 10000) {
          val from = syncState.enqueuedBlockNumber + 1
          val to = math.min(syncState.enqueuedBlockNumber + 200, syncState.bestBlockHeaderNumber)
          val (lastNumber, enqueueHashs) = storages.getHashsByBlockNumberRange(from, to)
          if (enqueueHashs.nonEmpty) {
            syncState.enqueuedBlockNumber = lastNumber

            syncState.pendingReceipts = enqueueHashs ::: syncState.pendingReceipts
            syncState.pendingBlockBodies = enqueueHashs ::: syncState.pendingBlockBodies
          }
        }

        val receiptWorks = if (syncState.pendingReceipts.nonEmpty) {
          unassignedPeers
            .take(maxConcurrentRequests - workingPeers.size)
            .foldLeft(Vector[(Peer, List[Hash])]()) {
              case (acc, peer) =>
                if (syncState.pendingReceipts.nonEmpty) {
                  val (requestingReceipts, remainingReceipts) = syncState.pendingReceipts.splitAt(receiptsPerRequest)

                  syncState.pendingReceipts = remainingReceipts
                  syncState.workingReceipts ++= requestingReceipts.map(x => (x -> null))

                  workingPeers += peer

                  acc :+ (peer, requestingReceipts)
                } else {
                  acc
                }
            }
        } else {
          Vector()
        }

        val bodyWorks = if (syncState.pendingBlockBodies.nonEmpty) {
          unassignedPeers
            .take(maxConcurrentRequests - workingPeers.size)
            .foldLeft(Vector[(Peer, List[Hash])]()) {
              case (acc, peer) =>
                if (syncState.pendingBlockBodies.nonEmpty) {
                  val (requestingHashes, remainingHashes) = syncState.pendingBlockBodies.splitAt(blockBodiesPerRequest)

                  syncState.pendingBlockBodies = remainingHashes
                  syncState.workingBlockBodies ++= requestingHashes.map(x => (x -> null))

                  workingPeers += peer

                  acc :+ (peer, requestingHashes)
                } else {
                  acc
                }
            }
        } else {
          Vector()
        }

        receiptWorks foreach { case (peer, requestingReceipts) => requestReceipts(peer, requestingReceipts) }
        bodyWorks foreach { case (peer, requestingHashes) => requestBlockBodies(peer, requestingHashes) }
        headerWork foreach { peer => requestBlockHeaders(peer) }
        nodeWorks foreach { case (peer, requestingNodes) => requestNodes(peer, requestingNodes) }
      }
    }

    private def nextPeer(candicates: Array[Peer]): Option[Peer] = {
      if (candicates.nonEmpty) {
        Some(candicates(nextCandicate(0, candicates.length)))
      } else {
        None
      }
    }

    private def nextCandicate(low: Int, high: Int) = { // >= low and < high
      val rnd = ThreadLocalRandom.current()
      rnd.nextInt(high - low) + low
    }

    private def unassignedPeers: List[Peer] = {
      val peerToUse = peersToDownloadFrom collect {
        case (peer, PeerInfo(_, totalDifficulty, true, _)) if !workingPeers.contains(peer) => (peer, totalDifficulty)
      }
      peerToUse.toList.sortBy { -_._2 } map (_._1)
    }

    private def isFullySynced =
      !isAnythingToDownload && workingPeers.isEmpty

    private def isAnythingToDownload =
      isThereHeaderToDownload || isAnythingQueued

    private def isThereHeaderToDownload =
      syncState.bestBlockHeaderNumber < syncState.targetBlockNumber

    private def isAnythingQueued =
      syncState.pendingNonMptNodes.nonEmpty ||
        syncState.pendingMptNodes.nonEmpty ||
        syncState.pendingBlockBodies.nonEmpty ||
        syncState.pendingReceipts.nonEmpty

    def requestBlockHeaders(peer: Peer) {
      val start = System.nanoTime

      val limit = math.min(blockHeadersPerRequest, syncState.targetBlockNumber - syncState.bestBlockHeaderNumber)
      log.debug(s"Request block headers $limit from ${peer.id}")
      //log.debug(s"Request block headers: ${request.message.block.fold(n => n, _.hexString)}, bestBlockHeaderNumber is ${syncState.bestBlockHeaderNumber}, target is ${syncState.targetBlockNumber}")
      blockchain.getBlockHeaderByNumber(syncState.bestBlockHeaderNumber) match {
        case Some(parentHeader) =>
          requestingHeaders(peer, Some(parentHeader), Left(syncState.bestBlockHeaderNumber + 1), limit, skip = 0, reverse = false) andThen {
            case Success(Some(BlockHeadersResponse(peerId, headers, true))) =>
              log.debug(s"Got block headers ${headers.size} from ${peer.id} in ${(System.nanoTime - start) / 1000000}ms")
              val headersWork = toHeadersWork(headers)

              self ! PeerWorkDone(peer, headersWork)

            case Success(Some(BlockHeadersResponse(peerId, List(), false))) =>
              self ! BlacklistPeer(peer.id, s"Got block headers non-consistent for requested: ${syncState.bestBlockHeaderNumber + 1}")
              self ! PeerWorkDone(peer, HeadersWork(Nil, Map(), Map(), None))

            case Success(None) =>
              self ! BlacklistPeer(peer.id, s"Got block headers empty for known header: ${syncState.bestBlockHeaderNumber + 1}")
              self ! PeerWorkDone(peer, HeadersWork(Nil, Map(), Map(), None))

            case Failure(e) =>
              self ! BlacklistPeer(peer.id, s"${e.getMessage}")
              self ! PeerWorkDone(peer, HeadersWork(Nil, Map(), Map(), None))
          }

        case None => // TODO
          log.error(s"previous best block ${syncState.bestBlockHeaderNumber} does not exist yet, something wrong !!!")
      }
    }

    def requestBlockBodies(peer: Peer, requestingHashes: List[Hash]) {
      val start = System.nanoTime

      log.debug(s"Request block bodies ${requestingHashes.size} from ${peer.id}")
      requestingBodies(peer, requestingHashes) andThen {
        case Success(Some(BlockBodiesResponse(peerId, remainingHashes, receivedHashes, bodies))) =>
          log.debug(s"Got block bodies ${bodies.size} from ${peer.id} in ${(System.nanoTime - start) / 1000000}ms")

          validateBlocks(receivedHashes, bodies) match {
            case Valid =>
              self ! PeerWorkDone(peer, BodiesWork(requestingHashes, remainingHashes, (receivedHashes zip bodies).toMap, receivedHashes))

            case Invalid =>
              self ! BlacklistPeer(peerId, s"$peerId responded with invalid block bodies that are not matching block headers")
              self ! PeerWorkDone(peer, BodiesWork(requestingHashes, requestingHashes, Map(), Nil))

            case DbError =>
              log.error("DbError")
              syncState.pendingBlockBodies = List()
              syncState.pendingReceipts = List()
              //todo adjust the formula to minimize redownloaded block headers
              syncState.bestBlockHeaderNumber = syncState.bestBlockHeaderNumber - 2 * blockHeadersPerRequest
              log.warning("missing block header for known hash")
              self ! PeerWorkDone(peer, BodiesWork(requestingHashes, Nil, Map(), Nil))
          }

        case Success(None) =>
          self ! BlacklistPeer(peer.id, s"Got block bodies empty response for known hashes from ${peer.id}: $requestingHashes")
          self ! PeerWorkDone(peer, BodiesWork(requestingHashes, requestingHashes, Map(), Nil))

        case Failure(e) =>
          self ! BlacklistPeer(peer.id, s"${e.getMessage}")
          self ! PeerWorkDone(peer, BodiesWork(requestingHashes, requestingHashes, Map(), Nil))
      }
    }

    def requestReceipts(peer: Peer, requestingHashes: List[Hash]) {
      val start = System.nanoTime

      log.debug(s"Request receipts from ${peer.id}")
      requestingReceipts(peer, requestingHashes) andThen {
        case Success(Some(ReceiptsResponse(peerId, remainingHashes, receivedHashes, receipts))) =>
          log.debug(s"Got receipts ${receipts.size} from ${peer.id} in ${(System.nanoTime - start) / 1000000}ms")

          // TODO valid receipts
          self ! PeerWorkDone(peer, ReceiptsWork(requestingHashes, remainingHashes, (receivedHashes zip receipts).toMap))

        case Success(None) =>
          self ! BlacklistPeer(peer.id, s"Got receipts empty for known hashes from ${peer.id}: $requestingHashes")
          self ! PeerWorkDone(peer, ReceiptsWork(requestingHashes, requestingHashes, Map()))

        case Failure(e) =>
          self ! BlacklistPeer(peer.id, s"${e.getMessage}")
          self ! PeerWorkDone(peer, ReceiptsWork(requestingHashes, requestingHashes, Map()))
      }
    }

    def requestNodes(peer: Peer, requestingHashes: List[NodeHash]) {
      val start = System.nanoTime

      log.debug(s"Request nodes from ${peer.id}")
      requestingNodeDatas(peer, requestingHashes) andThen {
        case Success(Some(NodeDatasResponse(peerId, nDownloadedNodes, remainingHashes, childrenHashes, accounts, storages, evmcodes))) =>
          log.debug(s"Got nodes $nDownloadedNodes from ${peer.id} in ${(System.nanoTime - start) / 1000000}ms")

          self ! PeerWorkDone(peer, NodesWork(requestingHashes, remainingHashes ++ childrenHashes, nDownloadedNodes, accounts.toMap, storages.toMap, evmcodes.toMap))

        case Success(None) =>
          log.debug(s"Got nodes empty response for known hashes. Mark ${peer.id} blockchain only")
          self ! MarkPeerBlockchainOnly(peer)
          self ! PeerWorkDone(peer, NodesWork(requestingHashes, requestingHashes, 0, Map(), Map(), Map()))

        case Failure(e) =>
          log.debug(s"Got node $e when request nodes. Mark ${peer.id} blockchain only")
          self ! MarkPeerBlockchainOnly(peer)
          self ! PeerWorkDone(peer, NodesWork(requestingHashes, requestingHashes, 0, Map(), Map(), Map()))
      }
    }

    private def validateBlocks(receviedHashes: Seq[Hash], blockBodies: Seq[PV62.BlockBody]): BlockBodyValidationResult = {
      val headerToBody = (receviedHashes zip blockBodies).map {
        case (hash, body) => (blockchain.getBlockHeaderByHash(hash), body)
      }

      headerToBody.collectFirst {
        case (None, _) => DbError
        case (Some(header), body) =>
          validators.blockValidator.validateHeaderAndBody(header, body) match {
            case Right(_) => Valid
            case Left(error) =>
              log.debug(s"[fast] invalid block body $error \nheader: $header \n$body")
              Invalid
          }
      } getOrElse (Valid)
    }

    private def toHeadersWork(headers: List[BlockHeader]): HeadersWork = {
      // calculate total difficulties
      val (obtains, hds, tds, lastNumber) = headers.foldLeft(mutable.ListBuffer[Hash](), Map[Hash, BlockHeader](), Map[Hash, DataWord](), None: Option[Long]) {
        case ((obtains, hds, tds, lastNumber), h) =>
          tds.get(h.parentHash) orElse blockchain.getTotalDifficultyByHash(h.parentHash) match {
            case Some(parentTotalDifficulty) =>
              (obtains :+ h.hash, hds + (h.hash -> h), tds + (h.hash -> (parentTotalDifficulty + h.difficulty)), Some(h.number))
            case None =>
              (obtains, hds, tds, lastNumber)
          }
      }

      HeadersWork(obtains.toList, hds, tds, lastNumber)
    }

    private def updateBestBlockIfNeeded(receivedBlockHashes: Seq[Hash]): Option[Long] = {
      val blockNumbers = for {
        blockHash <- receivedBlockHashes
        blockHeader <- blockchain.getBlockHeaderByHash(blockHash)
      } yield blockHeader.number

      if (blockNumbers.nonEmpty) {
        val bestReceivedBlockNumber = blockNumbers.max
        val prevSyncedBlockNumber = appStateStorage.getBestBlockNumber
        log.debug(s"bestReceivedBlockNumber: ${bestReceivedBlockNumber}, prevSyncedBlockNumber: ${prevSyncedBlockNumber}")
        if (bestReceivedBlockNumber > prevSyncedBlockNumber) {
          appStateStorage.putBestBlockNumber(bestReceivedBlockNumber)
          log.debug(s"bestReceivedBlockNumber: ${bestReceivedBlockNumber}, prevSyncedBlockNumber: ${prevSyncedBlockNumber}. Saved")
          Some(bestReceivedBlockNumber)
        } else {
          None
        }
      } else {
        None
      }
    }

    // --- saving methods

    private def saveHeaders(kvs: Map[Hash, BlockHeader]) {
      val start = System.nanoTime
      blockchain.saveBlockHeader(kvs)
      log.debug(s"SaveHeaders ${kvs.size} in ${(System.nanoTime - start) / 1000000}ms")
    }

    private def saveBodies(kvs: Map[Hash, PV62.BlockBody], receivedHashes: List[Hash]) {
      val start = System.nanoTime
      blockchain.saveBlockBody(kvs)
      updateBestBlockIfNeeded(receivedHashes)
      log.debug(s"SaveBodies ${kvs.size} in ${(System.nanoTime - start) / 1000000}ms")
    }

    private def saveTotalDifficulties(kvs: Map[Hash, DataWord]) {
      val start = System.nanoTime
      blockchain.saveTotalDifficulty(kvs)
      log.debug(s"SaveDifficulties ${kvs.size} in ${(System.nanoTime - start) / 1000000}ms")
    }

    private def saveReceipts(kvs: Map[Hash, Seq[Receipt]]) {
      val start = System.nanoTime
      blockchain.saveReceipts(kvs)
      log.debug(s"SaveReceipts ${kvs.size} in ${(System.nanoTime - start) / 1000000}ms")
    }

    private def saveAccountNodes(kvs: Map[Hash, Array[Byte]]) {
      val start = System.nanoTime
      saveNodes(accountNodeStorage, kvs)
      log.debug(s"SaveAccountNodes ${kvs.size} in ${(System.nanoTime - start) / 1000000}ms")
    }

    private def saveStorageNodes(kvs: Map[Hash, Array[Byte]]) {
      val start = System.nanoTime
      saveNodes(storageNodeStorage, kvs)
      log.debug(s"SaveStorageNodes ${kvs.size} in ${(System.nanoTime - start) / 1000000}ms")
    }

    private def saveEvmcodes(kvs: Map[Hash, ByteString]) {
      val start = System.nanoTime
      saveNodes(evmcodeStorage, kvs.map(x => x._1 -> x._2.toArray))
      log.debug(s"SaveEvmcodes ${kvs.size} in ${(System.nanoTime - start) / 1000000}ms")
    }

    private def saveNodes(storage: NodeKeyValueStorage, kvs: Map[Hash, Array[Byte]]) {
      //val prev = storage.count
      storage.update(Set(), kvs)
      //val post = storage.count
      //if (post - prev == kvs.size) {
      //  log.info(s"saving ${storage.tableName} ${kvs.size}, saved ${post - prev}")
      //} else {
      //  log.warning(s"saving ${storage.tableName} ${kvs.size}, but saved ${post - prev}")
      //}
    }

    //    private val accountNodeBuf = new mutable.HashMap[Hash, Array[Byte]]()
    //    private val storageNodeBuf = new mutable.HashMap[Hash, Array[Byte]]()
    //    private def saveNodes(storage: NodeKeyValueStorage, kvs: Map[Hash, Array[Byte]], buf: mutable.HashMap[Hash, Array[Byte]]) {
    //      var size = 0
    //      kvs foreach {
    //        case (k, v) =>
    //          buf += (k -> v)
    //          size += 1
    //          if (size > 100) { // save per 100 to keep the batched size around 4096 (~ 32*100 bytes)
    //            doSaveNodes(storage, buf)
    //            buf.clear()
    //            size = 0
    //          }
    //      }
    //      doSaveNodes(storage, buf)
    //      buf.clear()
    //    }
    //
    //    private def doSaveNodes(storage: NodeKeyValueStorage, buf: mutable.HashMap[Hash, Array[Byte]]) {
    //      storage.update(Set(), buf.toMap)
    //    }

    private def reportStatus() {
      val duration = (System.nanoTime - prevReportTime) / 1000000000.0
      val nPendingNodes = syncState.pendingMptNodes.size + syncState.pendingNonMptNodes.size
      val nWorkingNodes = syncState.workingMptNodes.size + syncState.workingNonMptNodes.size
      val nTotalNodes = syncState.downloadedNodesCount + nPendingNodes + nWorkingNodes
      val blockRate = ((currSyncedBlockNumber - prevSyncedBlockNumber) / duration).toInt
      val stateRate = ((syncState.downloadedNodesCount - prevDownloadeNodes) / duration).toInt
      val goodPeers = peersToDownloadFrom
      val nodeOkPeers = goodPeers -- blockchainOnlyPeers.values.toSet
      val nHeaderPeers = headerWhitePeers.size
      val nBlackPeers = handshakedPeers.size - goodPeers.size
      log.info(
        s"""|[fast] Block: ${currSyncedBlockNumber}/${syncState.targetBlockNumber}, $blockRate/s.
            |State: ${syncState.downloadedNodesCount}/$nTotalNodes, $stateRate/s.
            |Peers: (in/out) (${incomingPeers.size}/${outgoingPeers.size}), (working/good/header/node/black) (${workingPeers.size}/${goodPeers.size}/${nHeaderPeers}/${nodeOkPeers.size}/${nBlackPeers})
            |""".stripMargin.replace("\n", " ")
      )

      prevReportTime = System.nanoTime
      prevSyncedBlockNumber = currSyncedBlockNumber
      prevDownloadeNodes = syncState.downloadedNodesCount

      timers.startSingleTimer(ReportStatusTask, ReportStatusTick, reportStatusInterval)
    }

  }
}
