package khipu.blockchain.sync

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.PoisonPill
import akka.actor.Props
import akka.pattern.AskTimeoutException
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.ByteString
import java.math.BigInteger
import java.util.concurrent.ThreadLocalRandom
import khipu.Hash
import khipu.blockchain.sync
import khipu.blockchain.sync.HandshakedPeersService.BlacklistPeer
import khipu.crypto
import khipu.domain.BlockHeader
import khipu.domain.Blockchain
import khipu.domain.Receipt
import khipu.network.handshake.EtcHandshake.PeerInfo
import khipu.network.p2p.messages.PV62
import khipu.network.p2p.messages.CommonMessages.Status
import khipu.network.rlpx.Peer
import khipu.network.rlpx.RLPxStage
import khipu.store.AppStateStorage
import khipu.store.FastSyncStateStorage
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
  case object PersistSyncState

  final case class SyncState(
    var targetBlockNumber:     Long,
    var downloadedNodesCount:  Int                                     = 0,
    var bestBlockHeaderNumber: Long                                    = 0,
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
  final case class BlockBodiesWork(hashes: List[Hash], syncedBlockNumber: Option[Long]) extends Work
  final case class ReceiptsWork(hashes: List[Hash]) extends Work
  final case class NodesWork(hashes: List[NodeHash], downloadedCount: Option[Int]) extends Work

  final case class PeerWorkDone(peerId: String, work: Work)
  final case class HeaderWorkDone(peerId: String)

  final case class MarkPeerBlockchainOnly(peer: Peer)

  sealed trait BlockBodyValidationResult
  case object Valid extends BlockBodyValidationResult
  case object Invalid extends BlockBodyValidationResult
  case object DbError extends BlockBodyValidationResult

  // always enqueue hashes at the head, this will get shorter pending queue !!!
  // thus we use List hashes here 
  final case class EnqueueNodes(remainingHashes: List[NodeHash])
  final case class EnqueueBlockBodies(remainingHashes: List[Hash])
  final case class EnqueueReceipts(remainingHashes: List[Hash])

  final case class SaveDifficulties(kvs: Map[Hash, BigInteger])
  final case class SaveHeaders(kvs: Map[Hash, BlockHeader])
  final case class SaveBodies(kvs: Map[Hash, PV62.BlockBody], receivedHashes: List[Hash])
  final case class SaveReceipts(kvs: Map[Hash, Seq[Receipt]], receivedHashes: List[Hash])
  final case class SaveAccountNodes(kvs: Map[Hash, Array[Byte]])
  final case class SaveStorageNodes(kvs: Map[Hash, Array[Byte]])
  final case class SaveEvmcodes(kvs: Map[Hash, ByteString])
}
trait FastSyncService { _: SyncService =>
  import context.dispatcher
  import khipu.util.Config.Sync._
  import FastSyncService._

  private implicit val fastSyncTimeout = RLPxStage.decodeTimeout.plus(20.seconds)

  protected def startFastSync() {
    log.info("Trying to start block synchronization (fast mode)")
    fastSyncStateStorage.getSyncState match {
      case Some(syncState) => startFastSync(syncState)
      case None            => startFastSyncFromScratch()
    }
  }

  private def startFastSync(syncState: SyncState) {
    log.info("Start fast synchronization")
    context become ((new SyncingHandler(syncState).receive) orElse peerUpdateBehavior orElse ommersBehavior)
    self ! ProcessSyncingTick
  }

  private def startingFastSync: Receive = peerUpdateBehavior orElse ommersBehavior orElse stopBehavior orElse {
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
      context become startingFastSync
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
            val (goodPeers, blockHeaders) = peerToBlockHeader.unzip

            val peers = receivedHeaders.map(_._1)
            val badPeers = peers filterNot goodPeers.contains
            badPeers foreach { peer =>
              self ! BlacklistPeer(peer.id, s"Got uncertain block header", always = true)
            }

            val targetBlockHeader = blockHeaders.head
            log.info(s"Got enough block headers that have the same stateRoot, starting block synchronization (fast mode). Target block ${targetBlockHeader}")
            val initialSyncState = SyncState(
              targetBlockHeader.number,
              pendingMptNodes = List(StateMptNodeHash(targetBlockHeader.stateRoot.bytes))
            )
            startFastSync(initialSyncState)
          } else {
            log.info(s"Could not get enough block headers that have the same stateRoot, requires ${nSameHeadersRequired}, but only found ${peerToBlockHeader.size}")
            scheduleStartRetry(startRetryInterval)
            context become startingFastSync
          }
        }
      }

    } else {
      log.info(s"Block synchronization (fast mode) does not started. Need to receive block headers from at least $minPeersToChooseTargetBlock peers, but received only from ${receivedHeaders.size}. Retrying in ${startRetryInterval.toSeconds} seconds")
      scheduleStartRetry(startRetryInterval)
      context become startingFastSync
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

  private class SyncingHandler(syncState: SyncState) {

    private var workingPeers = Set[String]()

    private var headerWorkingPeer: Option[String] = None
    private var blockchainOnlyPeers = Map[String, Peer]()

    private val heartbeatTask = context.system.scheduler.schedule(syncRetryInterval, syncRetryInterval, self, ProcessSyncingTick)
    private val syncStatePersistTask = context.system.scheduler.schedule(persistStateSnapshotInterval, persistStateSnapshotInterval, self, PersistSyncState)

    private val syncStatePersist = context.actorOf(FastSyncStatePersist.props(fastSyncStateStorage, syncState), "state-persistor")
    private val persistenceService = context.actorOf(PersistenceService.props(blockchain, appStateStorage), "persistence-service")

    private var currSyncedBlockNumber = appStateStorage.getBestBlockNumber
    private var prevSyncedBlockNumber = currSyncedBlockNumber
    private var prevDownloadeNodes = syncState.downloadedNodesCount
    private var prevReportTime = System.currentTimeMillis

    def receive: Receive = peerUpdateBehavior orElse ommersBehavior orElse {
      // always enqueue hashes in the front, this will get shorter pending queue !!!
      case EnqueueNodes(remainingHashes) =>
        remainingHashes foreach {
          case h: EvmcodeHash                => syncState.pendingNonMptNodes = h :: syncState.pendingNonMptNodes
          case h: StorageRootHash            => syncState.pendingNonMptNodes = h :: syncState.pendingNonMptNodes
          case h: StateMptNodeHash           => syncState.pendingMptNodes = h :: syncState.pendingMptNodes
          case h: ContractStorageMptNodeHash => syncState.pendingMptNodes = h :: syncState.pendingMptNodes
        }

      case EnqueueBlockBodies(remainingHashes) =>
        syncState.pendingBlockBodies = remainingHashes ::: syncState.pendingBlockBodies

      case EnqueueReceipts(remaningHashes) =>
        syncState.pendingReceipts = remaningHashes ::: syncState.pendingReceipts

      case MarkPeerBlockchainOnly(peer) =>
        if (!blockchainOnlyPeers.contains(peer.id)) {
          blockchainOnlyPeers = blockchainOnlyPeers.take(blockchainOnlyPeersPoolSize) + (peer.id -> peer)
        }

      case ProcessSyncingTick =>
        processSyncing()

      case PeerWorkDone(peerId, work) =>
        workingPeers -= peerId

        work match {
          case BlockBodiesWork(hashes, syncedBlockNumber) =>
            syncState.workingBlockBodies --= hashes
            syncedBlockNumber foreach (currSyncedBlockNumber = _)

          case ReceiptsWork(hashes) =>
            syncState.workingReceipts --= hashes

          case NodesWork(hashes, downloadedCount) =>
            hashes foreach {
              case h: EvmcodeHash                => syncState.workingNonMptNodes -= h
              case h: StorageRootHash            => syncState.workingNonMptNodes -= h
              case h: StateMptNodeHash           => syncState.workingMptNodes -= h
              case h: ContractStorageMptNodeHash => syncState.workingMptNodes -= h
            }
            downloadedCount foreach (syncState.downloadedNodesCount += _)
        }

        processSyncing()

      case HeaderWorkDone(peerId) =>
        headerWorkingPeer = None
        workingPeers -= peerId
        processSyncing()

      case SyncService.ReportStatusTick =>
        reportStatus()

      case PersistSyncState =>
        syncStatePersist ! PersistSyncState
    }

    def processSyncing() {
      if (isFullySynced) {
        // TODO check isFullySynced is not enough, since saving blockbodies and appStateStorage are async 
        reportStatus
        val bestBlockNumber = appStateStorage.getBestBlockNumber
        if (bestBlockNumber == syncState.targetBlockNumber) {
          log.info("s[fast] Block synchronization in fast mode finished, switching to regular mode")
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
      heartbeatTask.cancel()
      syncStatePersistTask.cancel()
      syncStatePersist ! SyncService.FastSyncDone
      syncStatePersist ! PoisonPill
      persistenceService ! PoisonPill
      fastSyncStateStorage.purge()

      appStateStorage.fastSyncDone()
      context become idle
      blockchainOnlyPeers = Map()
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

        if (syncState.pendingNonMptNodes.nonEmpty || syncState.pendingMptNodes.nonEmpty) {
          val blockchainOnlys = blockchainOnlyPeers.values.toSet
          val nodeWorks = unassignedPeers.filterNot(blockchainOnlys.contains)
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

                  workingPeers += peer.id

                  acc :+ (peer, requestingNonMptNodes ::: requestingMptNodes)
                } else {
                  acc
                }
            }

          nodeWorks foreach { case (peer, requestingNodes) => requestNodes(peer, requestingNodes) }
        }

        if (syncState.pendingReceipts.nonEmpty) {
          val receiptWorks = unassignedPeers
            .take(maxConcurrentRequests - workingPeers.size)
            .foldLeft(Vector[(Peer, List[Hash])]()) {
              case (acc, peer) =>
                if (syncState.pendingReceipts.nonEmpty) {
                  val (requestingReceipts, remainingReceipts) = syncState.pendingReceipts.splitAt(receiptsPerRequest)

                  syncState.pendingReceipts = remainingReceipts
                  syncState.workingReceipts ++= requestingReceipts.map(x => (x -> null))

                  workingPeers += peer.id

                  acc :+ (peer, requestingReceipts)
                } else {
                  acc
                }
            }

          receiptWorks foreach { case (peer, requestingReceipts) => requestReceipts(peer, requestingReceipts) }
        }

        if (syncState.pendingBlockBodies.nonEmpty) {
          val bodyWorks = unassignedPeers
            .take(maxConcurrentRequests - workingPeers.size)
            .foldLeft(Vector[(Peer, List[Hash])]()) {
              case (acc, peer) =>
                if (syncState.pendingBlockBodies.nonEmpty) {
                  val (requestingHashes, remainingHashes) = syncState.pendingBlockBodies.splitAt(blockBodiesPerRequest)

                  syncState.pendingBlockBodies = remainingHashes
                  syncState.workingBlockBodies ++= requestingHashes.map(x => (x -> null))

                  workingPeers += peer.id

                  acc :+ (peer, requestingHashes)
                } else {
                  acc
                }
            }

          bodyWorks foreach { case (peer, requestingHashes) => requestBlockBodies(peer, requestingHashes) }
        }

        if (isThereHeaderToDownload && headerWorkingPeer.isEmpty) {
          val candicates = unassignedPeers
            .take(maxConcurrentRequests - workingPeers.size)

          val headerWorks = nextPeer(candicates) flatMap { peer =>
            if (isThereHeaderToDownload && headerWorkingPeer.isEmpty) {
              headerWorkingPeer = Some(peer.id)
              workingPeers += peer.id

              Some(peer)
            } else {
              None
            }
          }

          headerWorks foreach { requestBlockHeaders }
        }
      }
    }

    private def nextPeer(candicates: Seq[Peer]): Option[Peer] = {
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
        case (peer, PeerInfo(_, totalDifficulty, true, _)) if !workingPeers.contains(peer.id) => (peer, totalDifficulty)
      }
      peerToUse.toList.sortBy { _._2.negate } map (_._1)
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
      log.debug(s"Request block headers from ${peer.id}")
      val start = System.currentTimeMillis

      val limit = math.min(blockHeadersPerRequest, syncState.targetBlockNumber - syncState.bestBlockHeaderNumber)
      //log.debug(s"Request block headers: ${request.message.block.fold(n => n, _.hexString)}, bestBlockHeaderNumber is ${syncState.bestBlockHeaderNumber}, target is ${syncState.targetBlockNumber}")
      blockchain.getBlockHeaderByNumber(syncState.bestBlockHeaderNumber) match {
        case Some(parentHeader) =>
          requestingHeaders(peer, Some(parentHeader), Left(syncState.bestBlockHeaderNumber + 1), limit, skip = 0, reverse = false) andThen {
            case Success(Some(BlockHeadersResponse(peerId, headers, true))) =>
              log.debug(s"Got block headers ${headers.size} from ${peer.id} in ${System.currentTimeMillis - start}ms")
              val blockHeadersObtained = insertHeaders(headers)
              val blockHashes = blockHeadersObtained.map(_.hash)
              self ! EnqueueBlockBodies(blockHashes)
              self ! EnqueueReceipts(blockHashes)

            case Success(Some(BlockHeadersResponse(peerId, List(), false))) =>
              self ! BlacklistPeer(peer.id, s"Got block headers non-consistent for requested: ${syncState.bestBlockHeaderNumber + 1}")

            case Success(None) =>
              self ! BlacklistPeer(peer.id, s"Got block headers empty for known header: ${syncState.bestBlockHeaderNumber + 1}")

            case Failure(e: AskTimeoutException) =>
              self ! BlacklistPeer(peer.id, s"${e.getMessage}")
            case Failure(e) =>
              self ! BlacklistPeer(peer.id, s"${e.getMessage}")
          } andThen {
            case _ => self ! HeaderWorkDone(peer.id)
          }
        case None => // TODO
          log.error(s"previous best block ${syncState.bestBlockHeaderNumber} does not exist yet, something wrong !!!")
      }
    }

    def requestBlockBodies(peer: Peer, requestingHashes: List[Hash]) {
      log.debug(s"Request block bodies from ${peer.id}")
      val start = System.currentTimeMillis

      requestingBodies(peer, requestingHashes) andThen {
        case Success(Some(BlockBodiesResponse(peerId, remainingHashes, receivedHashes, bodies))) =>
          log.debug(s"Got block bodies ${bodies.size} from ${peer.id} in ${System.currentTimeMillis - start}ms")
          validateBlocks(receivedHashes, bodies) match {
            case Valid =>
              self ! EnqueueBlockBodies(remainingHashes)
              (persistenceService ? SaveBodies((receivedHashes zip bodies).toMap, receivedHashes))(20.seconds).mapTo[Option[Long]] andThen {
                case Success(syncedBlockNumber) =>
                  log.debug(s"SaveBodies success with syncedBlockNumber=$syncedBlockNumber")
                  self ! PeerWorkDone(peer.id, BlockBodiesWork(requestingHashes, syncedBlockNumber))
                case Failure(e) =>
                  log.error(e, e.getMessage)
                  self ! PeerWorkDone(peer.id, BlockBodiesWork(requestingHashes, None))
              }

            case Invalid =>
              self ! EnqueueBlockBodies(requestingHashes)
              self ! BlacklistPeer(peerId, s"$peerId responded with invalid block bodies that are not matching block headers")
              self ! PeerWorkDone(peer.id, BlockBodiesWork(requestingHashes, None))

            case DbError =>
              log.error("DbError")
              syncState.pendingBlockBodies = List()
              syncState.pendingReceipts = List()
              //todo adjust the formula to minimize redownloaded block headers
              syncState.bestBlockHeaderNumber = syncState.bestBlockHeaderNumber - 2 * blockHeadersPerRequest
              log.warning("missing block header for known hash")
              self ! PeerWorkDone(peer.id, BlockBodiesWork(requestingHashes, None))
          }

        case Success(None) =>
          self ! BlacklistPeer(peer.id, s"Got block bodies empty response for known hashes ${peer.id}")
          self ! EnqueueBlockBodies(requestingHashes)
          self ! PeerWorkDone(peer.id, BlockBodiesWork(requestingHashes, None))

        case Failure(e: AskTimeoutException) =>
          self ! EnqueueBlockBodies(requestingHashes)
          self ! BlacklistPeer(peer.id, s"${e.getMessage}")
          self ! PeerWorkDone(peer.id, BlockBodiesWork(requestingHashes, None))

        case Failure(e) =>
          self ! EnqueueBlockBodies(requestingHashes)
          self ! BlacklistPeer(peer.id, s"${e.getMessage}")
          self ! PeerWorkDone(peer.id, BlockBodiesWork(requestingHashes, None))
      }
    }

    def requestReceipts(peer: Peer, requestingHashes: List[Hash]) {
      log.debug(s"Request receipts from ${peer.id}")
      val start = System.currentTimeMillis
      requestingReceipts(peer, requestingHashes) andThen {
        case Success(Some(ReceiptsResponse(peerId, remainingHashes, receivedHashes, receipts))) =>
          log.debug(s"Got receipts ${receipts.size} from ${peer.id} in ${System.currentTimeMillis - start}ms")

          // TODO valid receipts
          persistenceService ! SaveReceipts((receivedHashes zip receipts).toMap, receivedHashes)

          self ! EnqueueReceipts(remainingHashes)
          self ! PeerWorkDone(peer.id, ReceiptsWork(requestingHashes))

        case Success(None) =>
          self ! EnqueueReceipts(requestingHashes)
          self ! BlacklistPeer(peer.id, s"Got receipts empty for known hashes ${peer.id}")
          self ! PeerWorkDone(peer.id, ReceiptsWork(requestingHashes))

        case Failure(e: AskTimeoutException) =>
          self ! EnqueueReceipts(requestingHashes)
          self ! BlacklistPeer(peer.id, s"${e.getMessage}")
          self ! PeerWorkDone(peer.id, ReceiptsWork(requestingHashes))

        case Failure(e) =>
          self ! EnqueueReceipts(requestingHashes)
          self ! BlacklistPeer(peer.id, s"${e.getMessage}")
          self ! PeerWorkDone(peer.id, ReceiptsWork(requestingHashes))
      }
    }

    def requestNodes(peer: Peer, requestingHashes: List[NodeHash]) {
      log.debug(s"Request nodes from ${peer.id}")
      val start = System.currentTimeMillis

      requestingNodeDatas(peer, requestingHashes) andThen {
        case Success(Some(NodeDatasResponse(peerId, nDownloadedNodes, remainingHashes, childrenHashes, accounts, storages, evmcodes))) =>
          log.debug(s"Got nodes $nDownloadedNodes from ${peer.id} in ${System.currentTimeMillis - start}ms")

          persistenceService ! SaveAccountNodes(accounts.map(kv => (kv._1, kv._2.toArray)).toMap)
          persistenceService ! SaveStorageNodes(storages.map(kv => (kv._1, kv._2.toArray)).toMap)
          persistenceService ! SaveEvmcodes(evmcodes.toMap)

          self ! EnqueueNodes(remainingHashes ++ childrenHashes)
          self ! PeerWorkDone(peer.id, NodesWork(requestingHashes, Some(nDownloadedNodes)))

        case Success(None) =>
          log.debug(s"Got nodes empty response for known hashes. Mark ${peer.id} blockchain only")
          self ! EnqueueNodes(requestingHashes)
          self ! MarkPeerBlockchainOnly(peer)
          self ! PeerWorkDone(peer.id, NodesWork(requestingHashes, None))

        case Failure(e: AskTimeoutException) =>
          log.debug(s"Got node $e when request nodes. Mark ${peer.id} blockchain only")
          self ! EnqueueNodes(requestingHashes)
          self ! MarkPeerBlockchainOnly(peer)
          self ! PeerWorkDone(peer.id, NodesWork(requestingHashes, None))

        case Failure(e) =>
          log.debug(s"Got node $e when request nodes. Mark ${peer.id} blockchain only")
          self ! EnqueueNodes(requestingHashes)
          self ! MarkPeerBlockchainOnly(peer)
          self ! PeerWorkDone(peer.id, NodesWork(requestingHashes, None))
      }
    }

    private def validateBlocks(receviedHashes: Seq[Hash], blockBodies: Seq[PV62.BlockBody]): BlockBodyValidationResult = {
      val headerToBody = (receviedHashes zip blockBodies).map {
        case (hash, body) => (blockchain.getBlockHeaderByHash(hash), body)
      }

      headerToBody.collectFirst {
        case (None, _) => DbError
        case (Some(header), body) if validators.blockValidator.validateHeaderAndBody(header, body).isLeft => Invalid
      } getOrElse (Valid)
    }

    private def insertHeaders(headers: List[BlockHeader]): List[BlockHeader] = {
      val blockHeadersObtained = headers.takeWhile { header =>
        blockchain.getTotalDifficultyByHash(header.parentHash) match {
          case Some(parentTotalDifficulty) =>
            // header is fetched and saved sequentially. TODO better logic
            blockchain.saveBlockHeader(header)
            blockchain.saveTotalDifficulty(header.hash, parentTotalDifficulty add header.difficulty)
            true
          case None =>
            false
        }
      }

      blockHeadersObtained.lastOption.foreach { lastHeader =>
        if (lastHeader.number > syncState.bestBlockHeaderNumber) {
          syncState.bestBlockHeaderNumber = lastHeader.number
        }
      }

      blockHeadersObtained
    }

    private def reportStatus() {
      val duration = (System.currentTimeMillis - prevReportTime) / 1000.0
      val nPendingNodes = syncState.pendingMptNodes.size + syncState.pendingNonMptNodes.size
      val nWorkingNodes = syncState.workingMptNodes.size + syncState.workingNonMptNodes.size
      val nTotalNodes = syncState.downloadedNodesCount + nPendingNodes + nWorkingNodes
      val blockRate = ((currSyncedBlockNumber - prevSyncedBlockNumber) / duration).toInt
      val stateRate = ((syncState.downloadedNodesCount - prevDownloadeNodes) / duration).toInt
      val goodPeers = peersToDownloadFrom
      val nodeOkPeers = goodPeers -- blockchainOnlyPeers.values.toSet
      val nBlackPeers = handshakedPeers.size - goodPeers.size
      prevReportTime = System.currentTimeMillis
      prevSyncedBlockNumber = currSyncedBlockNumber
      prevDownloadeNodes = syncState.downloadedNodesCount
      log.info(
        s"""|[fast] Block: ${currSyncedBlockNumber}/${syncState.targetBlockNumber}, $blockRate/s.
            |State: ${syncState.downloadedNodesCount}/$nTotalNodes, $stateRate/s, $nWorkingNodes in working.
            |Peers: (in/out) (${incomingPeers.size}/${outgoingPeers.size}), (working/good/node/black) (${workingPeers.size}/${goodPeers.size}/${nodeOkPeers.size}/${nBlackPeers})
            |""".stripMargin.replace("\n", " ")
      )
    }

  }
}

object FastSyncStatePersist {
  def props(storage: FastSyncStateStorage, syncState: FastSyncService.SyncState) = Props(classOf[FastSyncStatePersist], storage, syncState)
}
/**
 * Persists current state of fast sync to a storage. Can save only one state at a time.
 * If during persisting new state is received then it will be saved immediately after current state
 * was persisted.
 * If during persisting more than one new state is received then only the last state will be kept in queue.
 */
class FastSyncStatePersist(storage: FastSyncStateStorage, syncState: FastSyncService.SyncState) extends Actor with ActorLogging {
  import FastSyncStatePersist._
  import context.dispatcher

  private var isFastSyncAlreadyDone = false

  override def postStop() {
    super.postStop()
    // persist in the end
    persistSyncState(true)
    log.info("[fast] FastSyncStatePersist stopped")
  }

  def receive: Receive = {
    case FastSyncService.PersistSyncState =>
      persistSyncState(false)

    case SyncService.FastSyncDone =>
      isFastSyncAlreadyDone = true
  }

  private def persistSyncState(isShutdown: Boolean) {
    val start = System.currentTimeMillis
    try {
      storage.putSyncState(syncState)
    } catch {
      case e: Throwable =>
        log.error(e.getMessage, e)
    }
    log.info(s"[fast] Saved sync state in ${System.currentTimeMillis - start} ms, best header number: ${syncState.bestBlockHeaderNumber}, bodies queue: ${syncState.pendingBlockBodies.size + syncState.workingBlockBodies.size}, receipts queue: ${syncState.pendingReceipts.size + syncState.workingReceipts.size}, nodes queue: ${syncState.pendingMptNodes.size + syncState.pendingNonMptNodes.size + syncState.workingMptNodes.size + syncState.workingNonMptNodes.size}, downloaded nodes: ${syncState.downloadedNodesCount} ")
  }
}

object PersistenceService {
  def props(blockchain: Blockchain, appStateStorage: AppStateStorage) =
    Props(classOf[PersistenceService], blockchain, appStateStorage: AppStateStorage).withDispatcher("khipu-persistence-pinned-dispatcher")
}
class PersistenceService(blockchain: Blockchain, appStateStorage: AppStateStorage) extends Actor with ActorLogging {
  import FastSyncService._

  private val blockchainStorages = blockchain.storages
  private val accountNodeStorage = blockchainStorages.accountNodeStorageFor(None)
  private val storageNodeStorage = blockchainStorages.storageNodeStorageFor(None)

  override def preStart() {
    super.preStart()
    log.info("PersistenceService started")
  }

  override def postStop() {
    log.info("PersistenceService stopped")
    super.postStop()
  }

  def receive: Receive = {
    case SaveDifficulties(kvs) =>
      val start = System.currentTimeMillis
      kvs foreach { case (k, v) => blockchain.saveTotalDifficulty(k, v) }
      log.debug(s"SaveDifficulties ${kvs.size} in ${System.currentTimeMillis - start} ms")

    case SaveHeaders(kvs) =>
      val start = System.currentTimeMillis
      kvs foreach { case (k, v) => blockchain.saveBlockHeader(v) }
      log.debug(s"SaveHeaders ${kvs.size} in ${System.currentTimeMillis - start} ms")

    case SaveBodies(kvs, receivedHashes) =>
      val start = System.currentTimeMillis
      kvs foreach { case (k, v) => blockchain.saveBlockBody(k, v) }
      log.debug(s"SaveBodies ${kvs.size} in ${System.currentTimeMillis - start} ms")
      sender() ! updateBestBlockIfNeeded(receivedHashes)

    case SaveReceipts(kvs, receivedHashes) =>
      val start = System.currentTimeMillis
      kvs foreach { case (k, v) => blockchain.saveReceipts(k, v) }
      log.debug(s"SaveReceipts ${kvs.size} in ${System.currentTimeMillis - start} ms")

    case SaveAccountNodes(kvs) =>
      val start = System.currentTimeMillis
      kvs map { case (k, v) => }
      accountNodeStorage.update(Set(), kvs)
      log.debug(s"SaveAccountNodes ${kvs.size} in ${System.currentTimeMillis - start} ms")

    case SaveStorageNodes(kvs) =>
      val start = System.currentTimeMillis
      storageNodeStorage.update(Set(), kvs)
      log.debug(s"SaveStorageNodes ${kvs.size} in ${System.currentTimeMillis - start} ms")

    case SaveEvmcodes(kvs) =>
      val start = System.currentTimeMillis
      kvs foreach { case (k, v) => blockchain.saveEvmcode(k, v) }
      log.debug(s"SaveEvmcodes ${kvs.size} in ${System.currentTimeMillis - start} ms")
  }

  private def updateBestBlockIfNeeded(receivedBodyHashes: Seq[Hash]): Option[Long] = {
    val blockNumbers = receivedBodyHashes flatMap { blockHash =>
      blockchain.getBlockHeaderByHash(blockHash) map (_.number)
    }

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

}
