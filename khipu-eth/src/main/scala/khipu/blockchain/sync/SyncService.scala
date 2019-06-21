package khipu.blockchain.sync

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Timers
import akka.cluster.client.ClusterClientReceptionist
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.cluster.singleton.ClusterSingletonProxy
import akka.cluster.singleton.ClusterSingletonProxySettings
import akka.pattern.ask
import khipu.Hash
import khipu.Stop
import khipu.crypto
import khipu.domain.Block
import khipu.domain.BlockHeader
import khipu.network.p2p.Message
import khipu.network.p2p.messages.PV62
import khipu.network.p2p.messages.PV63
import khipu.network.rlpx.Peer
import khipu.ommers.OmmersPool
import khipu.service.ServiceBoard
import khipu.transactions.PendingTransactionsService
import khipu.util.Config
import scala.concurrent.duration._

object SyncService {
  def props() = Props(classOf[SyncService]).withDispatcher("khipu-sync-pinned-dispatcher")

  val name = "syncService"
  val managerName = "khipuSingleton-" + name
  val managerPath = "/user/" + managerName
  val path = managerPath + "/" + name
  val proxyName = "khipuSingletonProxy-" + name
  val proxyPath = "/user/" + proxyName

  def start(system: ActorSystem, role: Option[String]): ActorRef = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role).withSingletonName(name)
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(),
        terminationMessage = PoisonPill,
        settings = settings
      ), name = managerName
    )
  }

  def startProxy(system: ActorSystem, role: Option[String]): ActorRef = {
    val settings = ClusterSingletonProxySettings(system).withRole(role).withSingletonName(name)
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = managerPath,
        settings = settings
      ), name = proxyName
    )
    ClusterClientReceptionist(system).registerService(proxy)
    proxy
  }

  def proxy(system: ActorSystem) = system.actorSelection(proxyPath)

  final case class ReceivedMessage(peerId: String, response: Message)

  final case class MinedBlock(block: Block)

  case object StartSync
  case object FastSyncDone
}
class SyncService() extends FastSyncService with RegularSyncService with HandshakedPeersService with Actor with Timers with ActorLogging {
  import SyncService._
  import context.dispatcher

  protected val serviceBoard = ServiceBoard(context.system)

  protected val ledger = serviceBoard.ledger
  protected val validators = serviceBoard.validators
  protected val blockchain = serviceBoard.blockchain
  protected val peerConfiguration = serviceBoard.peerConfiguration
  private val miningConfig = serviceBoard.miningConfig

  protected val fastSyncStateStorage = serviceBoard.storages.fastSyncStateStorage
  protected val appStateStorage = serviceBoard.storages.appStateStorage
  protected val blockchainStorages = blockchain.storages
  protected val accountNodeStorage = blockchainStorages.accountNodeStorageFor(Some(0))
  protected val storageNodeStorage = blockchainStorages.storageNodeStorageFor(Some(0))

  protected val mediator = DistributedPubSub(context.system).mediator

  protected var isRequesting = false

  // This should be cluster single instance only, instant it in SyncService
  val ommersPool = context.actorOf(OmmersPool.props(blockchain, miningConfig), "ommersPool")

  protected def peerManager = serviceBoard.peerManage
  protected def pendingTransactionsService = PendingTransactionsService.proxy(context.system)

  override def postStop() {
    super.postStop()

    if (!appStateStorage.isFastSyncDone) {
      saveSyncState()
    }

    serviceBoard.storages.closeAll()

    log.info("SyncService stopped")
  }

  override def receive: Receive = idle

  def idle: Receive = peerUpdateBehavior orElse {
    case StartSync =>
      appStateStorage.putSyncStartingBlock(appStateStorage.getBestBlockNumber)
      (appStateStorage.isFastSyncDone, Config.Sync.doFastSync) match {
        case (false, true) =>
          startFastSync()

        case (true, true) =>
          log.debug(s"do-fast-sync is set to ${Config.Sync.doFastSync} but fast sync won't start because it already completed")
          startRegularSync()

        case (true, false) =>
          startRegularSync()

        case (false, false) =>
          fastSyncStateStorage.purge()
          startRegularSync()
      }

    case FastSyncDone =>
      startRegularSync()
  }

  def ommersBehavior: Receive = {
    case x: OmmersPool.GetOmmers =>
      ommersPool forward x
  }

  // Not used yet. The shutdown hook is implemented in Khipu.scala
  def stopBehavior: Receive = {
    case Stop => context stop self
  }

  protected def requestingHeaders(peer: Peer, parentHeader: Option[BlockHeader], blockNumberOrHash: Either[Long, Hash], maxHeaders: Long, skip: Long, reverse: Boolean)(implicit timeout: FiniteDuration) = {
    isRequesting = true
    val request = BlockHeadersRequest(peer.id, parentHeader, PV62.GetBlockHeaders(blockNumberOrHash, maxHeaders, skip, reverse))
    (peer.entity ? request)(timeout).mapTo[Option[BlockHeadersResponse]] andThen {
      case _ => isRequesting = false
    }
  }

  protected def requestingBodies(peer: Peer, hashes: Seq[Hash])(implicit timeout: FiniteDuration) = {
    isRequesting = true
    val request = BlockBodiesRequest(peer.id, PV62.GetBlockBodies(hashes))
    (peer.entity ? request)(timeout).mapTo[Option[BlockBodiesResponse]] andThen {
      case _ => isRequesting = false
    }
  }

  protected def requestingReceipts(peer: Peer, hashes: Seq[Hash])(implicit timeout: FiniteDuration) = {
    isRequesting = true
    val request = ReceiptsRequest(peer.id, PV63.GetReceipts(hashes))
    (peer.entity ? request)(timeout).mapTo[Option[ReceiptsResponse]] andThen {
      case _ => isRequesting = false
    }
  }

  protected def requestingNodeDatas(peer: Peer, hashes: List[NodeHash])(implicit timeout: FiniteDuration) = {
    isRequesting = true
    val request = NodeDatasRequest(peer.id, PV63.GetNodeData(hashes.map(_.toHash)), hashes)
    (peer.entity ? request)(timeout).mapTo[Option[NodeDatasResponse]] andThen {
      case _ => isRequesting = false
    }
  }
  protected def requestingNodeData(peer: Peer, hash: Hash)(implicit timeout: FiniteDuration) = {
    isRequesting = true
    val request = NodeDataRequest(peer.id, PV63.GetNodeData(List(hash)), hash)
    (peer.entity ? request)(timeout).mapTo[Option[NodeDataResponse]] andThen {
      case _ => isRequesting = false
    }
  }

}
