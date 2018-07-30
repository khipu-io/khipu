package khipu.network.rlpx

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Timers
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import java.net.URI
import khipu.BroadcastNewBlocks
import khipu.BroadcastTransactions
import khipu.ProcessedTransactions
import khipu.Command
import khipu.Hash
import khipu.blockchain.sync.BlockHeadersRequest
import khipu.blockchain.sync.BlockBodiesRequest
import khipu.blockchain.sync.NodeDataRequest
import khipu.blockchain.sync.NodeDatasRequest
import khipu.blockchain.sync.ReceiptsRequest
import khipu.blockchain.sync.PeerResponse
import khipu.blockchain.sync.RequestToPeer
import khipu.blockchain.sync.SyncService
import khipu.crypto
import khipu.domain.SignedTransaction
import khipu.network.handshake.EtcHandshake.PeerInfo
import khipu.network.p2p.Message
import khipu.network.p2p.MessageSerializable
import khipu.network.p2p.messages.CommonMessages.NewBlock
import khipu.network.p2p.messages.CommonMessages.SignedTransactions
import khipu.network.p2p.messages.PV62
import khipu.network.p2p.messages.PV63
import khipu.service.ServiceBoard
import khipu.transactions.PendingTransactionsService
import scala.collection.mutable
import scala.concurrent.duration._

/**
 * Peer is node which is currently connected to host node.
 *
 * We won't design PeerEntity as cluster sharding actors, since it's better to
 * keep it on the same host which received correspondign TCP connection
 *
 * To make sure that peerEntity is at the same host of connection, always create
 * it from the TCP stage
 */
object PeerEntity {
  def props(peer: Peer) = Props(classOf[PeerEntity], peer)

  final case class ConnectTo(uri: URI)

  private case object RetryConnectionTimeout
  private case object ResponseTimeout

  case object GetStatus
  final case class StatusResponse(status: Status)
  final case class DisconnectPeer(reason: Int)

  sealed trait Status
  object Status {
    case object Idle extends Status
    case object Connecting extends Status
    final case class Handshaking(numRetries: Int) extends Status
    case object Handshaked extends Status
    case object Disconnected extends Status
  }

  final case class FetchMessageToPeer(peerId: String) extends Command { def id = peerId }
  final case class MessageToPeer(peerId: String, message: MessageSerializable) extends Command { def id = peerId }
  final case class MessageFromPeer(peerId: String, message: Message)

  final case class PeerEntityCreated(peer: Peer) extends Command { def id = peer.id }
  final case class PeerHandshaked(peer: Peer, peerInfo: PeerInfo) extends Command { def id = peer.id }
  final case class PeerDisconnected(peerId: String) extends Command { def id = peerId }
  final case class PeerInfoUpdated(peerId: String, peerInfo: PeerInfo) extends Command { def id = peerId }
  final case class PeerEntityStopped(peerId: String) extends Command { def id = peerId }

  private case object RequestTimeoutTask
  private case object RequestTimeoutTick
}
class PeerEntity(peer: Peer) extends Actor with Timers with ActorLogging {
  import PeerEntity._

  private var pendingMessages = Vector[(ActorRef, Either[RequestToPeer[_ <: Message, _ <: PeerResponse], MessageSerializable])]()
  private var currRequest: Option[(ActorRef, RequestToPeer[_ <: Message, _ <: PeerResponse])] = None
  private var peerInfo: Option[PeerInfo] = None

  /**
   * stores information which tx hashes are "known" by which peers Seq[peerId]
   */
  private var knownTransactions = Set[Hash]()

  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe(khipu.TxTopic, self)
  mediator ! Subscribe(khipu.NewBlockTopic, self)

  val serviceBoard = ServiceBoard(context.system)
  def peerManager = serviceBoard.peerManage
  def syncService = SyncService.proxy(context.system)
  def pendingTransactionsService = PendingTransactionsService.proxy(context.system)

  private val requestTimeout = 180.seconds

  override def preStart() {
    super.preStart()
    peerManager ! PeerEntity.PeerEntityCreated(peer)
  }

  override def postStop() {
    log.debug(s"Peer stopped ${peer.id} ")
    peerManager ! PeerEntity.PeerEntityStopped(peer.id)
    super.postStop()
  }

  override def receive = messageBehavior orElse broadcastBehavior

  def broadcastBehavior: Receive = {
    case BroadcastNewBlocks(newBlocks) => // from khipu.NewBlockTopic
      for {
        pi <- peerInfo
        newBlock <- newBlocks if (shouldSendNewBlock(newBlock, pi))
      } {
        self ! PeerEntity.MessageToPeer(peer.id, newBlock)
      }

    case BroadcastTransactions(transactions) => // from khipu.TxTopic
      val transactionsNonKnown = transactions.filterNot(isTxKnown)
      if (transactionsNonKnown.nonEmpty) {
        // TODO broadcast it only after fast-sync done? otherwise seems will cause 
        // too busy to request/respond
        //self ! PeerEntity.MessageToPeer(peerId, SignedTransactions(transactionsNonKnown))
        setTxKnown(transactionsNonKnown)
      }

    case ProcessedTransactions(transactions) => // from khipu.TxTopic
      knownTransactions = knownTransactions.filterNot(transactions.map(_.hash).contains)

    case PeerHandshaked(peer, peerInfo) =>
      this.peerInfo = Some(peerInfo)

    case SubscribeAck(Subscribe(topic, None, `self`)) =>
      log.debug(s"Subscribed to $topic")
  }

  def messageBehavior: Receive = {
    case MessageToPeer(_, message) =>
      log.debug(s"Enqueue ${message.getClass.getName} at $peer")
      pendingMessages :+= (sender() -> Right(message))

      maybeUpdatePeerInfoByMessageToPeer(message) foreach { newPeerInfo =>
        log.debug(s"UpdatedPeerInfo: $peerInfo")
        peerInfo = Some(newPeerInfo)
        syncService ! PeerInfoUpdated(peer.id, newPeerInfo)
      }

    case request: RequestToPeer[_, _] =>
      log.debug(s"Enqueue ${request.getClass.getName} at $peer")
      pendingMessages :+= (sender() -> Left(request))

    case FetchMessageToPeer(_) =>
      pendingMessages match {
        case Vector() =>
          sender() ! None

        case Vector((commander, Left(request)), tail @ _*) =>
          // Here wa just drop off previous request, the commander should process timeout itself
          currRequest = Some(commander, request)
          pendingMessages = pendingMessages.tail
          sender() ! Some(MessageToPeer(peer.id, request.messageToSend))

        case Vector((_, Right(message)), tail @ _*) =>
          sender() ! Some(MessageToPeer(peer.id, message))
      }

    case msg @ MessageFromPeer(_, message) =>
      val hasRespondedToRequest = currRequest match {
        case Some((commander, request)) =>
          val response = (request, message) match {
            case (r: BlockHeadersRequest, m: PV62.BlockHeaders) => Some(r.processResponse(m))
            case (r: BlockBodiesRequest, m: PV62.BlockBodies)   => Some(r.processResponse(m))
            case (r: ReceiptsRequest, m: PV63.Receipts)         => Some(r.processResponse(m))
            case (r: NodeDatasRequest, m: PV63.NodeData)        => Some(r.processResponse(m))
            case (r: NodeDataRequest, m: PV63.NodeData)         => Some(r.processResponse(m))
            case (_, _)                                         => None // usally received other messages without matching request, ignore it and keep currRequest waiting for match 
          }

          response match {
            case Some(x) =>
              log.debug(s"Received response ${message.getClass.getName} from ${peer}")
              currRequest = None
              commander ! x
              true
            case None =>
              log.debug(s"Received ${message.getClass.getName} which unmatchs ${request.getClass.getName} from ${peer}")
              false
          }

        case None =>
          log.debug(s"Received ${message.getClass.getName} without corresponding request at ${peer}")
          false
      }

      message match {
        case SignedTransactions(transactions) =>
          setTxKnown(transactions)
          pendingTransactionsService ! msg
        case _ =>
      }

      maybeUpdatePeerInfoByMessageFromPeer(message) foreach { newPeerInfo =>
        log.debug(s"UpdatedPeerInfo: $peerInfo")
        peerInfo = Some(newPeerInfo)
        syncService ! PeerInfoUpdated(peer.id, newPeerInfo)
      }
  }

  /**
   * Processes the message and the old peer info and returns the peer info
   *
   * @param message to be processed
   * @param initialPeerWithInfo from before the message was processed
   * @return if Some, new updated peer info; if None, peer info does not change
   */
  private def maybeUpdatePeerInfoByMessageToPeer(message: Message): Option[PeerInfo] = {
    peerInfo flatMap (_.maybeUpdateMaxBlock(message))
  }

  /**
   * Processes the message and the old peer info and returns the peer info
   *
   * @param message to be processed
   * @param initialPeerWithInfo from before the message was processed
   * @return if Some, new updated peer info; if None, peer info does not change
   */
  private def maybeUpdatePeerInfoByMessageFromPeer(message: Message): Option[PeerInfo] = {
    peerInfo
      .flatMap(_.maybeUpdateTotalDifficulty(message)).orElse(peerInfo)
      .flatMap(_.maybeUpdateForkAccepted(message, serviceBoard.forkResolverOpt)).orElse(peerInfo)
      .flatMap(_.maybeUpdateMaxBlock(message))
  }

  private def shouldSendNewBlock(newBlock: NewBlock, peerInfo: PeerInfo): Boolean =
    newBlock.block.header.number > peerInfo.maxBlockNumber || newBlock.totalDifficulty.compareTo(peerInfo.totalDifficulty) > 0

  private def isTxKnown(transactions: SignedTransaction): Boolean =
    knownTransactions.contains(transactions.hash)

  private def setTxKnown(transactions: Seq[SignedTransaction]) {
    knownTransactions ++= transactions.map(_.hash)
  }

}
