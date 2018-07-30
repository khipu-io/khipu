package khipu.network.rlpx

import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.event.Logging
import akka.pattern.ask
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.TimerGraphStageLogic
import akka.util.ByteString
import akka.util.ByteStringBuilder
import java.util.concurrent.atomic.AtomicInteger
import khipu.blockchain.sync.SyncService
import khipu.network.Control
import khipu.network.Tick
import khipu.network.WireDisconnected
import khipu.network.handshake.EtcHandshake
import khipu.network.handshake.EtcHandshake.HandshakeFailure
import khipu.network.handshake.EtcHandshake.HandshakeSuccess
import khipu.network.handshake.EtcHandshake.NextMessage
import khipu.network.handshake.EtcHandshake.Noop
import khipu.network.handshake.EtcHandshake.PeerInfo
import khipu.network.p2p.Message
import khipu.network.p2p.MessageDecoder
import khipu.network.p2p.MessageSerializable
import khipu.network.p2p.messages.PV63
import khipu.network.p2p.messages.PV62
import khipu.network.p2p.messages.WireProtocol.Disconnect
import khipu.network.p2p.messages.WireProtocol.Ping
import khipu.network.p2p.messages.WireProtocol.Pong
import khipu.network.rlpx.auth.AuthHandshakeFailure
import khipu.network.rlpx.auth.AuthHandshakeSuccess
import khipu.network.rlpx.auth.AuthHandshake
import khipu.network.rlpx.discovery.NodeDiscoveryService
import khipu.service.ServiceBoard
import khipu.util.BytesUtil
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object RLPxStage {
  private case object AuthHandshakeTimeoutKey

  val decodeTimeout = 40.seconds
  private val decodeTimeoutInMillis = decodeTimeout.toMillis

  private val incomcomingPeersCount = new AtomicInteger()
}
final class RLPxStage(
    peer:            Peer,
    messageDecoder:  MessageDecoder,
    protocolVersion: Message.Version,
    authHandshake:   AuthHandshake,
    handshake:       EtcHandshake
)(implicit system: ActorSystem) extends GraphStage[FlowShape[Either[Control, ByteString], ByteString]] {
  import RLPxStage._
  import system.dispatcher

  private val log = Logging(system, this.getClass)

  private val in = Inlet[Either[Control, ByteString]]("rlpx-in")
  private val out = Outlet[ByteString]("rlpx-out")

  override val shape = new FlowShape[Either[Control, ByteString], ByteString](in, out)

  private var isIncomingPeersIncreased = false
  def isIncoming = peer.isInstanceOf[IncomingPeer]
  def isOutgoing = peer.isInstanceOf[OutgoingPeer]

  def createMessageCodec(secrets: Secrets, messageDecoder: MessageDecoder, protocolVersion: Message.Version): MessageCodec =
    new MessageCodec(new FrameCodec(secrets), messageDecoder, protocolVersion)

  val serviceBoard = ServiceBoard(system)
  def peerManager = serviceBoard.peerManage
  def hostService = serviceBoard.hostService
  def syncService = SyncService.proxy(system)
  def knownNodesService = KnownNodesService.proxy(system)
  def nodeDiscoveryService = NodeDiscoveryService.proxy(system)

  /**
   * To make sure that peer.entity is at the same host of connection, always create
   * it from the TCP stage
   */
  peer.entity = system.actorOf(PeerEntity.props(peer), name = peer.id)

  override def createLogic(inheritedAttributes: Attributes) = new TimerGraphStageLogic(shape) {
    override def preStart() {
      super.preStart()
    }

    override def postStop() {
      if (isIncomingPeersIncreased) {
        incomcomingPeersCount.decrementAndGet
      }
      peer.entity ! PoisonPill
      syncService ! PeerEntity.PeerDisconnected(peer.id)
      peerManager ! PeerEntity.PeerDisconnected(peer.id)
      super.postStop()
    }

    private def closeConnection(reason: String) {
      completeStage()
      log.debug(s"[net] Closed connection to ${peer.id} - $reason")
    }

    private def pushMessagesOrPull(messageCodec: MessageCodec, messages: Iterable[MessageSerializable], shouldDisconnect: Boolean = false) {
      if (messages.nonEmpty) {
        val (willDisconnect, payload) = encodeMessages(messageCodec, messages, shouldDisconnect)
        push(out, payload.result)
        if (willDisconnect) {
          closeConnection("Sent Disconnect message")
        }
      } else {
        pull(in)
      }
    }

    private def encodeMessages(messageCodec: MessageCodec, messages: Iterable[MessageSerializable], shouldDisconnect: Boolean = false, buffer: ByteStringBuilder = ByteString.createBuilder) = {
      messages.foldLeft(shouldDisconnect, buffer) {
        case ((disconnect, acc), message) =>
          val start = System.currentTimeMillis
          val encodedMsg = messageCodec.encodeMessage(message)
          val duration = System.currentTimeMillis - start
          if (messages.nonEmpty && duration > decodeTimeoutInMillis) {
            log.warning(s"[rlpx] Encoded ${message.getClass.getName} in $duration ms, will send to ${peer.id}")
          }

          acc ++= encodedMsg
          (disconnect || message.code == Disconnect.code, acc)
      }
    }

    override protected def onTimer(timerKey: Any) {
      timerKey match {
        case AuthHandshakeTimeoutKey => //handleTimeout()
      }
    }

    setHandler(in, new AuthHandshakeInHandler())
    setHandler(out, new AuthHandshakeOutHandler())

    private class AuthHandshakeOutHandler extends OutHandler {
      var willSendInitMessage = isOutgoing

      override def onPull() {
        log.debug(s"${peer.id} outport: onPull ==>")
        if (willSendInitMessage) {
          val initPacket = authHandshake.initiate(peer.uri.get)
          push(out, initPacket)
          //scheduleOnce(AuthHandshakeTimeoutKey, rlpxConfiguration.waitForHandshakeTimeout)
          willSendInitMessage = false
        } else {
          pull(in)
        }
      }
    }

    private class AuthHandshakeInHandler() extends InHandler {
      var stash = ByteString.empty

      override def onUpstreamFinish() {
        closeConnection("onUpstreamFinish")
      }

      override def onPush() {
        grab(in) match {
          case Left(WireDisconnected) => closeConnection("WireDisconnected")
          case Left(Tick)             => pull(in)
          case Right(data) =>
            cancelTimer(AuthHandshakeTimeoutKey)
            //log.debug(s"got $data")

            if (isIncoming) { // incoming
              incomcomingPeersCount.incrementAndGet
              isIncomingPeersIncreased = true

              def maybePreEIP8Result(data: ByteString) = Try {
                val (responsePacket, result) = authHandshake.handleInitialMessage(data.take(AuthHandshake.InitiatePacketLength))
                val remainingData = data.drop(AuthHandshake.InitiatePacketLength)
                (responsePacket, result, remainingData)
              }

              def maybePostEIP8Result(data: ByteString) = Try {
                val (packetData, remainingData) = decodeV4Packet(data)
                val (responsePacket, result) = authHandshake.handleInitialMessageV4(packetData)
                (responsePacket, result, remainingData)
              }

              maybePreEIP8Result(data) orElse maybePostEIP8Result(data) match {
                case Success((responsePacket, AuthHandshakeSuccess(secrets, remotePubKey), remainingData)) =>
                  peer.uri = Peer.uri(remotePubKey.toArray, peer.remoteAddress)
                  log.debug(s"[handshake] Auth success for $peer with reminingData ${remainingData.length}")

                  val buffer = ByteString.createBuilder ++= responsePacket
                  val messageCodec = createMessageCodec(secrets, messageDecoder, protocolVersion)
                  if (incomcomingPeersCount.get > serviceBoard.peerConfiguration.maxIncomingPeers) {
                    val disconnectMsg: MessageSerializable = Disconnect(Disconnect.Reasons.TooManyPeers)
                    val (willDisconnect, payload) = encodeMessages(messageCodec, Vector(disconnectMsg), true, ByteString.newBuilder)
                    push(out, payload.result)
                    closeConnection("Too many incoming peers")
                  } else {
                    becomeHandshaking(messageCodec, remainingData, buffer)
                  }

                case Success((responsePacket, AuthHandshakeFailure, remainingData)) =>
                  log.debug(s"[handshake] Auth handshake failed for $peer")
                  push(out, responsePacket)
                  closeConnection("AuthHandshakeFailure")

                case Failure(ex) =>
                  log.debug(s"[handshake] Init AuthHandshaker message handling failed for peer ${peer.id} due to ${ex.getMessage}")
                  closeConnection(ex.getMessage)
                //context.parent ! ConnectionFailed
                //context stop self
              }

            } else { // outgoing

              def maybePreEIP8Result(data: ByteString) = Try {
                val (packet, remainingData) = data.splitAt(AuthHandshake.ResponsePacketLength)
                val result = authHandshake.handleResponseMessage(packet)
                (result, remainingData)
              }

              def maybePostEIP8Result(data: ByteString) = Try {
                val (packetData, remainingData) = decodeV4Packet(data)
                val result = authHandshake.handleResponseMessageV4(packetData)
                (result, remainingData)
              }

              maybePreEIP8Result(data) orElse maybePostEIP8Result(data) match {
                case Success((AuthHandshakeSuccess(secrets, remotePubKey), remainingData)) =>
                  peer.uri = Peer.uri(remotePubKey.toArray, peer.remoteAddress)
                  log.debug(s"[handshake] Auth handshake success for $peer with reminingData ${remainingData.length}")

                  //log.debug(s"Auth handshake succeeded for peer $peerId")
                  //context.parent ! ConnectionEstablished(remotePubKey)
                  val buffer = ByteString.createBuilder
                  val messageCodec = createMessageCodec(secrets, messageDecoder, protocolVersion)
                  becomeHandshaking(messageCodec, remainingData, buffer)

                case Success((AuthHandshakeFailure, remainingData)) =>
                  log.debug(s"[handshake] Auth handshake failed for $peer")
                  closeConnection("AuthHandshakeFailure")
                //context.parent ! ConnectionFailed
                //context stop self

                case Failure(ex) =>
                  log.debug(s"[handshake] Response AuthHandshaker message handling failed for peer ${peer.id} due to ${ex.getMessage}")
                  closeConnection(ex.getMessage)
                //context.parent ! ConnectionFailed
                //context stop self
              }
            }
        }
      }

      /**
       * Decode V4 packet
       *
       * @param data, includes both the V4 packet with bytes from next messages
       * @return data of the packet and the remaining data
       */
      private def decodeV4Packet(data: ByteString): (ByteString, ByteString) = {
        val encryptedPayloadSize = BytesUtil.bigEndianToShort(data.take(2).toArray)
        val (packetData, remainingData) = data.splitAt(encryptedPayloadSize + 2)
        (packetData, remainingData)
      }
    }

    private def becomeHandshaking(messageCodec: MessageCodec, remainingData: ByteString, buffer: ByteStringBuilder) {
      val hello: MessageSerializable = handshake.theHelloMessage

      val (messagesSoFar, failed) = messageCodec.decodeMessages(remainingData)
      if (failed.nonEmpty) log.warning(s"[handshake] Got failed messages ${failed.map(_.getMessage)}")
      // TODO disconnect or just ignore failedMessages

      val (shouldDisconnect, messagesToSend) = processReceivedHandshakingMessages(messageCodec, messagesSoFar)
      val (willDisconnect, payload) = encodeMessages(messageCodec, hello +: messagesToSend, shouldDisconnect, buffer)
      push(out, payload.result)

      setHandler(in, new HandshakingInHandler(messageCodec))
      setHandler(out, new HandshakingOutHandler())
    }

    private class HandshakingOutHandler() extends OutHandler {
      override def onPull() {
        log.debug(s"${peer.id} outport: onPull ==>")
        pull(in)
      }
    }

    private class HandshakingInHandler(messageCodec: MessageCodec) extends InHandler {
      override def onUpstreamFinish() {
        closeConnection("onUpstreamFinish")
      }

      override def onPush() {
        val data = grab(in) match {
          case Left(WireDisconnected) => closeConnection("WireDisconnected")
          case Left(Tick)             => pull(in)
          case Right(data) =>
            //log.debug(s"got $data")
            val (messages, failed) = messageCodec.decodeMessages(data)
            if (failed.nonEmpty) log.debug(s"[handshake] Got failed messages ${failed.map(_.getMessage)}")
            // TODO disconnect or just ignore failedMessages

            val (shouldDisconnect, messagesToSend) = processReceivedHandshakingMessages(messageCodec, messages)
            pushMessagesOrPull(messageCodec, messagesToSend, shouldDisconnect)
        }
      }
    }

    def processReceivedHandshakingMessages(messageCodec: MessageCodec, messages: Seq[Message]): (Boolean, Seq[MessageSerializable]) = {
      messages.foldLeft((false, Vector[MessageSerializable]())) {
        case ((shouldDisconnect, acc), Ping()) =>
          val pong: MessageSerializable = Pong() // MessageSerializable to force implicit convert
          (shouldDisconnect, acc :+ pong)

        case ((shouldDisconnect, acc), x @ Disconnect(reason)) =>
          log.debug(s"[handshake] received $x from ${peer.id}")
          reason match {
            case Disconnect.Reasons.IncompatibleP2pProtocolVersion | Disconnect.Reasons.UselessPeer | Disconnect.Reasons.Other =>
              peerManager ! PeerManager.DropNode(peer.id)
            case _ =>
          }

          (shouldDisconnect, acc)

        case ((shouldDisconnect, acc), message) =>
          log.debug(s"[handshake] received ${message.getClass.getName} from ${peer.id}")
          // MessageSerializable force implicit convert
          val msg: Option[MessageSerializable] = handshake.respondTo(message) match {
            case NextMessage(msgToSend, timeoutTime) =>
              log.debug(s"[handshake] nextMessage ${msgToSend.getClass.getName} will send to ${peer.id}")
              Some(msgToSend)

            case HandshakeSuccess(peerInfo) =>
              log.debug(s"[handshake] success $peerInfo")
              peer.uri foreach { knownNodesService ! KnownNodesService.AddKnownNode(_) }
              syncService ! PeerEntity.PeerHandshaked(peer, peerInfo)
              peerManager ! PeerEntity.PeerHandshaked(peer, peerInfo)

              becomeWorking(messageCodec, peerInfo)
              None

            case HandshakeFailure(reason) =>
              log.debug(s"[handshake] failure to ${peer.id} reason $reason when received ${message.getClass.getName}")
              peer.uri foreach { knownNodesService ! KnownNodesService.RemoveKnownNode(_) }
              peer.uri foreach { syncService ! KnownNodesService.RemoveKnownNode(_) }
              reason match {
                case Disconnect.Reasons.IncompatibleP2pProtocolVersion | Disconnect.Reasons.UselessPeer =>
                  peerManager ! PeerManager.DropNode(peer.id)
                case _ =>
              }

              Some(Disconnect(reason))

            case Noop(info) =>
              None
          }

          (shouldDisconnect, acc ++ msg)
      }
    }

    def becomeWorking(messageCodec: MessageCodec, handshakeResult: PeerInfo) {
      // ask for the highest block from the peer 
      // TODO, how to got it work with PeerEntity's req/resp
      import PV62.GetBlockHeaders.GetBlockHeadersEnc

      setHandler(in, new WorkingInHandler(messageCodec))
      setHandler(out, new WorkingOutHandler())
    }

    private class WorkingOutHandler() extends OutHandler {

      override def onPull() {
        log.debug(s"${peer.id} outport: onPull ==>")
        pull(in)
      }
    }

    private class WorkingInHandler(messageCodec: MessageCodec) extends InHandler {
      override def onUpstreamFinish() {
        closeConnection("onUpstreamFinish")
      }

      override def onPush() {
        grab(in) match {
          case Left(WireDisconnected) => closeConnection("WireDisconnected")

          case Left(Tick) =>
            val future = (peer.entity ? PeerEntity.FetchMessageToPeer(peer.id))(120.seconds).mapTo[Option[PeerEntity.MessageToPeer]].recover {
              case e =>
                log.debug(s"[rlpx] Failed to fetch request from ${peer.id}, $e")
                None
            }

            val callback = getAsyncCallback[Option[PeerEntity.MessageToPeer]] {
              case Some(PeerEntity.MessageToPeer(peerId, message)) =>
                pushMessagesOrPull(messageCodec, List(message))
              case None =>
                pull(in)
            }

            future.foreach(callback.invoke)

          case Right(data) =>
            //log.debug(s"got $data")
            val start = System.currentTimeMillis
            val (messages, failed) = messageCodec.decodeMessages(data)
            val duration = System.currentTimeMillis - start
            if (messages.nonEmpty && duration > decodeTimeoutInMillis) {
              log.warning(s"[rlpx] Decoded ${messages.map(x => x.getClass.getName).mkString("(", ",", ")")} in $duration ms")
            }

            if (failed.nonEmpty) {
              // TODO disconnect or just ignore failedMessages
              log.debug(s"[rlpx] Got failed messages ${failed.map(_.getMessage)}")
            }

            val (receivedDisconnect, responses) = messages.foldLeft((None: Option[Disconnect], Vector[Future[Option[MessageSerializable]]]())) {
              case ((_, acc), x: Disconnect) =>
                log.debug(s"[rlpx] Received $x from ${peer.id}")
                (Some(x), acc)

              case ((willDisconnect, acc), message) =>
                message match {
                  case Ping() =>
                    val pong: MessageSerializable = Pong() // MessageSerializable to force implicit convert
                    (willDisconnect, acc :+ Future.successful(Some(pong)))

                  case (_: PV63.GetReceipts | _: PV63.GetNodeData | _: PV62.GetBlockHeaders | _: PV62.GetBlockBodies) => // request from outside
                    log.debug(s"[rlpx] Got request ${message.getClass.getName} from ${peer.id}")

                    // set timeout to a large number for big blocks // (120.seconds)
                    val respFuture = hostService.ask(message) map {
                      case Some(response) =>
                        log.debug(s"[rlpx] Will respond ${response.getClass.getName} to ${message.getClass.getName} at ${peer.id}")
                        Some(response)
                      case None =>
                        log.debug(s"[rlpx] Cannot respond to ${message.getClass.getName} at ${peer.id}")
                        None
                    }

                    (None, acc :+ respFuture)

                  case _ =>
                    peer.entity ! PeerEntity.MessageFromPeer(peer.id, message)
                    (None, acc)
                }
            }

            receivedDisconnect match {
              case Some(disconnectMessage) =>
                closeConnection(disconnectMessage.toString)
              case None =>
                val callback = getAsyncCallback[Vector[Option[MessageSerializable]]] { xs =>
                  pushMessagesOrPull(messageCodec, xs.flatten)
                }

                Future.sequence(responses).foreach(callback.invoke)
            }
        }
      }
    }
  }

}
