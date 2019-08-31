package khipu.network.rlpx

import akka.actor.ActorSystem
import akka.io.Inet.SocketOption
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Tcp
import java.net.InetSocketAddress
import khipu.network
import khipu.network.handshake.EtcHandshake
import khipu.network.p2p.Message
import khipu.network.p2p.MessageDecoder
import khipu.network.rlpx.auth.AuthHandshake
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

object RLPx {

  def startIncoming(
    serverAddress:   InetSocketAddress,
    messageDecoder:  MessageDecoder,
    protocolVersion: Message.Version,
    authHandshake:   AuthHandshake,
    handshake:       EtcHandshake,
    backlog:         Int                                 = 100,
    socketOptions:   immutable.Traversable[SocketOption] = Nil,
    halfClose:       Boolean                             = false,
    idleTimeout:     Duration                            = Duration.Inf
  )(system: ActorSystem)(implicit mat: Materializer) {
    implicit val _s = system

    // TODO limit max incoming connections
    val connectionSink = Sink.foreach[Tcp.IncomingConnection] { incomingConn =>
      system.log.debug(s"Incoming connection from: ${incomingConn.remoteAddress}")

      // we should build a new layer instance for each incomingConnection, so do it here
      val peer = new IncomingPeer(Peer.peerId(incomingConn.remoteAddress).get, incomingConn.remoteAddress)
      val incomingLayer = BluePrint(peer, messageDecoder, protocolVersion, authHandshake, handshake)

      incomingConn.handleWith(incomingLayer)
    }

    val binding = tcpBind(serverAddress.getHostName, serverAddress.getPort, backlog, socketOptions, halfClose, idleTimeout).to(connectionSink).run()

    import system.dispatcher
    binding onComplete {
      case Success(b) =>
        system.log.info(s"Server started, listening on: ${b.localAddress}")
      case Failure(e) =>
        system.log.info(s"Server could not be bound to ${serverAddress}: ${e.getMessage}")
    }
  }

  def startOutgoing(
    peer:            OutgoingPeer,
    messageDecoder:  MessageDecoder,
    protocolVersion: Message.Version,
    authHandshake:   AuthHandshake,
    handshake:       EtcHandshake
  )(system: ActorSystem)(implicit mat: Materializer) {
    implicit val _s = system
    val remoteAddr: InetSocketAddress = new InetSocketAddress(peer.uri.get.getHost, peer.uri.get.getPort)
    val tcpFlow = Tcp().outgoingConnection(remoteAddr)
    val outgoingLayer = BluePrint(peer, messageDecoder, protocolVersion, authHandshake, handshake)
    //val rlpxFlow = Flow.fromGraph(new RLPxOutgoingStage(uri, messageDecoder, protocolVersion, authHandshake, handshake, blockchainHost))
    //tcpFlow.join(rlpxFlow).run
    tcpFlow.join(outgoingLayer).run
  }

  private def tcpBind(
    interface: String, port: Int,
    backlog:       Int                                 = 100,
    socketOptions: immutable.Traversable[SocketOption],
    halfClose:     Boolean,
    idleTimeout:   Duration
  )(implicit system: ActorSystem): Source[Tcp.IncomingConnection, Future[Tcp.ServerBinding]] = {
    Tcp().bind(
      interface,
      port,
      backlog,
      socketOptions,
      halfClose = false,
      idleTimeout = Duration.Inf // we knowingly disable idle-timeout on TCP level, as we handle it explicitly by self
    )
    //.map { incoming => 
    //  val newFlow =
    //    incoming.flow
    //      // Prevent cancellation from the Http implementation to reach the TCP streams to prevent
    //      // completion / cancellation race towards TCP streams. See #459.
    //      .via(StreamUtils.delayCancellation(settings.lingerTimeout))
    //  incoming.copy(flow = newFlow)
    //}
  }
}
