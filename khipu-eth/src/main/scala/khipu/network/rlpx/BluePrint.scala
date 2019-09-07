package khipu.network.rlpx

import akka.actor.ActorSystem
import akka.stream.FlowShape
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.MergePreferred
import akka.stream.scaladsl.Source
import akka.util.ByteString
import khipu.network.Control
import khipu.network.Peer
import khipu.network.Tick
import khipu.network.handshake.EtcHandshake
import khipu.network.p2p.Message
import khipu.network.p2p.MessageDecoder
import khipu.network.rlpx.auth.AuthHandshake
import scala.concurrent.duration._

object BluePrint {
  /**
   *
   *                        +------------+  +------------+
   * incoming -> wireStage ->            |  |            -> outgoing
   *                        |  merge     -> | frameStage |
   *             control   ->            |  |            |
   *                        +------------+  +------------+
   *
   */
  def apply(
    peer:            Peer,
    messageDecoder:  MessageDecoder,
    protocolVersion: Message.Version,
    authHandshake:   AuthHandshake,
    handshake:       EtcHandshake
  )(implicit system: ActorSystem) = {
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val control = builder.add(Source.tick(0.seconds, 1.millis, Left(Tick)).buffer(1, OverflowStrategy.dropNew))
      val incoming = builder.add(Flow[ByteString])
      val wireStage = builder.add(new WireStage())
      val rlpxFlow = builder.add(Flow.fromGraph(new RLPxStage(peer, messageDecoder, protocolVersion, authHandshake, handshake)))

      val merge = builder.add(MergePreferred[Either[Control, ByteString]](1))

      incoming ~> wireStage ~> merge.preferred
      control ~> merge.in(0)
      merge ~> rlpxFlow.in

      FlowShape(incoming.in, rlpxFlow.out)
    })
  }
}
