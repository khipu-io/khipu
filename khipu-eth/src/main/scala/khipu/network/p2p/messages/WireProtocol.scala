package khipu.network.p2p.messages

import akka.util.ByteString
import khipu.network.p2p.{ Message, MessageSerializableImplicit }
import khipu.network.p2p.messages.WireProtocol.Disconnect.Reasons
import khipu.rlp
import khipu.rlp.RLPEncodeable
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.rlp.RLPSerializable

object WireProtocol {

  object Capability {
    implicit final class CapabilityEnc(val msg: Capability) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = RLPList(msg.name, msg.version)
    }

    implicit final class CapabilityDec(val bytes: Array[Byte]) {
      def toCapability: Capability = CapabilityRLPEncodableDec(rlp.rawDecode(bytes)).toCapability
    }

    implicit final class CapabilityRLPEncodableDec(val rLPEncodeable: RLPEncodeable) {
      def toCapability: Capability = rLPEncodeable match {
        case RLPList(name, version) => Capability(name, version)
        case _                      => throw new RuntimeException("Cannot decode Capability")
      }
    }
  }
  final case class Capability(name: String, version: Byte)

  object Hello {
    val code = 0x00

    implicit final class HelloEnc(val underlying: Hello) extends MessageSerializableImplicit[Hello](underlying) with RLPSerializable {
      import khipu.rlp._

      override def code: Int = Hello.code

      override def toRLPEncodable: RLPEncodeable = {
        import msg._
        RLPList(p2pVersion, clientId, RLPList(capabilities.map(_.toRLPEncodable): _*), listenPort, nodeId)
      }
    }

    implicit final class HelloDec(val bytes: Array[Byte]) {
      import Capability._

      def toHello: Hello = rlp.rawDecode(bytes) match {
        case RLPList(p2pVersion, clientId, (capabilities: RLPList), listenPort, nodeId, _*) =>
          Hello(p2pVersion, clientId, capabilities.items.map(_.toCapability), listenPort, nodeId)
        case _ => throw new RuntimeException("Cannot decode Hello")
      }
    }
  }
  final case class Hello(
      p2pVersion:   Long,
      clientId:     String,
      capabilities: Seq[Capability],
      listenPort:   Long,
      nodeId:       ByteString
  ) extends Message {

    override val code: Int = Hello.code

    override def toString: String = {
      s"""Hello {
         |p2pVersion: $p2pVersion
         |clientId: $clientId
         |capabilities: $capabilities
         |listenPort: $listenPort
         |nodeId: ${khipu.toHexString(nodeId)}
         |}""".stripMargin
    }
  }

  object Disconnect {
    object Reasons {
      val DisconnectRequested = 0x00
      val TcpSubsystemError = 0x01
      val UselessPeer = 0x03
      val TooManyPeers = 0x04
      val AlreadyConnected = 0x05
      val IncompatibleP2pProtocolVersion = 0x06
      val NullNodeIdentityReceived = 0x07
      val ClientQuitting = 0x08
      val UnexpectedIdentity = 0x09
      val IdentityTheSame = 0xa
      val TimeoutOnReceivingAMessage = 0x0b
      val Other = 0x10
    }

    val code = 0x01

    implicit final class DisconnectEnc(val underlying: Disconnect) extends MessageSerializableImplicit[Disconnect](underlying) with RLPSerializable {
      override def code: Int = Disconnect.code

      override def toRLPEncodable: RLPEncodeable = RLPList(msg.reason)
    }

    implicit final class DisconnectDec(val bytes: Array[Byte]) {
      def toDisconnect: Disconnect = rlp.rawDecode(bytes) match {
        case RLPList(reason, _*) => Disconnect(reason = reason)
        case _                   => throw new RuntimeException("Cannot decode Disconnect")
      }
    }
  }
  final case class Disconnect(reason: Long) extends Message {
    override val code: Int = Disconnect.code

    override def toString: String = {
      val message = reason match {
        case Reasons.DisconnectRequested            => "Disconnect requested"
        case Reasons.TcpSubsystemError              => "TCP sub-system error"
        case Reasons.UselessPeer                    => "Useless peer"
        case Reasons.TooManyPeers                   => "Too many peers"
        case Reasons.AlreadyConnected               => "Already connected"
        case Reasons.IncompatibleP2pProtocolVersion => "Incompatible P2P protocol version"
        case Reasons.NullNodeIdentityReceived       => "Null node identity received - this is automatically invalid"
        case Reasons.ClientQuitting                 => "Client quitting"
        case Reasons.UnexpectedIdentity             => "Unexpected identity"
        case Reasons.IdentityTheSame                => "Identity is the same as this node"
        case Reasons.TimeoutOnReceivingAMessage     => "Timeout on receiving a message"
        case Reasons.Other                          => "Some other reason specific to a subprotocol"
        case other                                  => s"unknown reason code: $other"
      }

      s"Disconnect($message)"
    }
  }

  object Ping {
    val code = 0x02

    implicit final class PingEnc(val underlying: Ping) extends MessageSerializableImplicit[Ping](underlying) with RLPSerializable {
      override def code: Int = Ping.code

      override def toRLPEncodable: RLPEncodeable = RLPList()
    }

    implicit final class PingDec(val bytes: Array[Byte]) {
      def toPing: Ping = Ping()
    }
  }
  final case class Ping() extends Message {
    override val code: Int = Ping.code
  }

  object Pong {
    val code = 0x03

    implicit final class PongEnc(val underlying: Pong) extends MessageSerializableImplicit[Pong](underlying) with RLPSerializable {
      override def code: Int = Pong.code

      override def toRLPEncodable: RLPEncodeable = RLPList()
    }

    implicit final class PongDec(val bytes: Array[Byte]) {
      def toPong: Pong = Pong()
    }
  }
  final case class Pong() extends Message {
    override val code: Int = Pong.code
  }

}
