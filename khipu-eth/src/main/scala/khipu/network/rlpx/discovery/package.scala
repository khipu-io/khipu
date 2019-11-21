package khipu.network.rlpx

import akka.util.ByteString
import khipu.rlp.{ RLPDecoder, RLPEncodeable, RLPEncoder, RLPList }
import khipu.rlp.RLPImplicits._
import com.typesafe.config.Config
import java.net.InetSocketAddress
import khipu.rlp.RLPImplicitConversions._
import khipu.util.BytesUtil
import scala.concurrent.duration._

package object discovery {

  val VERSION = 4

  sealed trait Message {
    def packetType: Byte
  }

  object Endpoint {
    implicit val rlpEncDec = new RLPEncoder[Endpoint] with RLPDecoder[Endpoint] {
      override def encode(obj: Endpoint): RLPEncodeable = {
        import obj._
        RLPList(address, udpPort, tcpPort)
      }

      override def decode(rlp: RLPEncodeable): Endpoint = rlp match {
        case RLPList(address, udpPort, tcpPort, _*) =>
          Endpoint(address, udpPort, tcpPort)
        case _ => throw new RuntimeException("Cannot decode Endpoint")
      }
    }

  }
  final case class Endpoint(address: ByteString, udpPort: Int, tcpPort: Int) {
    val ip = BytesUtil.bytesToIp(address)
    val udpAddress = new InetSocketAddress(ip, udpPort)
    val tcpAddress = new InetSocketAddress(ip, tcpPort)
  }

  object Ping {
    val packetType: Byte = 0x01

    implicit val rlpEncDec = new RLPEncoder[Ping] with RLPDecoder[Ping] {
      override def encode(obj: Ping): RLPEncodeable = {
        import obj._
        RLPList(version, from, to, timestamp)
      }

      override def decode(rlp: RLPEncodeable): Ping = rlp match {
        case RLPList(version, from, to, timestamp, _*) =>
          Ping(version, Endpoint.rlpEncDec.decode(from), Endpoint.rlpEncDec.decode(to), timestamp)
        case _ => throw new RuntimeException("Cannot decode Ping")
      }
    }
  }
  final case class Ping(version: Int, from: Endpoint, to: Endpoint, timestamp: Long) extends Message {
    override val packetType: Byte = Ping.packetType
  }

  object Neighbours {
    val packetType: Byte = 0x04

    implicit val rlpEncDec = new RLPEncoder[Neighbours] with RLPDecoder[Neighbours] {
      override def encode(obj: Neighbours): RLPEncodeable = {
        import obj._
        RLPList(RLPList(nodes.map(Neighbour.rlpEncDec.encode): _*), expires)
      }

      override def decode(rlp: RLPEncodeable): Neighbours = rlp match {
        case RLPList(nodes: RLPList, expires, _*) =>
          Neighbours(nodes.items.map(Neighbour.rlpEncDec.decode), expires)
        case _ => throw new RuntimeException("Cannot decode Neighbours")
      }
    }
  }
  final case class Neighbours(nodes: Seq[Neighbour], expires: Long) extends Message {
    override val packetType: Byte = Neighbours.packetType
  }

  object Neighbour {
    implicit val rlpEncDec = new RLPEncoder[Neighbour] with RLPDecoder[Neighbour] {
      override def encode(obj: Neighbour): RLPEncodeable = {
        import obj._
        RLPList(endpoint, nodeId)
      }

      override def decode(rlp: RLPEncodeable): Neighbour = rlp match {
        case RLPList(address, udpPort, tcpPort, nodeId, _*) =>
          Neighbour(Endpoint(address, udpPort, tcpPort), nodeId)
        case _ =>
          throw new RuntimeException("Cannot decode Neighbour")
      }
    }
  }
  final case class Neighbour(endpoint: Endpoint, nodeId: ByteString)

  object FindNode {
    val packetType: Byte = 0x03

    implicit val rlpEncDec = new RLPEncoder[FindNode] with RLPDecoder[FindNode] {
      override def encode(obj: FindNode): RLPEncodeable = {
        import obj._
        RLPList(target, expires)
      }

      override def decode(rlp: RLPEncodeable): FindNode = rlp match {
        case RLPList(target, expires, _*) =>
          FindNode(target, expires)
        case _ => throw new RuntimeException("Cannot decode FindNode")
      }
    }
  }
  final case class FindNode(target: ByteString, expires: Long) extends Message {
    override val packetType: Byte = FindNode.packetType
  }

  object Pong {
    val packetType: Byte = 0x02

    implicit val rlpEncDec = new RLPEncoder[Pong] with RLPDecoder[Pong] {
      override def encode(obj: Pong): RLPEncodeable = {
        import obj._
        RLPList(to, token, timestamp)
      }

      override def decode(rlp: RLPEncodeable): Pong = rlp match {
        case RLPList(to, token, timestamp, _*) =>
          Pong(Endpoint.rlpEncDec.decode(to), token, timestamp)
        case _ => throw new RuntimeException("Cannot decode Pong")
      }
    }
  }
  final case class Pong(to: Endpoint, token: ByteString, timestamp: Long) extends Message {
    override val packetType: Byte = Pong.packetType
  }

  object DiscoveryConfig {
    def apply(etcClientConfig: Config): DiscoveryConfig = {
      import scala.collection.JavaConverters._
      val discoveryConfig = etcClientConfig.getConfig("network.discovery")
      val bootstrapNodes = discoveryConfig.getStringList("bootstrap-nodes").asScala.toSet
      val interface = discoveryConfig.getString("interface")
      val port = discoveryConfig.getInt("port")
      val listenAddress = new InetSocketAddress(interface, port)
      DiscoveryConfig(
        discoveryEnabled = discoveryConfig.getBoolean("discovery-enabled"),
        interface = interface,
        port = port,
        listenAddress = listenAddress,
        bootstrapNodes = bootstrapNodes,
        nodesLimit = discoveryConfig.getInt("nodes-limit"),
        scanMaxNodes = discoveryConfig.getInt("scan-max-nodes"),
        scanInitialDelay = discoveryConfig.getDuration("scan-initial-delay").toMillis.millis,
        scanInterval = discoveryConfig.getDuration("scan-interval").toMillis.millis,
        messageExpiration = discoveryConfig.getDuration("message-expiration").toMillis.millis
      )
    }
  }
  final case class DiscoveryConfig(
    discoveryEnabled:  Boolean,
    interface:         String,
    port:              Int,
    listenAddress:     InetSocketAddress,
    bootstrapNodes:    Set[String],
    nodesLimit:        Int /* TODO: remove once proper discovery protocol is in place */ ,
    scanMaxNodes:      Int /* TODO: remove once proper discovery protocol is in place */ ,
    scanInitialDelay:  FiniteDuration,
    scanInterval:      FiniteDuration,
    messageExpiration: FiniteDuration
  )

}