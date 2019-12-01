package khipu.network.rlpx

import akka.util.ByteString
import com.typesafe.config.Config
import java.net.InetSocketAddress
import java.net.URI
import khipu.crypto
import khipu.crypto.ECDSASignature
import khipu.rlp
import khipu.rlp.{ RLPDecoder, RLPEncodeable, RLPEncoder, RLPList }
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPImplicitConversions._
import khipu.util.BytesUtil
import org.spongycastle.crypto.AsymmetricCipherKeyPair
import org.spongycastle.util.BigIntegers
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

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
  final case class Endpoint(address: Array[Byte], udpPort: Int, tcpPort: Int) {
    val ip = BytesUtil.bytesToIp(address)
    val udpAddress = new InetSocketAddress(ip, udpPort)
    val tcpAddress = new InetSocketAddress(ip, tcpPort)
  }

  object Ping {
    val packetType: Byte = 0x01

    implicit val rlpEncDec = new RLPEncoder[Ping] with RLPDecoder[Ping] {
      override def encode(obj: Ping): RLPEncodeable = {
        import obj._
        RLPList(version, from, to, expiration)
      }

      override def decode(rlp: RLPEncodeable): Ping = rlp match {
        case RLPList(version, from, to, expiration, _*) =>
          Ping(version, Endpoint.rlpEncDec.decode(from), Endpoint.rlpEncDec.decode(to), expiration)
        case _ => throw new RuntimeException("Cannot decode Ping")
      }
    }
  }
  final case class Ping(version: Int, from: Endpoint, to: Endpoint, expiration: Long) extends Message {
    override val packetType: Byte = Ping.packetType
  }

  object Pong {
    val packetType: Byte = 0x02

    implicit val rlpEncDec = new RLPEncoder[Pong] with RLPDecoder[Pong] {
      override def encode(obj: Pong): RLPEncodeable = {
        import obj._
        RLPList(to, pingHash, expiration)
      }

      override def decode(rlp: RLPEncodeable): Pong = rlp match {
        case RLPList(to, pingHash, expiration, _*) =>
          Pong(Endpoint.rlpEncDec.decode(to), pingHash, expiration)
        case _ => throw new RuntimeException("Cannot decode Pong")
      }
    }
  }
  final case class Pong(to: Endpoint, pingHash: ByteString, expiration: Long) extends Message {
    override val packetType: Byte = Pong.packetType
  }

  object FindNode {
    val packetType: Byte = 0x03

    implicit val rlpEncDec = new RLPEncoder[FindNode] with RLPDecoder[FindNode] {
      override def encode(obj: FindNode): RLPEncodeable = {
        import obj._
        RLPList(target, expiration)
      }

      override def decode(rlp: RLPEncodeable): FindNode = rlp match {
        case RLPList(target, expiration, _*) =>
          FindNode(target, expiration)
        case _ => throw new RuntimeException("Cannot decode FindNode")
      }
    }
  }
  final case class FindNode(target: ByteString, expiration: Long) extends Message {
    override val packetType: Byte = FindNode.packetType
  }

  object Neighbours {
    val packetType: Byte = 0x04

    implicit val rlpEncDec = new RLPEncoder[Neighbours] with RLPDecoder[Neighbours] {
      override def encode(obj: Neighbours): RLPEncodeable = {
        import obj._
        RLPList(RLPList(nodes.map(Neighbour.rlpEncDec.encode): _*), expiration)
      }

      override def decode(rlp: RLPEncodeable): Neighbours = rlp match {
        case RLPList(nodes: RLPList, expiration, _*) =>
          Neighbours(nodes.items.map(Neighbour.rlpEncDec.decode), expiration)
        case _ => throw new RuntimeException("Cannot decode Neighbours")
      }
    }
  }
  final case class Neighbours(nodes: Seq[Neighbour], expiration: Long) extends Message {
    override val packetType: Byte = Neighbours.packetType
  }

  object Neighbour {
    implicit val rlpEncDec = new RLPEncoder[Neighbour] with RLPDecoder[Neighbour] {
      override def encode(obj: Neighbour): RLPEncodeable = {
        import obj._
        RLPList(endpoint, nodeId)
      }

      override def decode(rlp: RLPEncodeable): Neighbour = rlp match {
        case RLPList(address, port, nodeId) =>
          Neighbour(Endpoint(address, port, port), nodeId)
        case RLPList(address, udpPort, tcpPort, nodeId) =>
          Neighbour(Endpoint(address, udpPort, tcpPort), nodeId)
        case _ =>
          throw new RuntimeException("Cannot decode Neighbour")
      }
    }
  }
  final case class Neighbour(endpoint: Endpoint, nodeId: Array[Byte])

  object Packet {
    private val MdcLength = 32
    private val PacketTypeByteIndex = MdcLength + ECDSASignature.EncodedLength
    private val DataOffset = PacketTypeByteIndex + 1

    def decodePacket(input: ByteString): Try[Packet] = {
      if (input.length < 98) {
        Failure(new RuntimeException("Bad message"))
      } else {
        Try(Packet(input)) match {
          case x @ Success(packet) =>
            if (java.util.Arrays.equals(packet.mdc.toArray, crypto.kec256(input.drop(32)))) {
              x
            } else {
              Failure(new RuntimeException("MDC check failed"))
            }
          case e: Failure[_] =>
            e
        }
      }
    }

    def extractMessage(packet: Packet): Try[Message] = Try {
      packet.packetType match {
        case Ping.packetType       => rlp.decode[Ping](packet.data.toArray)
        case Pong.packetType       => rlp.decode[Pong](packet.data.toArray)
        case FindNode.packetType   => rlp.decode[FindNode](packet.data.toArray)
        case Neighbours.packetType => rlp.decode[Neighbours](packet.data.toArray)
        case _                     => throw new RuntimeException(s"Unknown packet type ${packet.packetType}")
      }
    }

    def encodePacket[M <: Message](msg: M, keyPair: AsymmetricCipherKeyPair)(implicit rlpEnc: RLPEncoder[M]): (ByteString, ByteString) = {
      val encodedData = rlp.encode(msg)

      val payload = Array(msg.packetType) ++ encodedData
      val forSig = crypto.kec256(payload)
      val signature = ECDSASignature.sign(forSig, keyPair, None)

      val sigBytes =
        BigIntegers.asUnsignedByteArray(32, signature.r) ++
          BigIntegers.asUnsignedByteArray(32, signature.s) ++
          Array[Byte]((signature.v - 27).toByte)

      val forSha = sigBytes ++ Array(msg.packetType) ++ encodedData
      val mdc = crypto.kec256(forSha)

      (ByteString(mdc), ByteString(mdc ++ forSha))
    }
  }
  final case class Packet(wire: ByteString) {
    import Packet._

    val mdc: ByteString = wire.take(MdcLength)

    val signature: ECDSASignature = {
      val signatureBytes = wire.drop(MdcLength).take(ECDSASignature.EncodedLength)
      val r = signatureBytes.take(ECDSASignature.RLength)
      val s = signatureBytes.drop(ECDSASignature.RLength).take(ECDSASignature.SLength)
      val v = ByteString(Array[Byte]((signatureBytes(ECDSASignature.EncodedLength - 1) + 27).toByte))

      ECDSASignature(r, s, v)
    }

    val nodeId = {
      val msgHash = crypto.kec256(wire.drop(MdcLength + ECDSASignature.EncodedLength))
      ECDSASignature.recoverPublicKey(signature, msgHash, None)
    }.get

    val packetType: Byte = wire(PacketTypeByteIndex)
    val data: ByteString = wire.drop(DataOffset)
  }

  object Node {
    trait State
    object State {
      /**
       * The new node was just discovered either by receiving it with Neighbours
       * message or by receiving Ping from a new node
       * In either case we are sending Ping and waiting for Pong
       * If the Pong is received the node becomes {@link #Alive}
       * If the Pong was timed out the node becomes {@link #Dead}
       */
      case object Discovered extends State
      /**
       * The node didn't send the Pong message back withing acceptable timeout
       * This is the final state
       */
      case object Dead extends State
      /**
       * The node responded with Pong and is now the candidate for inclusion to the table
       * If the table has bucket space for this node it is added to table and becomes {@link #Active}
       * If the table bucket is full this node is challenging with the old node from the bucket
       *     if it wins then old node is dropped, and this node is added and becomes {@link #Active}
       *     else this node becomes {@link #NonActive}
       */
      case object Alive extends State
      /**
       * The node is included in the table. It may become {@link #EvictCandidate} if a new node
       * wants to become Active but the table bucket is full.
       */
      case object Active extends State
      /**
       * This node is in the table but is currently challenging with a new Node candidate
       * to survive in the table bucket
       * If it wins then returns back to {@link #Active} state, else is evicted from the table
       * and becomes {@link #NonActive}
       */
      case object EvictCandidate extends State
      /**
       * Veteran. It was Alive and even Active but is now retired due to loosing the challenge
       * with another Node.
       * For no this is the final state
       * It's an option for future to return veterans back to the table
       */
      case object NonActive extends State
    }

    def apply(id: Array[Byte], udpAddress: InetSocketAddress, tcpAddress: InetSocketAddress) = {
      new Node(id, udpAddress, tcpAddress, System.currentTimeMillis)
    }

    def apply(uri: URI): Node = {
      val id = khipu.hexDecode(uri.getUserInfo)
      new Node(id, new InetSocketAddress(uri.getHost, uri.getPort), new InetSocketAddress(uri.getHost, uri.getPort), System.currentTimeMillis)
    }
  }
  final class Node private (val id: Array[Byte], val udpAddress: InetSocketAddress, var tcpAddress: InetSocketAddress, val addTimestamp: Long) {
    private val uriString = s"enode://${khipu.toHexString(id)}@${tcpAddress.getAddress.getHostAddress}:${tcpAddress.getPort}"

    val uri = new URI(uriString)

    var state: Node.State = _

    var replaceCandicate: Option[Node] = None

    var waitForPong: Option[ByteString] = None
    var waitForNeighbors = false
    var pingSent = 0L
    var pingTrials = 3

    override def equals(o: Any) = {
      o match {
        case n: Node => this.uri == n.uri
        case _       => false
      }
    }

    override def hashCode = uriString.hashCode
    override def toString = uriString
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

  object KademliaOptions {
    val BUCKET_SIZE = 16 // Kademlia bucket size
    val ALPHA = 3 // Kademlia concurrency factor
    val ID_LENGTH_IN_BITS = 256 // id length in bits
    val MAX_STEPS = 8

    val REQ_TIMEOUT = 300L
    val BUCKET_REFRESH = 7200L // bucket refreshing interval in millis
    val DISCOVER_CYCLE = 30L // discovery cycle interval in seconds
  }

}