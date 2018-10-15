import akka.util.ByteString
import khipu.crypto
import khipu.crypto.ECDSASignature
import khipu.domain.SignedTransaction
import khipu.network.p2p.messages.CommonMessages.NewBlock
import java.io.File
import java.io.PrintWriter
import java.net.InetSocketAddress
import java.security.SecureRandom
import org.spongycastle.crypto.AsymmetricCipherKeyPair
import org.spongycastle.crypto.params.ECPublicKeyParameters
import org.spongycastle.util.encoders.Hex
import scala.io.Source

package object khipu {

  type UInt256 = UInt256_biginteger.UInt256
  val UInt256 = UInt256_biginteger.UInt256

  def loadAsymmetricCipherKeyPair(filePath: String, secureRandom: SecureRandom): AsymmetricCipherKeyPair = {
    val file = new File(filePath)
    if (!file.exists) {
      val keysValuePair = crypto.generateKeyPair(secureRandom)

      // write keys to file
      val (priv, _) = crypto.keyPairToByteArrays(keysValuePair)
      require(file.getParentFile.exists || file.getParentFile.mkdirs(), "Key's file parent directory creation failed")
      val writer = new PrintWriter(filePath)
      try {
        writer.write(khipu.toHexString(priv))
      } finally {
        writer.close()
      }

      keysValuePair
    } else {
      val reader = Source.fromFile(filePath)
      try {
        val privHex = reader.mkString
        crypto.keyPairFromPrvKey(khipu.hexDecode(privHex))
      } finally {
        reader.close()
      }
    }
  }

  def toHexString(bytes: Array[Byte]): String = Hex.toHexString(bytes)
  def toHexString(bytes: ByteString): String = Hex.toHexString(bytes.toArray)
  def hexDecode(hexString: String): Array[Byte] = Hex.decode(hexString)

  object ServerStatus {
    case object NotListening extends ServerStatus
    final case class Listening(address: InetSocketAddress) extends ServerStatus
  }
  sealed trait ServerStatus

  final case class NodeStatus(
      key:             AsymmetricCipherKeyPair,
      serverStatus:    ServerStatus,
      discoveryStatus: ServerStatus
  ) {
    val nodeId = ECDSASignature.nodeIdFromPublicKey(key.getPublic.asInstanceOf[ECPublicKeyParameters])
  }

  val TxTopic = "khipu.tx"
  val NewBlockTopic = "khipu.newblock"

  final case class BroadcastNewBlocks(blocks: Seq[NewBlock])
  final case class BroadcastTransactions(transactions: Seq[SignedTransaction])
  final case class ProcessedTransactions(transactions: Seq[SignedTransaction])

  trait Command extends Serializable { def id: String }
  case object ActiveCheckTickKey
  case object ActiveCheckTick
  case object Loaded
  case object Unlock

  sealed trait Log[+T] { def value: T }
  sealed trait Changed[+T] extends Log[T]
  final case class Deleted[T](value: T) extends Changed[T]
  final case class Updated[T](value: T) extends Changed[T]
  final case class Original[T](value: T) extends Log[T]

  case object Stop
}
