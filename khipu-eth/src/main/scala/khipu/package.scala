package khipu

import khipu.crypto.ECDSASignature
import khipu.domain.SignedTransaction
import khipu.network.p2p.messages.CommonMessages.NewBlock
import java.net.InetSocketAddress
import org.spongycastle.crypto.AsymmetricCipherKeyPair
import org.spongycastle.crypto.params.ECPublicKeyParameters

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
