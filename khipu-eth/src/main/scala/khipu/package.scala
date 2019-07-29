package khipu

import khipu.crypto.ECDSASignature
import khipu.domain.Receipt
import khipu.domain.SignedTransaction
import khipu.network.p2p.messages.CommonMessages.NewBlock
import khipu.network.p2p.messages.PV62
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

case object Stop

sealed trait WithBlockNumber[T <: WithBlockNumber[T]] extends Ordered[T] {
  def number: Long

  def compare(that: T) = {
    if (number < that.number) {
      -1
    } else if (number == that.number) {
      0
    } else {
      1
    }
  }
}
final case class HashWithBlockNumber(number: Long, hash: Hash) extends WithBlockNumber[HashWithBlockNumber]
final case class BodyWithBlockNumber(number: Long, hash: Hash, body: PV62.BlockBody) extends WithBlockNumber[BodyWithBlockNumber]
final case class ReceiptsWithBlockNumber(number: Long, hash: Hash, receipts: Seq[Receipt]) extends WithBlockNumber[ReceiptsWithBlockNumber]