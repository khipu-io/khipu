package khipu.network.p2p

import khipu.network.p2p.Message.Version
import khipu.network.p2p.messages.CommonMessages.NewBlock._
import khipu.network.p2p.messages.CommonMessages.SignedTransactions._
import khipu.network.p2p.messages.CommonMessages.Status._
import khipu.network.p2p.messages.CommonMessages._
import khipu.network.p2p.messages.PV61
import khipu.network.p2p.messages.PV61.BlockHashesFromNumber._
import khipu.network.p2p.messages.PV62
import khipu.network.p2p.messages.PV62.NewBlockHashes._
import khipu.network.p2p.messages.PV62.BlockBodies._
import khipu.network.p2p.messages.PV62.BlockHeaders._
import khipu.network.p2p.messages.PV62.GetBlockBodies._
import khipu.network.p2p.messages.PV62.GetBlockHeaders._
import khipu.network.p2p.messages.PV63
import khipu.network.p2p.messages.PV63.GetNodeData._
import khipu.network.p2p.messages.PV63.GetReceipts._
import khipu.network.p2p.messages.PV63.NodeData._
import khipu.network.p2p.messages.PV63.Receipts._
import khipu.network.p2p.messages.WireProtocol.Disconnect._
import khipu.network.p2p.messages.WireProtocol.Hello._
import khipu.network.p2p.messages.WireProtocol.Ping._
import khipu.network.p2p.messages.WireProtocol.Pong._
import khipu.network.p2p.messages.WireProtocol._
import khipu.network.p2p.messages.Versions
import scala.util.Try

trait MessageDecoder {
  def fromBytes(`type`: Int, payload: Array[Byte], protocolVersion: Message.Version): Try[Message]
}

object MessageDecoder extends MessageDecoder {

  override def fromBytes(tpe: Int, payload: Array[Byte], protocolVersion: Version): Try[Message] =
    Try {
      (protocolVersion, tpe) match {
        // network messages
        case (_, Disconnect.code) => payload.toDisconnect
        case (_, Ping.code) => payload.toPing
        case (_, Pong.code) => payload.toPong

        // wire protocol
        case (_, Hello.code) => payload.toHello

        // common
        case (_, Status.code) => payload.toStatus
        case (_, SignedTransactions.code) => payload.toSignedTransactions
        case (_, NewBlock.code) => payload.toNewBlock

        case (Versions.PV61, tpe) => handlePV61(tpe, payload)

        case (Versions.PV62 | Versions.PV63, PV62.NewBlockHashes.code) => payload.toNewBlockHashes
        case (Versions.PV62 | Versions.PV63, PV62.GetBlockHeaders.code) => payload.toGetBlockHeaders
        case (Versions.PV62 | Versions.PV63, PV62.BlockHeaders.code) => payload.toBlockHeaders
        case (Versions.PV62 | Versions.PV63, PV62.GetBlockBodies.code) => payload.toGetBlockBodies
        case (Versions.PV62 | Versions.PV63, PV62.BlockBodies.code) => payload.toBlockBodies

        case (Versions.PV63, tpe) => handlePV63(tpe, payload)

        case _ => throw new RuntimeException(s"Unknown message type: ${tpe}")
      }
    }

  private def handlePV61(tpe: Int, payload: Array[Byte]): Message = {
    import khipu.network.p2p.messages.PV61.NewBlockHashes._
    tpe match {
      case PV61.NewBlockHashes.code        => payload.toNewBlockHashes
      case PV61.BlockHashesFromNumber.code => payload.toBlockHashesFromNumber
      case _                               => throw new RuntimeException(s"Unknown message type: ${tpe}")
    }
  }

  private def handlePV63(tpe: Int, payload: Array[Byte]): Message = tpe match {
    case PV63.GetNodeData.code => payload.toGetNodeData
    case PV63.NodeData.code    => payload.toNodeData
    case PV63.GetReceipts.code => payload.toGetReceipts
    case PV63.Receipts.code    => payload.toReceipts
    case _                     => throw new RuntimeException(s"Unknown message type: ${tpe}")
  }
}
