package khipu.domain

import akka.util.ByteString
import khipu.Hash
import khipu.trie.ByteArraySerializable

object Receipt {
  val byteArraySerializable = new ByteArraySerializable[Receipt] {
    import khipu.network.p2p.messages.PV63.ReceiptImplicits._

    override def fromBytes(bytes: Array[Byte]): Receipt = bytes.toReceipt
    override def toBytes(input: Receipt): Array[Byte] = input.toBytes
  }

  val Failure = Hash(Array[Byte]())
  val Success = Hash(Array[Byte](1))
}

/**
 * postTxState: the intermediate state root field (pre-eip658) or success/failure status (eip658)
 */
final case class Receipt(
    postTxState:       Hash,
    cumulativeGasUsed: Long,
    logsBloomFilter:   ByteString,
    logs:              Seq[TxLogEntry]
) {

  def hasTxStatus = {
    postTxState.length <= 1
  }

  def isTxStatusOK = {
    postTxState == Receipt.Success
  }

  private def stateHashOrStatus = {
    if (hasTxStatus) {
      if (isTxStatusOK) "OK" else "Failed"
    } else {
      postTxState.hexString
    }
  }

  override def toString: String = {
    s"""
       |Receipt{
       | ${stateHashOrStatus}
       | cumulativeGasUsed: $cumulativeGasUsed
       | logsBloomFilter: ${khipu.toHexString(logsBloomFilter)}
       | logs: $logs
       |}
       """.stripMargin
  }
}
