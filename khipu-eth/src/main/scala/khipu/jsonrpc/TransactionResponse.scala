package khipu.jsonrpc

import akka.util.ByteString
import khipu.Hash
import khipu.domain.{ BlockHeader, SignedTransaction }
import khipu.vm.UInt256

object TransactionResponse {

  def apply(
    stx:              SignedTransaction,
    blockHeader:      Option[BlockHeader] = None,
    transactionIndex: Option[Long]        = None
  ): TransactionResponse =
    TransactionResponse(
      hash = stx.hash,
      nonce = stx.tx.nonce,
      blockHash = blockHeader.map(_.hash),
      blockNumber = blockHeader.map(_.number),
      transactionIndex = transactionIndex,
      from = stx.sender.bytes,
      to = stx.tx.receivingAddress.map(_.bytes),
      value = stx.tx.value,
      gasPrice = stx.tx.gasPrice,
      gas = stx.tx.gasLimit,
      input = stx.tx.payload
    )
}
final case class TransactionResponse(
  hash:             Hash,
  nonce:            UInt256,
  blockHash:        Option[Hash],
  blockNumber:      Option[Long],
  transactionIndex: Option[Long],
  from:             ByteString,
  to:               Option[ByteString],
  value:            UInt256,
  gasPrice:         UInt256,
  gas:              Long,
  input:            ByteString
)

