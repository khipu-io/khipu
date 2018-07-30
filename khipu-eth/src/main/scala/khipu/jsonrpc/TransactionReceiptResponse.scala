package khipu.jsonrpc

import khipu.Hash
import khipu.domain.Address
import khipu.jsonrpc.FilterManager.TxLog

final case class TransactionReceiptResponse(
  transactionHash:   Hash,
  transactionIndex:  Long,
  blockNumber:       Long,
  blockHash:         Hash,
  cumulativeGasUsed: Long,
  gasUsed:           Long,
  contractAddress:   Option[Address],
  logs:              Seq[TxLog]
)
