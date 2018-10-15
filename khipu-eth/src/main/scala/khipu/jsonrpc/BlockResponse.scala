package khipu.jsonrpc

import akka.util.ByteString
import khipu.Hash
import khipu.UInt256
import khipu.domain.{ Block, BlockHeader }
import khipu.network.p2p.messages.PV62.BlockBody

object BlockResponse {
  def apply(
    block:           Block,
    totalDifficulty: Option[UInt256] = None,
    fullTxs:         Boolean         = false,
    pendingBlock:    Boolean         = false
  ): BlockResponse = {
    val transactions =
      if (fullTxs)
        Right(block.body.transactionList.zipWithIndex.map {
          case (stx, transactionIndex) =>
            TransactionResponse(stx = stx, blockHeader = Some(block.header), transactionIndex = Some(transactionIndex))
        })
      else
        Left(block.body.transactionList.map(_.hash))

    BlockResponse(
      number = block.header.number,
      hash = if (pendingBlock) None else Some(block.header.hash),
      parentHash = block.header.parentHash,
      nonce = if (pendingBlock) None else Some(block.header.nonce),
      sha3Uncles = block.header.ommersHash,
      logsBloom = block.header.logsBloom,
      transactionsRoot = block.header.transactionsRoot,
      stateRoot = block.header.stateRoot,
      receiptsRoot = block.header.receiptsRoot,
      miner = if (pendingBlock) None else Some(block.header.beneficiary),
      difficulty = block.header.difficulty,
      totalDifficulty = totalDifficulty,
      extraData = block.header.extraData,
      size = Block.size(block),
      gasLimit = block.header.gasLimit,
      gasUsed = block.header.gasUsed,
      timestamp = block.header.unixTimestamp,
      transactions = transactions,
      uncles = block.body.uncleNodesList.map(_.hash)
    )
  }

  def apply(blockHeader: BlockHeader, totalDifficulty: Option[UInt256], pendingBlock: Boolean): BlockResponse =
    BlockResponse(
      block = Block(blockHeader, BlockBody(Nil, Nil)),
      totalDifficulty = totalDifficulty,
      pendingBlock = pendingBlock
    )
}
final case class BlockResponse(
  number:           Long,
  hash:             Option[Hash],
  parentHash:       Hash,
  nonce:            Option[ByteString],
  sha3Uncles:       Hash,
  logsBloom:        ByteString,
  transactionsRoot: Hash,
  stateRoot:        Hash,
  receiptsRoot:     Hash,
  miner:            Option[ByteString],
  difficulty:       UInt256,
  totalDifficulty:  Option[UInt256],
  extraData:        ByteString,
  size:             Long,
  gasLimit:         Long,
  gasUsed:          Long,
  timestamp:        Long,
  transactions:     Either[Seq[Hash], Seq[TransactionResponse]],
  uncles:           Seq[Hash]
)

