package khipu.validators

import akka.util.ByteString
import khipu.crypto
import khipu.domain.{ Block, BlockHeader, Receipt, SignedTransaction }
import khipu.ledger.BloomFilter
import khipu.network.p2p.messages.PV62.BlockBody
import khipu.util.BytesUtil
import khipu.validators.BlockValidator.BlockError
import scala.language.reflectiveCalls

trait BlockValidator {
  def validateHeaderAndBody(blockHeader: BlockHeader, blockBody: BlockBody): Either[BlockError, Block]
  def validateBlockAndReceipts(block: Block, receipts: Seq[Receipt]): Either[BlockError, Block]
}

object BlockValidator extends BlockValidator {

  /**
   * This method allows validate a Block. It only perfoms the following validations (stated on
   * section 4.4.2 of http://paper.gavwood.com/):
   *   - BlockValidator.validateTransactionRoot
   *   - BlockValidator.validateOmmersHash
   *   - BlockValidator.validateReceipts
   *   - BlockValidator.validateLogBloom
   *
   * @param block    Block to validate
   * @param receipts Receipts to be in validation process
   * @return The block if validations are ok, error otherwise
   */
  def validate(block: Block, receipts: Seq[Receipt]): Either[BlockError, Block] = {
    for {
      _ <- validateHeaderAndBody(block.header, block.body)
      _ <- validateBlockAndReceipts(block, receipts)
    } yield block
  }

  /**
   * This method allows validate that a BlockHeader matches a BlockBody. It only perfoms the following validations (stated on
   * section 4.4.2 of http://paper.gavwood.com/):
   *   - BlockValidator.validateTransactionRoot
   *   - BlockValidator.validateOmmersHash
   *
   * @param blockHeader to validate
   * @param blockBody to validate
   * @return The block if the header matched the body, error otherwise
   */
  def validateHeaderAndBody(blockHeader: BlockHeader, blockBody: BlockBody): Either[BlockError, Block] = {
    val block = Block(blockHeader, blockBody)
    for {
      _ <- validateTransactionRoot(block)
      _ <- validateOmmersHash(block)
    } yield block
  }

  /**
   * This method allows validations of the block with its associated receipts.
   * It only perfoms the following validations (stated on section 4.4.2 of http://paper.gavwood.com/):
   *   - BlockValidator.validateReceipts
   *   - BlockValidator.validateLogBloom
   *
   * @param block    Block to validate
   * @param receipts Receipts to be in validation process
   * @return The block if validations are ok, error otherwise
   */
  def validateBlockAndReceipts(block: Block, receipts: Seq[Receipt]): Either[BlockError, Block] = {
    for {
      _ <- validateReceipts(block, receipts)
      _ <- validateLogBloom(block, receipts)
    } yield block
  }

  /**
   * Validates [[khipu.domain.BlockHeader.transactionsRoot]] matches [[BlockBody.transactionList]]
   * based on validations stated in section 4.4.2 of http://paper.gavwood.com/
   *
   * @param block Block to validate
   * @return Block if valid, a Some otherwise
   */
  private def validateTransactionRoot(block: Block): Either[BlockError, Block] = {
    val isValid = MptListValidator.isValid[SignedTransaction](
      block.header.transactionsRoot.bytes,
      block.body.transactionList,
      SignedTransaction.byteArraySerializable
    )

    if (isValid) {
      Right(block)
    } else {
      //println(s"====\n${block.header}\n${block.body.transactionList.map(x => (x, x.hashAsHexString))}\n====\n")
      Left(BlockTransactionsHashError)
    }
  }

  /**
   * Validates [[BlockBody.uncleNodesList]] against [[khipu.domain.BlockHeader.ommersHash]]
   * based on validations stated in section 4.4.2 of http://paper.gavwood.com/
   *
   * @param block Block to validate
   * @return Block if valid, a Some otherwise
   */
  private def validateOmmersHash(block: Block): Either[BlockError, Block] = {
    import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
    val encodedOmmers: Array[Byte] = block.body.uncleNodesList.toBytes
    if (java.util.Arrays.equals(crypto.kec256(encodedOmmers), block.header.ommersHash.bytes)) Right(block)
    else Left(BlockOmmersHashError)
  }

  /**
   * Validates [[Receipt]] against [[khipu.domain.BlockHeader.receiptsRoot]]
   * based on validations stated in section 4.4.2 of http://paper.gavwood.com/
   *
   * @param block    Block to validate
   * @param receipts Receipts to use
   * @return
   */
  private def validateReceipts(block: Block, receipts: Seq[Receipt]): Either[BlockError, Block] = {
    val isValid = MptListValidator.isValid[Receipt](
      block.header.receiptsRoot.bytes,
      receipts,
      Receipt.byteArraySerializable
    )

    if (isValid) {
      Right(block)
    } else {
      Left(BlockReceiptsHashError)
    }
  }

  /**
   * Validates [[khipu.domain.BlockHeader.logsBloom]] against [[Receipt.logsBloomFilter]]
   * based on validations stated in section 4.4.2 of http://paper.gavwood.com/
   *
   * @param block    Block to validate
   * @param receipts Receipts to use
   * @return
   */
  private def validateLogBloom(block: Block, receipts: Seq[Receipt]): Either[BlockError, Block] = {
    val logsBloomOr = if (receipts.isEmpty) {
      BloomFilter.emptyBloomFilter
    } else {
      ByteString(BytesUtil.or(receipts.map(_.logsBloomFilter.toArray): _*))
    }

    if (logsBloomOr == block.header.logsBloom) {
      Right(block)
    } else {
      Left(BlockLogBloomError)
    }
  }

  sealed trait BlockError
  case object BlockTransactionsHashError extends BlockError
  case object BlockOmmersHashError extends BlockError
  case object BlockReceiptsHashError extends BlockError
  case object BlockLogBloomError extends BlockError

}
