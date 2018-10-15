package khipu.validators

import java.math.BigInteger
import khipu.Hash
import khipu.UInt256
import khipu.crypto
import khipu.domain.Block
import khipu.domain.BlockHeader
import khipu.domain.Blockchain
import khipu.domain.DifficultyCalculator
import khipu.util.BlockchainConfig

object BlockHeaderValidator {
  trait I {
    def validate(blockHeader: BlockHeader, blockchain: Blockchain, validatingBlocks: Map[Hash, Block]): Either[BlockHeaderError, BlockHeader]
  }
}

final class BlockHeaderValidator(blockchainConfig: BlockchainConfig) extends BlockHeaderValidator.I {

  val MaxExtraDataSize: Int = 32
  val GasLimitBoundDivisor: Int = 1024
  val MinGasLimit: Long = 5000L //Although the paper states this value is 125000, on the different clients 5000 is used
  val difficulty = new DifficultyCalculator(blockchainConfig)
  import BlockHeaderError._

  /**
   * This method allows validate a BlockHeader (stated on
   * section 4.4.4 of http://paper.gavwood.com/).
   *
   * @param blockHeader BlockHeader to validate.
   * @param blockchain from where the header of the parent of the block will be fetched.
   */
  def validate(blockHeader: BlockHeader, blockchain: Blockchain, validatingBlocks: Map[Hash, Block]): Either[BlockHeaderError, BlockHeader] = {
    for {
      blockHeaderParent <- getBlockParentHeader(blockHeader, blockchain, validatingBlocks)
      _ <- validate(blockHeader, blockHeaderParent)
    } yield blockHeader
  }

  /**
   * This method allows validate a BlockHeader (stated on
   * section 4.4.4 of http://paper.gavwood.com/).
   *
   * @param blockHeader BlockHeader to validate.
   * @param parentHeader BlockHeader of the parent of the block to validate.
   */
  private def validate(blockHeader: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    for {
      _ <- validateExtraData(blockHeader)
      _ <- validateTimestamp(blockHeader, parentHeader)
      _ <- validateDifficulty(blockHeader, parentHeader)
      _ <- validateGasUsed(blockHeader)
      _ <- validateGasLimit(blockHeader, parentHeader)
      _ <- validateNumber(blockHeader, parentHeader)
      _ <- validatePoW(blockHeader)
    } yield blockHeader
  }

  /**
   * Validates [[khipu.domain.BlockHeader.extraData]] length
   * based on validations stated in section 4.4.4 of http://paper.gavwood.com/
   *
   * @param blockHeader BlockHeader to validate.
   * @return BlockHeader if valid, an [[HeaderExtraDataError]] otherwise
   */
  private def validateExtraData(blockHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] =
    if (blockHeader.extraData.length <= MaxExtraDataSize) {
      Right(blockHeader)
    } else {
      Left(HeaderExtraDataError)
    }

  /**
   * Validates [[khipu.domain.BlockHeader.unixTimestamp]] is greater than the one of its parent
   * based on validations stated in section 4.4.4 of http://paper.gavwood.com/
   *
   * @param blockHeader BlockHeader to validate.
   * @param parentHeader BlockHeader of the parent of the block to validate.
   * @return BlockHeader if valid, an [[HeaderTimestampError]] otherwise
   */
  private def validateTimestamp(blockHeader: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] =
    if (blockHeader.unixTimestamp > parentHeader.unixTimestamp) {
      Right(blockHeader)
    } else {
      Left(HeaderTimestampError)
    }

  /**
   * Validates [[khipu.domain.BlockHeader.difficulty]] is correct
   * based on validations stated in section 4.4.4 of http://paper.gavwood.com/
   *
   * @param blockHeader BlockHeader to validate.
   * @param parentHeader BlockHeader of the parent of the block to validate.
   * @return BlockHeader if valid, an [[HeaderDifficultyError]] otherwise
   */
  private def validateDifficulty(blockHeader: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    val d = difficulty.calculateDifficulty(blockHeader, parentHeader)
    if (d.compareTo(blockHeader.difficulty) == 0) {
      Right(blockHeader)
    } else {
      Left(HeaderDifficultyError(blockHeader.difficulty, d))
    }
  }

  /**
   * Validates [[khipu.domain.BlockHeader.gasUsed]] is not greater than [[khipu.domain.BlockHeader.gasLimit]]
   * based on validations stated in section 4.4.4 of http://paper.gavwood.com/
   *
   * @param blockHeader BlockHeader to validate.
   * @return BlockHeader if valid, an [[HeaderGasUsedError]] otherwise
   */
  private def validateGasUsed(blockHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] =
    if (blockHeader.gasUsed >= 0 && blockHeader.gasUsed <= blockHeader.gasLimit) {
      Right(blockHeader)
    } else {
      Left(HeaderGasUsedError)
    }

  /**
   * Validates [[khipu.domain.BlockHeader.gasLimit]] follows the restrictions based on its parent gasLimit
   * based on validations stated in section 4.4.4 of http://paper.gavwood.com/
   *
   * @param blockHeader BlockHeader to validate.
   * @param parentHeader BlockHeader of the parent of the block to validate.
   * @return BlockHeader if valid, an [[HeaderGasLimitError]] otherwise
   */
  private def validateGasLimit(blockHeader: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    val gasLimitDiff = (blockHeader.gasLimit - parentHeader.gasLimit).abs
    val gasLimitDiffLimit = parentHeader.gasLimit / GasLimitBoundDivisor
    if (gasLimitDiff < gasLimitDiffLimit && blockHeader.gasLimit >= MinGasLimit) {
      Right(blockHeader)
    } else {
      Left(HeaderGasLimitError)
    }
  }

  /**
   * Validates [[khipu.domain.BlockHeader.number]] is the next one after its parents number
   * based on validations stated in section 4.4.4 of http://paper.gavwood.com/
   *
   * @param blockHeader BlockHeader to validate.
   * @param parentHeader BlockHeader of the parent of the block to validate.
   * @return BlockHeader if valid, an [[HeaderNumberError]] otherwise
   */
  private def validateNumber(blockHeader: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] =
    if (blockHeader.number == parentHeader.number + 1) {
      Right(blockHeader)
    } else {
      Left(HeaderNumberError)
    }

  /**
   * Validates [[khipu.domain.BlockHeader.nonce]] and [[khipu.domain.BlockHeader.mixHash]] are correct
   * based on validations stated in section 4.4.4 of http://paper.gavwood.com/
   *
   * @param blockHeader BlockHeader to validate.
   * @return BlockHeader if valid, an [[HeaderPoWError]] otherwise
   */
  //FIXME: Simple PoW validation without using DAG [EC-88]
  private def validatePoW(blockHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    val powBoundary = UInt256.Modulus / blockHeader.difficulty
    val powValue = UInt256(new BigInteger(1, calculatePoWValue(blockHeader).toArray))
    if (powValue.compareTo(powBoundary) <= 0) {
      Right(blockHeader)
    } else {
      Left(HeaderPoWError)
    }
  }

  private def calculatePoWValue(blockHeader: BlockHeader): Array[Byte] = {
    val nonceReverted = blockHeader.nonce.reverse
    val hashBlockWithoutNonce = crypto.kec256(BlockHeader.getEncodedWithoutNonce(blockHeader))
    val seedHash = crypto.kec512(hashBlockWithoutNonce ++ nonceReverted.toArray[Byte])

    crypto.kec256(seedHash ++ blockHeader.mixHash.bytes)
  }

  /**
   * Retrieves the header of the parent of a block from the Blockchain, if it exists.
   *
   * @param blockHeader BlockHeader whose parent we want to fetch.
   * @param blockchain where the header of the parent of the block will be fetched.
   * @return the BlockHeader of the parent if it exists, an [[HeaderParentNotFoundError]] otherwise
   */
  private def getBlockParentHeader(blockHeader: BlockHeader, blockchain: Blockchain, validatingBlocks: Map[Hash, Block]): Either[BlockHeaderError, BlockHeader] = {
    validatingBlocks.get(blockHeader.parentHash).map(_.header).orElse(blockchain.getBlockHeaderByHash(blockHeader.parentHash)) match {
      case Some(blockParentHeader) => Right(blockParentHeader)
      case None                    => Left(HeaderParentNotFoundError)
    }
  }
}

sealed trait BlockHeaderError
object BlockHeaderError {
  case object HeaderParentNotFoundError extends BlockHeaderError
  case object HeaderExtraDataError extends BlockHeaderError
  case object HeaderTimestampError extends BlockHeaderError
  final case class HeaderDifficultyError(header: UInt256, calculated: UInt256) extends BlockHeaderError
  case object HeaderGasUsedError extends BlockHeaderError
  case object HeaderGasLimitError extends BlockHeaderError
  case object HeaderNumberError extends BlockHeaderError
  case object HeaderPoWError extends BlockHeaderError
}
