package khipu.validators

import khipu.Hash
import khipu.DataWord
import khipu.config.BlockchainConfig
import khipu.consensus.pow.EthashAlgo
import khipu.crypto
import khipu.domain.Block
import khipu.domain.BlockHeader
import khipu.domain.Blockchain
import khipu.domain.DifficultyCalculator

object BlockHeaderValidator {
  trait I {
    def validate(blockHeader: BlockHeader, blockchain: Blockchain, validatingBlocks: Map[Hash, Block]): Either[BlockHeaderError, BlockHeader]
    def syncDone(): Unit
  }

  val MaxExtraDataSize: Int = 32
  val GasLimitBoundDivisor: Int = 1024
  val MinGasLimit: Long = 5000L //Although the paper states this value is 125000, on the different clients 5000 is used
  val MaxGasLimit = Long.MaxValue // max gasLimit is equal 2^63-1 according to EIP106
  val MaxPowCaches: Int = 2 // maximum number of epochs for which PoW cache is stored in memory

  final class PowCacheData(val cache: Array[Int], val dagSize: Long)

  // FIXME [EC-331]: this is used to speed up ETS Blockchain tests. All blocks in those tests have low numbers (1, 2, 3 ...)
  // so keeping the cache for epoch 0 avoids recalculating it for each individual test. The difference in test runtime
  // can be dramatic - full suite: 26 hours vs 21 minutes on same machine
  // It might be desirable to find a better solution for this - one that doesn't keep this cache unnecessarily
  //  lazy val epoch0PowCache = new PowCacheData(
  //    cache = Ethash.makeCache(0),
  //    dagSize = Ethash.dagSize(0))
}

final class BlockHeaderValidator(blockchainConfig: BlockchainConfig, ethashAlgo: EthashAlgo) extends BlockHeaderValidator.I {
  import BlockHeaderValidator._
  import BlockHeaderError._

  val difficulty = new DifficultyCalculator(blockchainConfig)
  val powValidator = new PoWValidator(ethashAlgo, blockchainConfig.powValidateMode)

  def syncDone() {
    powValidator.syncDone = true
  }

  /**
   * This method allows validate a BlockHeader (stated on
   * section 4.4.4 of http://paper.gavwood.com/).
   *
   * @param blockHeader BlockHeader to validate.
   * @param blockchain from where the header of the parent of the block will be fetched.
   */
  def validate(header: BlockHeader, blockchain: Blockchain, validatingBlocks: Map[Hash, Block]): Either[BlockHeaderError, BlockHeader] = {
    for {
      blockHeaderParent <- getBlockParentHeader(header, blockchain, validatingBlocks)
      _ <- validate(header, blockHeaderParent)
    } yield header
  }

  /**
   * This method allows validate a BlockHeader (stated on
   * section 4.4.4 of http://paper.gavwood.com/).
   *
   * @param blockHeader BlockHeader to validate.
   * @param parentHeader BlockHeader of the parent of the block to validate.
   */
  private def validate(header: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    for {
      _ <- validateExtraData(header)
      _ <- validateTimestamp(header, parentHeader)
      _ <- validateDifficulty(header, parentHeader)
      _ <- validateGasUsed(header)
      _ <- validateGasLimit(header, parentHeader)
      _ <- validateNumber(header, parentHeader)
      _ <- validatePoW(header)
    } yield header
  }

  /**
   * Validates [[khipu.domain.BlockHeader.extraData]] length
   * based on validations stated in section 4.4.4 of http://paper.gavwood.com/
   *
   * @param blockHeader BlockHeader to validate.
   * @return BlockHeader if valid, an [[HeaderExtraDataError]] otherwise
   */
  private def validateExtraData(header: BlockHeader): Either[BlockHeaderError, BlockHeader] =
    if (header.extraData.length <= MaxExtraDataSize) {
      Right(header)
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
  private def validateTimestamp(header: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] =
    if (header.unixTimestamp > parentHeader.unixTimestamp) {
      Right(header)
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
  private def validateDifficulty(header: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    val d = difficulty.calculateDifficulty(header, parentHeader)
    if (d.compareTo(header.difficulty) == 0) {
      Right(header)
    } else {
      Left(HeaderDifficultyError(header.difficulty, d))
    }
  }

  /**
   * Validates [[khipu.domain.BlockHeader.gasUsed]] is not greater than [[khipu.domain.BlockHeader.gasLimit]]
   * based on validations stated in section 4.4.4 of http://paper.gavwood.com/
   *
   * @param blockHeader BlockHeader to validate.
   * @return BlockHeader if valid, an [[HeaderGasUsedError]] otherwise
   */
  private def validateGasUsed(header: BlockHeader): Either[BlockHeaderError, BlockHeader] =
    if (header.gasUsed >= 0 && header.gasUsed <= header.gasLimit) {
      Right(header)
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
  private def validateGasLimit(header: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    val gasLimitDiff = (header.gasLimit - parentHeader.gasLimit).abs
    val gasLimitDiffLimit = parentHeader.gasLimit / GasLimitBoundDivisor
    if (gasLimitDiff < gasLimitDiffLimit && header.gasLimit >= MinGasLimit) {
      Right(header)
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
  private def validateNumber(header: BlockHeader, parentHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] =
    if (header.number == parentHeader.number + 1) {
      Right(header)
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
  private def validatePoW(header: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    powValidator.validate(header)
  }

  /**
   * Retrieves the header of the parent of a block from the Blockchain, if it exists.
   *
   * @param blockHeader BlockHeader whose parent we want to fetch.
   * @param blockchain where the header of the parent of the block will be fetched.
   * @return the BlockHeader of the parent if it exists, an [[HeaderParentNotFoundError]] otherwise
   */
  private def getBlockParentHeader(header: BlockHeader, blockchain: Blockchain, validatingBlocks: Map[Hash, Block]): Either[BlockHeaderError, BlockHeader] = {
    validatingBlocks.get(header.parentHash).map(_.header).orElse(blockchain.getBlockHeaderByHash(header.parentHash)) match {
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
  final case class HeaderDifficultyError(header: DataWord, calculated: DataWord) extends BlockHeaderError
  case object HeaderGasUsedError extends BlockHeaderError
  case object HeaderGasLimitError extends BlockHeaderError
  case object HeaderNumberError extends BlockHeaderError
  final case class HeaderPoWError(reason: String) extends BlockHeaderError
}
