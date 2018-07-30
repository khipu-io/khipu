package khipu.validators

import khipu.Hash
import khipu.domain.Block
import khipu.domain.Blockchain
import khipu.domain.BlockHeader
import khipu.util.BlockchainConfig
import khipu.validators.OmmersValidator.OmmersError
import khipu.validators.OmmersValidator.OmmersError._

object OmmersValidator {
  trait I {
    def validate(blockNumber: Long, ommers: Seq[BlockHeader], blockchain: Blockchain, validatingBlocks: Map[Hash, Block]): Either[OmmersError, Unit]
  }

  sealed trait OmmersError
  object OmmersError {
    case object OmmersLengthError extends OmmersError
    case object OmmersNotValidError extends OmmersError
    case object OmmersUsedBeforeError extends OmmersError
    case object OmmersAncestorsError extends OmmersError
    case object OmmersDuplicatedError extends OmmersError
  }
}

final class OmmersValidator(blockchainConfig: BlockchainConfig) extends OmmersValidator.I {

  val blockHeaderValidator = new BlockHeaderValidator(blockchainConfig)

  val OmmerGenerationLimit: Int = 6 //Stated on section 11.1, eq. (143) of the YP
  val OmmerSizeLimit: Int = 2

  /**
   * This method allows validating the ommers of a Block. It performs the following validations (stated on
   * section 11.1 of the YP):
   *   - OmmersValidator.validateOmmersLength
   *   - OmmersValidator.validateOmmersHeaders
   *   - OmmersValidator.validateOmmersAncestors
   * It also includes validations mentioned in the white paper (https://github.com/ethereum/wiki/wiki/White-Paper)
   * and implemented in the different ETC clients:
   *   - OmmersValidator.validateOmmersNotUsed
   *   - OmmersValidator.validateDuplicatedOmmers
   *
   * @param blockNumber    The number of the block to which the ommers belong
   * @param ommers         The list of ommers to validate
   * @param blockchain     from where the previous blocks are obtained
   * @return ommers if valid, an [[OmmersValidator.OmmersError]] otherwise
   */
  def validate(blockNumber: Long, ommers: Seq[BlockHeader], blockchain: Blockchain, validatingBlocks: Map[Hash, Block]): Either[OmmersError, Unit] = {
    for {
      _ <- validateOmmersLength(ommers)
      _ <- validateDuplicatedOmmers(ommers)
      _ <- validateOmmersHeaders(ommers, blockchain, validatingBlocks)
      searchingMap = validatingBlocks.map(x => x._2.header.number -> x._2).toMap
      _ <- validateOmmersAncestors(blockNumber, ommers, blockchain, searchingMap)
      _ <- validateOmmersNotUsed(blockNumber, ommers, blockchain, searchingMap)
    } yield ()
  }

  /**
   * Validates ommers length
   * based on validations stated in section 11.1 of the YP
   *
   * @param ommers         The list of ommers to validate
   * @return ommers if valid, an [[OmmersLengthError]] otherwise
   */
  private def validateOmmersLength(ommers: Seq[BlockHeader]): Either[OmmersError, Unit] = {
    if (ommers.length <= OmmerSizeLimit) {
      Right(())
    } else {
      Left(OmmersLengthError)
    }
  }

  /**
   * Validates that each ommer's header is valid
   * based on validations stated in section 11.1 of the YP
   *
   * @param ommers         The list of ommers to validate
   * @param blockchain     from where the ommer's parents will be obtained
   * @return ommers if valid, an [[OmmersNotValidError]] otherwise
   */
  private def validateOmmersHeaders(ommers: Seq[BlockHeader], blockchain: Blockchain, validatingBlocks: Map[Hash, Block]): Either[OmmersError, Unit] = {
    if (ommers.forall(blockHeaderValidator.validate(_, blockchain, validatingBlocks).isRight)) {
      Right(())
    } else {
      Left(OmmersNotValidError)
    }
  }

  /**
   * Validates that each ommer is not too old and that it is a sibling as one of the current block's ancestors
   * based on validations stated in section 11.1 of the YP
   *
   * @param blockNumber    The number of the block to which the ommers belong
   * @param ommers         The list of ommers to validate
   * @param blockchain     from where the ommer's parents will be obtained
   * @return ommers if valid, an [[OmmersUsedBeforeError]] otherwise
   */
  private def validateOmmersAncestors(blockNumber: Long, ommers: Seq[BlockHeader], blockchain: Blockchain, validatingBlocks: Map[Long, Block]): Either[OmmersError, Unit] = {
    val ancestorsOpt = collectAncestors(blockNumber, OmmerGenerationLimit, blockchain, validatingBlocks)

    val validOmmersAncestors: Seq[BlockHeader] => Boolean = ancestors =>
      ommers.forall { ommer =>
        val ommerIsNotAncestor = ancestors.forall(_.hash != ommer.hash)
        val ommersParentIsAncestor = ancestors.exists(_.parentHash == ommer.parentHash)
        ommerIsNotAncestor && ommersParentIsAncestor
      }

    if (ancestorsOpt.exists(validOmmersAncestors)) {
      Right(())
    } else {
      Left(OmmersAncestorsError)
    }
  }

  /**
   * Validates that each ommer was not previously used
   * based on validations stated in the white paper (https://github.com/ethereum/wiki/wiki/White-Paper)
   *
   * @param blockNumber    The number of the block to which the ommers belong
   * @param ommers         The list of ommers to validate
   * @param blockchain     from where the ommer's parents will be obtained
   * @return ommers if valid, an [[OmmersUsedBeforeError]] otherwise
   */
  private def validateOmmersNotUsed(blockNumber: Long, ommers: Seq[BlockHeader], blockchain: Blockchain, validatingBlocks: Map[Long, Block]): Either[OmmersError, Unit] = {
    val ommersFromAncestorsOpt = collectOmmersFromAncestors(blockNumber, OmmerGenerationLimit, blockchain, validatingBlocks)

    if (ommersFromAncestorsOpt.exists(ommers.intersect(_).isEmpty)) {
      Right(())
    } else {
      Left(OmmersUsedBeforeError)
    }
  }

  /**
   * Validates that there are no duplicated ommers
   * based on validations stated in the white paper (https://github.com/ethereum/wiki/wiki/White-Paper)
   *
   * @param ommers         The list of ommers to validate
   * @return ommers if valid, an [[OmmersDuplicatedError]] otherwise
   */
  private def validateDuplicatedOmmers(ommers: Seq[BlockHeader]): Either[OmmersError, Unit] = {
    if (ommers.distinct.length == ommers.length) {
      Right(())
    } else {
      Left(OmmersDuplicatedError)
    }
  }

  private def collectAncestors(blockNumber: Long, numberOfBlocks: Int, blockchain: Blockchain, validatingBlocks: Map[Long, Block]): Option[Seq[BlockHeader]] = {
    var ancestors = Vector[BlockHeader]()

    var foundNoExistHeader = false
    var i = math.max(blockNumber - numberOfBlocks, 0)
    while (i < blockNumber && !foundNoExistHeader) {
      validatingBlocks.get(i).orElse(blockchain.getBlockByNumber(i)).map(_.header) match {
        case Some(x) => ancestors :+= x
        case None    => foundNoExistHeader = true
      }
      i += 1
    }

    if (foundNoExistHeader) {
      None
    } else {
      Some(ancestors)
    }
  }

  private def collectOmmersFromAncestors(blockNumber: Long, numberOfBlocks: Int, blockchain: Blockchain, validatingBlocks: Map[Long, Block]): Option[Seq[BlockHeader]] = {
    var ommersFromAncestors = Vector[Seq[BlockHeader]]()

    var foundNoExistHeader = false
    var i = math.max(blockNumber - numberOfBlocks, 0)
    while (i < blockNumber && !foundNoExistHeader) {
      validatingBlocks.get(i).orElse(blockchain.getBlockByNumber(i)).map(_.body.uncleNodesList) match {
        case Some(xs) => ommersFromAncestors :+= xs
        case None     => foundNoExistHeader = true
      }
      i += 1
    }

    if (foundNoExistHeader) {
      None
    } else {
      Some(ommersFromAncestors.flatten)
    }
  }

  private def ancestorsBlockNumbers(blockNumber: Long, numberOfBlocks: Int): Seq[Long] =
    math.max(blockNumber - numberOfBlocks, 0) until blockNumber
}
