package khipu.validators

import java.math.BigInteger
import java.util.Random
import khipu.DataWord
import khipu.consensus.pow.EthashAlgo
import khipu.consensus.pow.EthashCache
import khipu.domain.BlockHeader
import khipu.util.FastByteComparisons
import org.slf4j.LoggerFactory

/**
 * Runs block header validation against Ethash dataset.
 *
 * <p>
 *     Configurable to work in several modes:
 *     <ul>
 *         <li> fake - partial checks without verification against Ethash dataset
 *         <li> strict - full check for each block
 *         <li> mixed  - run full check for each block if main import flow during short sync,
 *                       run full check in random fashion (<code>1/{@link #MIX_DENOMINATOR}</code> blocks are checked)
 *                                during long sync, fast sync headers and blocks downloading
 *
 */
object PoWValidator {
  private val MIX_DENOMINATOR = 5

  object Mode {
    def values = Set(Strict, Mixed, Fake)
    def parse(name: String, defaultMode: Mode): Mode = {
      values.find(_.name == name.toLowerCase).getOrElse(defaultMode)
    }
  }
  sealed trait Mode { def name: String }
  case object Fake extends Mode { val name = "fake" }
  case object Mixed extends Mode { val name = "mixed" }
  case object Strict extends Mode { val name = "strict" }

  sealed trait ChainType { def isSide: Boolean }
  /** main chain, cache updates are stick to best block events, requires listener */
  case object Main extends ChainType { def isSide = false }
  /** side chain, cache is triggered each validation attempt, no listener required */
  case object Direct extends ChainType { def isSide = true }
  /** side chain with reverted validation order */
  case object Reverse extends ChainType { def isSide = true }
}
final class PoWValidator(ethashAlgo: EthashAlgo, mode: PoWValidator.Mode, chain: PoWValidator.ChainType = PoWValidator.Main) {
  import PoWValidator._
  private val log = LoggerFactory.getLogger("blockchain")

  private val ethashCache: EthashCache = {
    val cacheOrder = chain match {
      case Reverse => EthashCache.Reverse
      case _       => EthashCache.Direct
    }

    new EthashCache(cacheOrder, ethashAlgo)
  }

  private val rnd = new Random()

  var syncDone: Boolean = false

  def validate(header: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    if (header.number == 0) {
      Right(header)
    } else {
      // trigger cache for side chains before mixed mode condition
      if (chain.isSide) {
        ethashCache.preCache(header.number)
      }

      // mixed mode payload
      mode match {
        case Fake =>
          simpleValidate(header)
        case Mixed if !syncDone && rnd.nextInt(100) % MIX_DENOMINATOR > 0 =>
          simpleValidate(header)
        case _ =>
          try {
            ethashCache.ethashWorkFor(header, header.nonce.toArray, cachedOnly = true) match {
              case Some(pow) =>
                log.debug(s"ethash found for ${header.number}")
                if (!FastByteComparisons.equal(pow.mixHash, header.mixHash.bytes)) {
                  Left(BlockHeaderError.HeaderPoWError(s"header's mixHash ${header.mixHash} doesn't match pow mixHash ${khipu.Hash(pow.mixHash)}, ${header.number}"))
                } else if (FastByteComparisons.compareTo(pow.difficultyBoundary, 0, 32, header.getPowBoundary, 0, 32) > 0) {
                  Left(BlockHeaderError.HeaderPoWError(s"header's powBoundary ${header.getPowBoundary} <= pow ${pow.difficultyBoundary}, ${header.number}"))
                } else {
                  Right(header)
                }
              case None => // no cache for the epoch? fallback to fake rule
                log.debug(s"ethash not found for ${header.number}, bypass it")
                simpleValidate(header)
            }
          } catch {
            case e: Exception =>
              Left(BlockHeaderError.HeaderPoWError(s"Failed to verify ethash work for block ${header.number}"))
          }
      }
    }
  }

  private def simpleValidate(blockHeader: BlockHeader): Either[BlockHeaderError, BlockHeader] = {
    val powBoundary = DataWord.Modulus / blockHeader.difficulty
    val powValue = DataWord(new BigInteger(1, blockHeader.calculatePoWValue))
    if (powValue.compareTo(powBoundary) <= 0) {
      Right(blockHeader)
    } else {
      Left(BlockHeaderError.HeaderPoWError("proofValue > header.getPowBoundary"))
    }
  }
}
