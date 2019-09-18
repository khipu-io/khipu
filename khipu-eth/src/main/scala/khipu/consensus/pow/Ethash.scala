package khipu.consensus.pow

import khipu.config.MiningConfig
import khipu.consensus.pow.EthashAlgo.ProofOfWork
import khipu.crypto
import khipu.domain.Block
import khipu.domain.BlockHeader
import khipu.util.BytesUtil
import khipu.util.FastByteComparisons
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.math.BigInteger
import java.util.Random
import org.slf4j.LoggerFactory

/**
 * More high level validator/miner class which keeps a cache for the last requested block epoch
 *
 */
object Ethash {
  val MAX_NONCE = BigInteger.valueOf(2).pow(256)

  var cachedInstance: Ethash = null

  def fileCacheEnabled = true

  /**
   * Returns instance for the specified block number
   * either from cache or calculates a new one
   */
  def getForBlock(config: MiningConfig, ethashAlgo: EthashAlgo, blockNumber: Long): Ethash = {
    val params = config.ethashParams
    val epoch = blockNumber / params.EPOCH_LENGTH
    if (cachedInstance == null || epoch != cachedInstance.epoch) {
      cachedInstance = new Ethash(config, ethashAlgo, epoch * params.EPOCH_LENGTH)
    }
    cachedInstance
  }

  //final case class MiningResult(nonce: Long, digest: Array[Byte], block: Block)
  sealed trait MiningResult {
    def nTriedHashes: Int
  }
  final case class MiningSuccessful(nTriedHashes: Int, pow: ProofOfWork, nonce: Array[Byte]) extends MiningResult
  final case class MiningUnsuccessful(nTriedHashes: Int) extends MiningResult

}
final class Ethash(config: MiningConfig, ethashAlgo: EthashAlgo, blockNumber: Long) {
  import Ethash._

  private val log = LoggerFactory.getLogger("mine")

  val epoch = blockNumber / ethashAlgo.params.EPOCH_LENGTH
  val mineRounds = config.mineRounds

  private val startNonce = config.startNonce

  private var cacheLight: Array[Int] = null
  private var fullData: Array[Int] = null

  def getCacheLight(): Array[Int] = synchronized {
    if (cacheLight == null) {
      getCacheLightImpl()
    }

    cacheLight
  }

  /**
   * Checks whether light DAG is already generated and loads it
   * from cache, otherwise generates it
   * @return  Light DAG
   */
  private def getCacheLightImpl(): Array[Int] = synchronized {
    if (cacheLight == null) {
      val file = new File(config.ethashDir, "mine-dag-light.dat")
      if (fileCacheEnabled && file.canRead) {
        try {
          val ois = new ObjectInputStream(new FileInputStream(file))
          log.info("Loading light dataset from " + file.getAbsolutePath)
          val bNum = ois.readLong()
          if (bNum == blockNumber) {
            cacheLight = ois.readObject().asInstanceOf[Array[Int]]
            log.info("Dataset loaded.");
          } else {
            log.info("Dataset block number miss: " + bNum + " != " + blockNumber)
          }
        } catch {
          case e: IOException            => throw new RuntimeException(e)
          case e: ClassNotFoundException => throw new RuntimeException(e)
        }
      }

      if (cacheLight == null) {
        log.info("Calculating light dataset...")
        cacheLight = ethashAlgo.makeCache(ethashAlgo.params.getCacheSize(blockNumber), ethashAlgo.getSeedHash(blockNumber))
        log.info("Light dataset calculated.")

        if (fileCacheEnabled) {
          file.getParentFile.mkdirs()
          try {
            val oos = new ObjectOutputStream(new FileOutputStream(file))
            log.info("Writing light dataset to " + file.getAbsolutePath)
            oos.writeLong(blockNumber)
            oos.writeObject(cacheLight)
          } catch {
            case e: IOException => throw new RuntimeException(e)
          }
        }
        //fireDatatasetStatusUpdate(LIGHT_DATASET_GENERATED);
      }
    }

    cacheLight
  }

  def getFullDataset(): Array[Int] = synchronized {
    if (fullData == null) {
      val file = new File(config.ethashDir, "mine-dag.dat")
      if (fileCacheEnabled && file.canRead) {
        try {
          val ois = new ObjectInputStream(new FileInputStream(file))
          log.info("Loading dataset from " + file.getAbsolutePath)
          val bNum = ois.readLong()
          if (bNum == blockNumber) {
            fullData = ois.readObject().asInstanceOf[Array[Int]]
            log.info("Dataset loaded.")
          } else {
            log.info("Dataset block number miss: " + bNum + " != " + blockNumber)
          }
        } catch {
          case e: IOException            => throw new RuntimeException(e)
          case e: ClassNotFoundException => throw new RuntimeException(e)
        }
      }

      if (fullData == null) {
        log.info("Calculating full dataset...");
        val cacheLight = getCacheLightImpl()
        fullData = ethashAlgo.calcDataset(getFullSize, cacheLight)
        log.info("Full dataset calculated.")

        if (fileCacheEnabled) {
          file.getParentFile.mkdirs()
          try {
            val oos = new ObjectOutputStream(new FileOutputStream(file))
            log.info("Writing dataset to " + file.getAbsolutePath)
            oos.writeLong(blockNumber)
            oos.writeObject(fullData)
          } catch {
            case e: IOException => throw new RuntimeException(e)
          }
        }
      }
    }

    fullData
  }

  def getFullData = fullData

  private def getFullSize: Long = ethashAlgo.params.getFullSize(blockNumber)

  /**
   *  See {@link EthashAlgo#hashimotoLight}
   */
  def hashimotoLight(header: BlockHeader, nonce: Long): ProofOfWork = {
    hashimotoLight(header, BytesUtil.longToBytes(nonce))
  }

  private def hashimotoLight(header: BlockHeader, nonce: Array[Byte]): ProofOfWork = {
    ethashAlgo.hashimotoLight(getFullSize, getCacheLight, crypto.sha3(header.getEncodedWithoutNonce), nonce)
  }

  /**
   *  See {@link EthashAlgo#hashimotoFull}
   */
  def hashimotoFull(header: BlockHeader, nonce: Long): ProofOfWork = {
    ethashAlgo.hashimotoFull(getFullSize, getFullDataset, crypto.sha3(header.getEncodedWithoutNonce), BytesUtil.longToBytes(nonce))
  }

  /**
   *  Mines the nonce for the specified Block with difficulty BlockHeader.getDifficulty()
   *  When mined the Block 'nonce' and 'mixHash' fields are updated
   *  Uses the full dataset i.e. it faster but takes > 1Gb of memory and may
   *  take up to 10 mins for starting up (depending on whether the dataset was cached)
   *
   *  @param block The block to mine. The difficulty is taken from the block header
   *               This block is updated when mined
   *  @return the task which may be cancelled. On success returns nonce
   */
  def mine(block: Block): MiningResult = {
    val taskStartNonce = if (startNonce >= 0) BigInteger.valueOf(startNonce) else new BigInteger(64, new Random())
    val threadStartNonce = taskStartNonce.add(BigInteger.valueOf(0x100000000L))
    val blockHeaderTruncHash = crypto.kec256(block.header.getEncodedWithoutNonce)
    mine(getFullSize, getFullDataset, blockHeaderTruncHash, block.header.difficulty.longValue, mineRounds, threadStartNonce) match {
      case MiningSuccessful(nTriedHashes, _, nonce) =>
        // TODO Why again?
        val pow = ethashAlgo.hashimotoLight(getFullSize, getCacheLight, crypto.sha3(block.header.getEncodedWithoutNonce), nonce)
        MiningSuccessful(nTriedHashes, pow, nonce)
      case x => x

    }
  }

  /**
   *  Mines the nonce for the specified Block with difficulty BlockHeader.getDifficulty()
   *  When mined the Block 'nonce' and 'mixHash' fields are updated
   *  Uses the light cache i.e. it slower but takes only ~16Mb of memory and takes less
   *  time to start up
   *
   *  @param block The block to mine. The difficulty is taken from the block header
   *               This block is updated when mined
   *  @return the task which may be cancelled. On success returns nonce
   */
  def mineLight(block: Block): MiningResult = {
    val taskStartNonce = if (startNonce >= 0) BigInteger.valueOf(startNonce) else new BigInteger(64, new Random())
    val threadStartNonce = taskStartNonce.add(BigInteger.valueOf(0x100000000L))
    val blockHeaderTruncHash = crypto.kec256(block.header.getEncodedWithoutNonce)
    mineLight(getFullSize, getCacheLight, blockHeaderTruncHash, block.header.difficulty.longValue, mineRounds, threadStartNonce) match {
      case MiningSuccessful(nTriedHashes, _, nonce) =>
        // TODO Why again?
        val pow = ethashAlgo.hashimotoLight(getFullSize, getCacheLight, crypto.sha3(block.header.getEncodedWithoutNonce), nonce)
        MiningSuccessful(nTriedHashes, pow, nonce)
      case x => x
    }
  }

  private def mine(fullSize: Long, dataset: Array[Int], blockHeaderTruncHash: Array[Byte], difficulty: Long, numRounds: Int): MiningResult = {
    mine(fullSize, dataset, blockHeaderTruncHash, difficulty, numRounds, new BigInteger(64, new Random()))
  }

  private def mine(fullSize: Long, dataset: Array[Int], blockHeaderTruncHash: Array[Byte], difficulty: Long, numRounds: Int, startNonce: BigInteger): MiningResult = {
    val target = MAX_NONCE.divide(BigInteger.valueOf(difficulty))
    var round = 0
    var nonce = startNonce
    var gotPoW: Option[ProofOfWork] = None
    while (!gotPoW.isEmpty && round < numRounds) {
      nonce = nonce.add(BigInteger.ONE)
      val nonceBytes = BytesUtil.padLeft(toUnsignedByteArrayNonce(nonce), 8, 0.toByte)
      val pow = ethashAlgo.hashimotoFull(fullSize, dataset, blockHeaderTruncHash, nonceBytes)
      val h = new BigInteger(1, pow.difficultyBoundary)
      if (h.compareTo(target) < 0) {
        gotPoW = Some(pow)
      }
      round += 1
    }

    gotPoW match {
      case Some(pow) => MiningSuccessful(round, pow, toUnsignedByteArrayNonce(nonce))
      case None      => MiningUnsuccessful(round)
    }
  }

  /**
   * This the slower miner version which uses only cache thus taking much less memory than
   * regular {@link #mine} method
   */
  private def mineLight(fullSize: Long, cache: Array[Int], blockHeaderTruncHash: Array[Byte], difficulty: Long, numRounds: Int): MiningResult = {
    mineLight(fullSize, cache, blockHeaderTruncHash, difficulty, numRounds, new BigInteger(64, new Random()))
  }

  private def mineLight(fullSize: Long, cache: Array[Int], blockHeaderTruncHash: Array[Byte], difficulty: Long, numRounds: Int, startNonce: BigInteger): MiningResult = {
    val target = MAX_NONCE.divide(BigInteger.valueOf(difficulty))
    var round = 0
    var nonce = startNonce
    var gotPoW: Option[ProofOfWork] = None
    while (!gotPoW.isEmpty && round < numRounds) {
      nonce = nonce.add(BigInteger.ONE)
      val nonceBytes = BytesUtil.padLeft(toUnsignedByteArrayNonce(nonce), 8, 0.toByte)
      val pow = ethashAlgo.hashimotoLight(fullSize, cache, blockHeaderTruncHash, nonceBytes)
      val h = new BigInteger(1, pow.difficultyBoundary)
      if (h.compareTo(target) < 0) {
        gotPoW = Some(pow)
      }
      round += 1
    }

    gotPoW match {
      case Some(pow) => MiningSuccessful(round, pow, toUnsignedByteArrayNonce(nonce))
      case None      => MiningUnsuccessful(round)
    }
  }

  private def toUnsignedByteArrayNonce(nouce: BigInteger): Array[Byte] = {
    val asByteArray = nouce.toByteArray
    if (asByteArray.head == 0) {
      asByteArray.tail
    } else {
      asByteArray
    }
  }

  /**
   *  Validates the BlockHeader against its getDifficulty() and getNonce()
   */
  def validate(header: BlockHeader): Boolean = {
    val boundary = header.getPowBoundary
    val hash = hashimotoLight(header, header.nonce.toArray).difficultyBoundary

    println(s"boundary: ${boundary.mkString(", ")}, hash: ${hash.mkString(", ")}")
    FastByteComparisons.compareTo(hash, 0, 32, boundary, 0, 32) < 0
  }
}
