package khipu.consensus.pow

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock
import khipu.consensus.pow.EthashAlgo.ProofOfWork
import khipu.crypto
import khipu.domain.BlockHeader
import org.slf4j.LoggerFactory

/**
 * Maintains datasets of {@link EthashAlgo} for verification purposes.
 *
 * <p>
 *     Takes a burden of light dataset caching and provides convenient interface for keeping this cache up to date.
 *     Featured with full dataset lookup if such dataset is available (created for mining purposes),
 *     full dataset usage increases verification speed dramatically.
 *
 * <p>
 *     Entry point is {@link #ethashWorkFor(BlockHeader, byte[], boolean)}
 *
 * <p>
 *     Cache management interface: {@link #preCache(long)}, {@link CacheOrder}
 */
object EthashCache {
  private val MAX_CACHED_EPOCHS = 2
  private val LONGEST_CHAIN = 192

  sealed trait CacheOrder
  /** cache is updated to fit main import process, toward big numbers */
  case object Direct extends CacheOrder
  /** for maintaining reverse header validation, cache is updated to fit block validation with decreasing numbers */
  case object Reverse extends CacheOrder
}
final class EthashCache(cacheOrder: EthashCache.CacheOrder, ethashAlgo: EthashAlgo) {
  import EthashCache._

  private val log = LoggerFactory.getLogger("ethash")

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  val caches = new CopyOnWriteArrayList[Cache]()
  var lastCachedEpoch = -1L

  private val cacheStrategy = {
    cacheOrder match {
      case Direct  => new DirectCache()
      case Reverse => new ReverseCache()
    }
  }

  class Cache(blockNumber: Long) {
    val epoch = EthashCache.this.epoch(blockNumber)

    val dataset = {
      val start = System.currentTimeMillis
      val seed = ethashAlgo.getSeedHash(blockNumber)
      val size = ethashAlgo.params.getCacheSize(blockNumber)
      val x = ethashAlgo.makeCache(size, seed)
      log.info(s"made ethash cache for $blockNumber in ${System.currentTimeMillis - start}ms")
      x
    }

    def isFor(blockNumber: Long): Boolean = {
      epoch == EthashCache.this.epoch(blockNumber)
    }
  }

  private var currentCache: Option[Cache] = None

  /**
   * Calculates ethash results for particular block and nonce.
   *
   * @param cachedOnly flag that defined behavior of method when dataset has not been cached:
   *                   if set to true  - returns null immediately
   *                   if set to false - generates dataset for block epoch and then runs calculations on it
   */
  @throws(classOf[Exception])
  def ethashWorkFor(header: BlockHeader, nonce: Array[Byte], cachedOnly: Boolean): Option[ProofOfWork] = {
    val fullSize = ethashAlgo.params.getFullSize(header.number)
    val hashWithoutNonce = crypto.kec256(header.getEncodedWithoutNonce)

    // lookup with full dataset if it's available
    val cachedInstance = Ethash.cachedInstance
    if (cachedInstance != null && cachedInstance.epoch == epoch(header.number) && cachedInstance.getFullData != null) {
      Some(ethashAlgo.hashimotoFull(fullSize, cachedInstance.getFullData, hashWithoutNonce, nonce))
    } else {
      getCachedFor(header.number) match {
        case Some(cache) =>
          Some(ethashAlgo.hashimotoLight(fullSize, cache.dataset, hashWithoutNonce, nonce))

        case None if !cachedOnly =>
          val cache = currentCache match {
            case Some(x) if x.isFor(header.number) => x
            case _ =>
              try {
                writeLock.lock()

                val x = new Cache(header.number)
                currentCache = Some(x)
                x
              } finally {
                writeLock.unlock()
              }
          }
          Some(ethashAlgo.hashimotoLight(fullSize, cache.dataset, hashWithoutNonce, nonce))

        case None =>
          None
      }
    }
  }

  def getCachedFor(blockNumber: Long): Option[Cache] = {
    try {
      readLock.lock()

      val itr = caches.iterator
      while (itr.hasNext) {
        val cache = itr.next()
        if (cache.isFor(blockNumber)) {
          return Some(cache)
        }
      }

      None
    } finally {
      readLock.unlock()
    }
  }

  def preCache(blockNumber: Long) {
    cacheStrategy.cache(blockNumber)
  }

  def epochLength: Long = ethashAlgo.params.EPOCH_LENGTH
  def epoch(blockNumber: Long): Long = blockNumber / ethashAlgo.params.EPOCH_LENGTH

  trait CacheStrategy {
    def cache(blockNumber: Long)
  }

  class ReverseCache extends CacheStrategy {
    def cache(blockNumber: Long) {
      // reset cache if it's outdated
      if (epoch(blockNumber) < lastCachedEpoch || lastCachedEpoch < 0) {
        reset(blockNumber)
        return
      }

      // lock-free check
      if (blockNumber < epochLength || epoch(blockNumber) - 1 >= lastCachedEpoch || blockNumber % epochLength >= epochLength / 2) {
        return
      }

      if (blockNumber < epochLength || epoch(blockNumber) - 1 >= lastCachedEpoch || blockNumber % epochLength >= epochLength / 2) {
        return
      }

      try {
        writeLock.lock()

        // cache previous epoch
        caches.add(new Cache(blockNumber - epochLength))
        lastCachedEpoch -= 1

        // remove redundant caches
        while (caches.size > MAX_CACHED_EPOCHS) {
          caches.remove(0)
        }
        log.info(s"Kept caches: cnt: ${caches.size} epochs: ${caches.get(0).epoch}...${caches.get(caches.size() - 1).epoch}")
      } finally {
        writeLock.unlock()
      }
    }

    private def reset(blockNumber: Long): Unit = {
      try {
        writeLock.lock()

        caches.clear()
        caches.add(new Cache(blockNumber))

        if (blockNumber % epochLength >= epochLength / 2) {
          caches.add(0, new Cache(blockNumber + epochLength))
        } else if (blockNumber >= epochLength) {
          caches.add(new Cache(blockNumber - epochLength))
        }

        lastCachedEpoch = caches.get(caches.size() - 1).epoch

        log.info(s"Kept caches: cnt: ${caches.size} epochs: ${caches.get(0).epoch}...${caches.get(caches.size - 1).epoch}")
      } finally {
        writeLock.unlock()
      }
    }
  }

  class DirectCache extends CacheStrategy {
    def cache(blockNumber: Long) {
      // reset cache if it's outdated
      if (epoch(blockNumber) > lastCachedEpoch || lastCachedEpoch < 0) {
        reset(blockNumber)
        return
      }

      // lock-free check
      if (epoch(blockNumber) + 1 <= lastCachedEpoch || blockNumber % epochLength <= LONGEST_CHAIN) {
        return
      }

      if (epoch(blockNumber) + 1 <= lastCachedEpoch || blockNumber % epochLength <= LONGEST_CHAIN) {
        return
      }

      try {
        writeLock.lock()

        // cache next epoch
        caches.add(new Cache(blockNumber + epochLength))
        lastCachedEpoch += 1

        // remove redundant caches
        while (caches.size > MAX_CACHED_EPOCHS) {
          caches.remove(0)
        }

        log.info(s"Kept caches: cnt: ${caches.size} epochs: ${caches.get(0).epoch}...${caches.get(caches.size() - 1).epoch}")
      } finally {
        writeLock.unlock()
      }
    }

    private def reset(blockNumber: Long) {
      try {
        writeLock.lock()

        caches.clear()
        caches.add(new Cache(blockNumber))

        if (blockNumber % epochLength > LONGEST_CHAIN) {
          caches.add(new Cache(blockNumber + epochLength))
        } else if (blockNumber >= epochLength) {
          caches.add(0, new Cache(blockNumber - epochLength))
        }

        lastCachedEpoch = caches.get(caches.size() - 1).epoch

        log.info(s"Kept caches: cnt: ${caches.size} epochs: ${caches.get(0).epoch}...${caches.get(caches.size() - 1).epoch}")
      } finally {
        writeLock.unlock()
      }
    }
  }
}
