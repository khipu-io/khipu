package khipu.consensus.common

import khipu.Hash
import khipu.domain.Blockchain
import khipu.domain.BlockHeader

import java.util.ArrayDeque
import java.util.Deque
import java.util.NoSuchElementException
import java.util.concurrent.ExecutionException

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder

class VoteTallyCache(
    blockchain:       Blockchain,
    voteTallyUpdater: VoteTallyUpdater,
    epochManager:     EpochManager,
    blockInterface:   BlockInterface
) {

  private val voteTallyCache: Cache[Hash, VoteTally] = CacheBuilder.newBuilder().maximumSize(100).build()

  def getVoteTallyAtHead(): VoteTally = {
    getVoteTallyAfterBlock(blockchain.getBestBlockHeader)
  }

  /**
   * Determines the VoteTally for a given block header, by back-tracing the blockchain to a
   * previously cached value or epoch block. Then appyling votes in each intermediate header such
   * that representative state can be provided. This function assumes the vote cast in {@code
   * header} is applied, thus the voteTally returned contains the group of validators who are
   * permitted to partake in the next block's creation.
   *
   * @param header the header of the block after which the VoteTally is to be returned
   * @return The Vote Tally (and therefore validators) following the application of all votes upto
   *     and including the requested header.
   */
  def getVoteTallyAfterBlock(header: BlockHeader): VoteTally = {
    try {
      voteTallyCache.get(header.hash, new java.util.concurrent.Callable[VoteTally] { def call() = populateCacheUptoAndIncluding(header) })
    } catch {
      case ex: ExecutionException => throw new RuntimeException("Unable to determine a VoteTally object for the requested block.")
    }
  }

  private def populateCacheUptoAndIncluding(start: BlockHeader): VoteTally = {
    var header = start
    val intermediateBlocks = new ArrayDeque[BlockHeader]()
    var voteTally: VoteTally = null

    var break = false
    while (!break) { // Will run into an epoch block (and thus a VoteTally) to break loop.
      intermediateBlocks.push(header)
      voteTally = getValidatorsAfter(header)
      if (voteTally != null) {
        break = true
      } else {
        header = blockchain.getBlockHeaderByHash(header.parentHash).getOrElse(
          throw new NoSuchElementException("Supplied block was on a orphaned chain, unable to generate VoteTally.")
        )
      }
    }
    return constructMissingCacheEntries(intermediateBlocks, voteTally)
  }

  protected def getValidatorsAfter(header: BlockHeader): VoteTally = {
    if (epochManager.isEpochBlock(header.number)) {
      new VoteTally(blockInterface.validatorsInBlock(header))
    } else {
      voteTallyCache.getIfPresent(header.parentHash)
    }
  }

  private def constructMissingCacheEntries(headers: Deque[BlockHeader], tally: VoteTally): VoteTally = {
    val mutableVoteTally = tally.copy()
    while (!headers.isEmpty) {
      val h = headers.pop()
      voteTallyUpdater.updateForBlock(h, mutableVoteTally)
      voteTallyCache.put(h.hash, mutableVoteTally.copy())
    }
    mutableVoteTally
  }
}
