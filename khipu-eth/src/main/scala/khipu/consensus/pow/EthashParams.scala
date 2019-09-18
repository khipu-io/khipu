package khipu.consensus.pow

class EthashParams {

  // Revision number of https://github.com/ethereum/wiki/wiki/Ethash
  val Revision: Int = 23

  // bytes in word
  val WORD_BYTES = 4

  // bytes in dataset at genesis
  val DATASET_BYTES_INIT = 1L << 30

  // dataset growth per epoch
  val DATASET_BYTES_GROWTH = 1L << 23

  //  bytes in dataset at genesis
  val CACHE_BYTES_INIT = 1L << 24

  // cache growth per epoch
  val CACHE_BYTES_GROWTH = 1L << 17

  //  Size of the DAG relative to the cache
  val CACHE_MULTIPLIER = 1024L

  //  blocks per epoch
  val EPOCH_LENGTH = 30000L

  // width of mix
  val MIX_BYTES = 128

  //  hash length in bytes
  val HASH_BYTES = 64

  // number of parents of each dataset element
  val DATASET_PARENTS = 256L

  // number of rounds in cache production
  val CACHE_ROUNDS = 3L

  //  number of accesses in hashimoto loop
  val ACCESSES = 64L

  /**
   * The parameters for Ethash's cache and dataset depend on the block number.
   * The cache size and dataset size both grow linearly; however, we always take the highest
   * prime below the linearly growing threshold in order to reduce the risk of accidental
   * regularities leading to cyclic behavior.
   */
  def getCacheSize(blockNumber: Long): Long = {
    var sz = CACHE_BYTES_INIT + CACHE_BYTES_GROWTH * (blockNumber / EPOCH_LENGTH) - HASH_BYTES
    while (!isPrime(sz / HASH_BYTES)) {
      sz -= 2 * HASH_BYTES
    }
    sz
  }

  def getFullSize(blockNumber: Long): Long = {
    var sz = DATASET_BYTES_INIT + DATASET_BYTES_GROWTH * (blockNumber / EPOCH_LENGTH) - MIX_BYTES
    while (!isPrime(sz / MIX_BYTES)) {
      sz -= 2 * MIX_BYTES
    }
    sz
  }

  private def isPrime(num: Long): Boolean = {
    if (num == 2) {
      true
    } else if (num % 2 == 0) {
      false
    } else {
      var i = 3
      while (i * i < num) {
        if (num % i == 0) {
          return false
        }
        i += 2
      }

      true
    }
  }
}
