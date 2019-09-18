package khipu.consensus.pow

import khipu.crypto
import khipu.util.BytesUtil
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.spongycastle.util.Arrays

/**
 * The Ethash algorithm described in https://github.com/ethereum/wiki/wiki/Ethash
 */
object EthashAlgo {

  final case class ProofOfWork(mixHash: Array[Byte], difficultyBoundary: Array[Byte])

  // Little-Endian !
  def getWord(arr: Array[Byte], wordOff: Int): Int = {
    return ByteBuffer.wrap(arr, wordOff * 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt()
  }

  def setWord(arr: Array[Byte], wordOff: Int, v: Long) {
    val bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v.toInt)
    bb.rewind()
    bb.get(arr, wordOff * 4, 4)
  }

  def remainderUnsigned(dividend: Int, divisor: Int): Int = {
    if (divisor >= 0) {
      if (dividend >= 0) {
        return dividend % divisor
      } else {
        // The implementation is a Java port of algorithm described in the book
        // "Hacker's Delight" (section "Unsigned short division from signed division").
        val q = ((dividend >>> 1) / divisor) << 1
        val w = dividend - q * divisor
        if (w < 0 || w >= divisor) w - divisor else w
      }
    } else {
      if (dividend >= 0 || dividend < divisor) dividend else dividend - divisor
    }
  }

  private val FNV_PRIME = 0x01000193
  private def fnv(v1: Int, v2: Int): Int = (v1 * FNV_PRIME) ^ v2

  def apply() = new EthashAlgo(new EthashParams())
  def apply(params: EthashParams) = new EthashAlgo(params)
}
final class EthashAlgo private (val params: EthashParams) {
  import EthashAlgo._

  private def makeCacheBytes(cacheSize: Long, seed: Array[Byte]): Array[Array[Byte]] = {
    val n = (cacheSize / params.HASH_BYTES).toInt
    val o = Array.ofDim[Array[Byte]](n)
    o(0) = crypto.kec512(seed)
    var i = 1
    while (i < n) {
      o(i) = crypto.kec512(o(i - 1))
      i += 1
    }

    var cacheRound = 0
    while (cacheRound < params.CACHE_ROUNDS) {
      var i = 0
      while (i < n) {
        val v = remainderUnsigned(getWord(o(i), 0), n)
        o(i) = crypto.kec512(BytesUtil.xor(o((i - 1 + n) % n), o(v)))
        i += 1
      }
      cacheRound += 1
    }

    o
  }

  def makeCache(cacheSize: Long, seed: Array[Byte]): Array[Int] = {
    val bytes = makeCacheBytes(cacheSize, seed)
    val ret = Array.ofDim[Int](bytes.length * bytes(0).length / 4)
    val ints = Array.ofDim[Int](bytes(0).length / 4)
    var i = 0
    while (i < bytes.length) {
      BytesUtil.bytesToInts(bytes(i), ints, false)
      System.arraycopy(ints, 0, ret, i * ints.length, ints.length)
      i += 1
    }
    ret
  }

  def sha512(arr: Array[Int], bigEndian: Boolean): Array[Int] = {
    var bytesTmp = Array.ofDim[Byte](arr.length << 2)
    BytesUtil.intsToBytes(arr, bytesTmp, bigEndian)
    bytesTmp = crypto.kec512(bytesTmp)
    BytesUtil.bytesToInts(bytesTmp, arr, bigEndian)
    arr
  }

  def calcDatasetItem(cache: Array[Int], i: Int): Array[Int] = {
    val r = params.HASH_BYTES / params.WORD_BYTES
    val n = cache.length / r
    var mix = Arrays.copyOfRange(cache, i % n * r, (i % n + 1) * r)

    mix(0) = i ^ mix(0)
    mix = sha512(mix, false)
    val dsParents = params.DATASET_PARENTS.toInt
    val mixLen = mix.length
    var j = 0
    while (j < dsParents) {
      var cacheIdx = fnv(i ^ j, mix(j % r))
      cacheIdx = remainderUnsigned(cacheIdx, n)
      val off = cacheIdx * r
      var k = 0
      while (k < mixLen) {
        mix(k) = fnv(mix(k), cache(off + k))
        k += 1
      }
      j += 1
    }

    sha512(mix, false)
  }

  def calcDataset(fullSize: Long, cache: Array[Int]): Array[Int] = {
    val hashesCount = (fullSize / params.HASH_BYTES).toInt
    val ret = Array.ofDim[Int](hashesCount * (params.HASH_BYTES / 4))
    var i = 0
    while (i < hashesCount) {
      val item = calcDatasetItem(cache, i)
      System.arraycopy(item, 0, ret, i * (params.HASH_BYTES / 4), item.length)
      i += 1
    }

    ret
  }

  def hashimotoLight(fullSize: Long, cache: Array[Int], blockHeaderTruncHash: Array[Byte], nonce: Array[Byte]): ProofOfWork = {
    hashimoto(blockHeaderTruncHash, nonce, fullSize, cache, false)
  }

  def hashimotoFull(fullSize: Long, dataset: Array[Int], blockHeaderTruncHash: Array[Byte], nonce: Array[Byte]): ProofOfWork = {
    hashimoto(blockHeaderTruncHash, nonce, fullSize, dataset, true)
  }

  def hashimoto(blockHeaderTruncHash: Array[Byte], nonce: Array[Byte], fullSize: Long, cacheOrDataset: Array[Int], full: Boolean): ProofOfWork = {
    if (nonce.length != 8) throw new RuntimeException("nonce.length != 8")

    val hashWords = params.HASH_BYTES / 4
    val w = params.MIX_BYTES / params.WORD_BYTES
    val mixhashes = params.MIX_BYTES / params.HASH_BYTES
    val s = BytesUtil.bytesToInts(crypto.kec512(BytesUtil.merge(blockHeaderTruncHash, Arrays.reverse(nonce))), false)
    val mix = Array.ofDim[Int](params.MIX_BYTES / 4)
    var i = 0
    while (i < mixhashes) {
      System.arraycopy(s, 0, mix, i * s.length, s.length)
      i += 1
    }

    val numFullPages = (fullSize / params.MIX_BYTES).toInt
    i = 0
    while (i < params.ACCESSES) {
      val p = remainderUnsigned(fnv(i ^ s(0), mix(i % w)), numFullPages)
      val newData = Array.ofDim[Int](mix.length)
      val off = p * mixhashes
      var j = 0
      while (j < mixhashes) {
        val itemIdx = off + j
        if (!full) {
          val lookup1 = calcDatasetItem(cacheOrDataset, itemIdx)
          System.arraycopy(lookup1, 0, newData, j * lookup1.length, lookup1.length)
        } else {
          System.arraycopy(cacheOrDataset, itemIdx * hashWords, newData, j * hashWords, hashWords)
        }
        j += 1
      }
      var i1 = 0
      while (i1 < mix.length) {
        mix(i1) = fnv(mix(i1), newData(i1))
        i1 += 1
      }
      i += 1
    }

    val cmix = Array.ofDim[Int](mix.length / 4)
    i = 0
    while (i < mix.length) {
      val fnv1 = fnv(mix(i), mix(i + 1))
      val fnv2 = fnv(fnv1, mix(i + 2))
      val fnv3 = fnv(fnv2, mix(i + 3))
      cmix(i >> 2) = fnv3
      i += 4
    }

    ProofOfWork(
      BytesUtil.intsToBytes(cmix, false),
      crypto.sha3(BytesUtil.merge(BytesUtil.intsToBytes(s, false), BytesUtil.intsToBytes(cmix, false)))
    )
  }

  def getSeedHash(blockNumber: Long): Array[Byte] = {
    val epoch = blockNumber / params.EPOCH_LENGTH
    var ret = Array.ofDim[Byte](32)
    var i = 0
    while (i < epoch) {
      ret = crypto.sha3(ret)
      i += 1
    }

    ret
  }
}
