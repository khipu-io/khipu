package khipu.ledger

import akka.util.ByteString
import java.util.Arrays
import khipu.crypto
import khipu.domain.TxLogEntry
import khipu.util.BytesUtil

object BloomFilter {

  private val BloomFilterByteSize = 256
  val EmptyBloomFilter = ByteString(Array.ofDim[Byte](BloomFilterByteSize))

  def containsAnyOf(bloomFilterBytes: ByteString, toCheck: List[ByteString]): Boolean = {
    toCheck exists { bytes =>
      val bloomFilterForBytes = Bloom(bytes.toArray).data
      val andResult = BytesUtil.and(bloomFilterForBytes, bloomFilterBytes.toArray)
      Arrays.equals(andResult, bloomFilterForBytes)
    }
  }

  /**
   * Given the logs of a receipt creates the bloom filter associated with them
   * as stated in section 4.4.1 of the YP
   *
   * @param logs from the receipt whose bloom filter will be created
   * @return bloom filter associated with the logs
   */
  def create(logs: Seq[TxLogEntry]): ByteString = {
    if (logs.isEmpty) {
      EmptyBloomFilter
    } else {
      var bloom: Bloom = null
      val itr = logs.iterator
      while (itr.hasNext) {
        val b = createBloomFilterForLogEntry(itr.next())
        if (bloom eq null) {
          bloom = b
        } else {
          bloom = bloom or b
        }
      }
      ByteString(bloom.data)
    }
  }

  // Bloom filter function that reduces a log to a single 256-byte hash based on equation 24 from the YP
  private def createBloomFilterForLogEntry(logEntry: TxLogEntry): Bloom = {
    logEntry.logTopics.foldLeft(Bloom(logEntry.loggerAddress.bytes.toArray)) {
      case (acc, topic) => acc or Bloom(topic.toArray)
    }
  }

  private object Bloom {
    private val _3LOW_BITS = 7

    def apply(rawBytes: Array[Byte]): Bloom = createBloomFilter(crypto.kec256(rawBytes))

    // Bloom filter that sets 3 bits out of 2048 based on equations 25-28 from the YP
    private def createBloomFilter(toBloom: Array[Byte]): Bloom = {
      val mov1 = (((toBloom(0) & 0xFF) & (_3LOW_BITS)) << 8) + ((toBloom(1)) & 0xFF)
      val mov2 = (((toBloom(2) & 0xFF) & (_3LOW_BITS)) << 8) + ((toBloom(3)) & 0xFF)
      val mov3 = (((toBloom(4) & 0xFF) & (_3LOW_BITS)) << 8) + ((toBloom(5)) & 0xFF)

      val data = Array.ofDim[Byte](256)
      setBit(data, mov1, 1)
      setBit(data, mov2, 1)
      setBit(data, mov3, 1)

      new Bloom(data)
    }

    private def setBit(data: Array[Byte], pos: Int, v: Int) {
      require((data.length * 8) - 1 >= pos, s"outside byte array limit, pos: $pos")

      val posByte = data.length - 1 - (pos / 8)
      val posBit = (pos) % 8
      val setter = (1 << (posBit)).toByte
      val toBeSet = data(posByte)
      val res = if (v == 1) {
        (toBeSet | setter).toByte
      } else {
        (toBeSet & ~setter).toByte
      }

      data(posByte) = res
    }
  }
  /**
   * mutable data
   */
  private class Bloom private (val data: Array[Byte]) {

    def or(bloom: Bloom): this.type = {
      var i = 0
      while (i < data.length) {
        data(i) = (data(i) | bloom.data(i)).toByte
        i += 1
      }
      this
    }

    def matches(topicBloom: Bloom): Boolean = {
      val cp = copy()
      cp.or(topicBloom)
      this.equals(cp)
    }

    def copy(): Bloom = {
      val dataCopy = Array.ofDim[Byte](data.length)
      System.arraycopy(data, 0, dataCopy, 0, data.length)
      new Bloom(dataCopy)
    }

    override def toString(): String = khipu.toHexString(data)

    override def equals(that: Any): Boolean = {
      that match {
        case that: Bloom => (this eq that) || Arrays.equals(data, that.data)
        case _           => false
      }
    }

    override def hashCode(): Int = if (data != null) Arrays.hashCode(data) else 0
  }
}

