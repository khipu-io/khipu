package khipu.ledger

import akka.util.ByteString
import khipu.domain.TxLogEntry
import khipu.crypto
import khipu.util.BytesUtil

object BloomFilter {

  private val BloomFilterByteSize = 256
  private val BloomFilterBitSize = BloomFilterByteSize * 8
  private val IntIndexesToAccess = Set(0, 2, 4)

  // alyways create new array instead of a val, since we cannot guarantee if it 
  // will be changed outside (it's an array which is mutable)
  def emptyBloomFilterBytes = Array.fill[Byte](BloomFilterByteSize)(0)
  def emptyBloomFilter = ByteString(emptyBloomFilterBytes)

  def containsAnyOf(bloomFilterBytes: ByteString, toCheck: Seq[ByteString]): Boolean = {
    toCheck.exists { bytes =>
      val bloomFilterForBytes = bloomFilter(bytes.toArray[Byte])

      val andResult = BytesUtil.and(bloomFilterForBytes, bloomFilterBytes.toArray[Byte])
      andResult sameElements bloomFilterForBytes
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
    val bloomFilters = logs.map(createBloomFilterForLogEntry)
    if (bloomFilters.isEmpty) {
      emptyBloomFilter
    } else {
      ByteString(BytesUtil.or(bloomFilters: _*))
    }
  }

  // Bloom filter function that reduces a log to a single 256-byte hash based on equation 24 from the YP
  private def createBloomFilterForLogEntry(logEntry: TxLogEntry): Array[Byte] = {
    val dataForBloomFilter = logEntry.loggerAddress.bytes +: logEntry.logTopics
    val bloomFilters = dataForBloomFilter.map(bytes => bloomFilter(bytes.toArray))

    BytesUtil.or(bloomFilters: _*)
  }

  // Bloom filter that sets 3 bits out of 2048 based on equations 25-28 from the YP
  private def bloomFilter(bytes: Array[Byte]): Array[Byte] = {
    val hashedBytes = crypto.kec256(bytes)

    val bitsToSet = IntIndexesToAccess.map { i =>
      val index16bit = (hashedBytes(i + 1) & 0xFF) + ((hashedBytes(i) & 0xFF) << 8)
      index16bit % BloomFilterBitSize // Obtain only 11 bits from the index
    }

    bitsToSet.foldLeft(emptyBloomFilterBytes) {
      case (acc, index) => setBit(acc, index)
    }.reverse
  }

  // in-place set 
  private def setBit(bytes: Array[Byte], bitIndex: Int): Array[Byte] = {
    require(bitIndex / 8 < bytes.length, "Only bits between the bytes array should be set")

    val byteIndex = bitIndex / 8
    val newByte = (bytes(byteIndex) | 1 << (bitIndex % 8).toByte).toByte
    bytes(byteIndex) = newByte
    bytes
  }
}
