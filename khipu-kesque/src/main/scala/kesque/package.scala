
import java.nio.ByteBuffer
import org.apache.kafka.common.record.AbstractRecords
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.ByteBufferOutputStream

package object kesque {
  val fetchMaxBytesInLoadOffsets = 100 * 1024 * 1024 // 100M
  val DEFAULT_FETCH_MAX_BYTES = 4 * 1024 // 4K - the size of SSD block

  /**
   * Special function to extract bytes from kafka's DefaultRecord key or value ByteBuffer
   * @see org.apache.kafka.common.utils.Utils.writeTo
   */
  def getBytes(buffer: ByteBuffer): Array[Byte] = {
    val length = buffer.remaining
    val value = Array.ofDim[Byte](length)
    if (buffer.hasArray) {
      System.arraycopy(buffer.array, buffer.position + buffer.arrayOffset, value, 0, length)
    } else {
      val pos = buffer.position
      var i = pos
      while (i < length + pos) {
        value(i) = buffer.get(i)
        i += 1
      }
    }
    value
  }

  def buildRecords(compressionType: CompressionType, initialOffset: Long, records: SimpleRecord*): MemoryRecords = buildRecords(
    RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, compressionType,
    TimestampType.CREATE_TIME, 0, 0,
    0, RecordBatch.NO_PARTITION_LEADER_EPOCH, isTransactional = false,
    records: _*
  )

  def buildRecords(magic: Byte, initialOffset: Long, compressionType: CompressionType,
                   timestampType: TimestampType, producerId: Long, producerEpoch: Short,
                   baseSequence: Int, partitionLeaderEpoch: Int, isTransactional: Boolean,
                   records: SimpleRecord*): MemoryRecords = {
    if (records.isEmpty) {
      MemoryRecords.EMPTY
    } else {
      import scala.collection.JavaConverters._
      val sizeEstimate = AbstractRecords.estimateSizeInBytes(magic, compressionType, records.asJava)
      val bufferStream = new ByteBufferOutputStream(sizeEstimate)
      val logAppendTime = timestampType match {
        case TimestampType.LOG_APPEND_TIME => System.currentTimeMillis()
        case _                             => RecordBatch.NO_TIMESTAMP
      }

      val builder = new MemoryRecordsBuilder(bufferStream, magic, compressionType, timestampType,
        initialOffset, logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, false,
        partitionLeaderEpoch, sizeEstimate)

      records foreach builder.append

      builder.build()
    }
  }
}