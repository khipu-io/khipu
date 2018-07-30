package kesque

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import kafka.cluster.Replica
import kafka.log.Log
import kafka.log.LogAppendInfo
import kafka.log.LogManager
import kafka.server.BrokerTopicStats
import kafka.server.DelayedDeleteRecords
import kafka.server.DelayedFetch
import kafka.server.DelayedOperationKey
import kafka.server.DelayedOperationPurgatory
import kafka.server.DelayedProduce
import kafka.server.FetchDataInfo
import kafka.server.KafkaConfig
import kafka.server.LogAppendResult
import kafka.server.LogDeleteRecordsResult
import kafka.server.LogDirFailureChannel
import kafka.server.LogOffsetMetadata
import kafka.server.LogReadResult
import kafka.server.MetadataCache
import kafka.server.ReplicaQuota
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils.Logging
import kafka.utils.Pool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.errors.NotEnoughReplicasException
import org.apache.kafka.common.errors.NotLeaderForPartitionException
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.errors.RecordBatchTooLargeException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.ReplicaNotAvailableException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.DeleteRecordsRequest
import org.apache.kafka.common.requests.DescribeLogDirsResponse
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo
import org.apache.kafka.common.requests.DescribeLogDirsResponse.ReplicaInfo
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.common.utils.Time
import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.record.MemoryRecordsBuilder
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.record.AbstractRecords

/**
 * from kafka.server.ReplicaManager.scala
 */
object KafkaReaderWriter {
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L
  val OfflinePartition = new Partition("", -1, null, null, isOffline = true)

  def buildRecords(initialOffset: Long, records: SimpleRecord*): MemoryRecords =
    buildRecords(RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, CompressionType.NONE,
      TimestampType.CREATE_TIME, 0, 0,
      0, RecordBatch.NO_PARTITION_LEADER_EPOCH, isTransactional = false,
      records: _*)

  def buildRecords(compressionType: CompressionType, initialOffset: Long, records: SimpleRecord*): MemoryRecords =
    buildRecords(RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, compressionType,
      TimestampType.CREATE_TIME, 0, 0,
      0, RecordBatch.NO_PARTITION_LEADER_EPOCH, isTransactional = false,
      records: _*)

  def buildRecords(magic: Byte, initialOffset: Long, compressionType: CompressionType,
                   timestampType: TimestampType, producerId: Long, producerEpoch: Short,
                   baseSequence: Int, partitionLeaderEpoch: Int, isTransactional: Boolean,
                   records: SimpleRecord*): MemoryRecords = {
    if (records.isEmpty)
      return MemoryRecords.EMPTY

    import scala.collection.JavaConverters._
    val sizeEstimate = AbstractRecords.estimateSizeInBytes(magic, compressionType, records.asJava)
    val bufferStream = new ByteBufferOutputStream(sizeEstimate)
    var logAppendTime = RecordBatch.NO_TIMESTAMP
    if (timestampType == TimestampType.LOG_APPEND_TIME)
      logAppendTime = System.currentTimeMillis()
    val builder = new MemoryRecordsBuilder(bufferStream, magic, compressionType, timestampType,
      initialOffset, logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, false,
      partitionLeaderEpoch, sizeEstimate)

    for (record <- records) builder.append(record)

    builder.build()
  }
}
class KafkaReaderWriter(
    val config:                        KafkaConfig,
    time:                              Time,
    val logManager:                    LogManager,
    val brokerTopicStats:              BrokerTopicStats,
    val metadataCache:                 MetadataCache,
    logDirFailureChannel:              LogDirFailureChannel,
    val delayedProducePurgatory:       DelayedOperationPurgatory[DelayedProduce],
    val delayedFetchPurgatory:         DelayedOperationPurgatory[DelayedFetch],
    val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords]
) extends Logging {

  def this(
    config:               KafkaConfig,
    time:                 Time,
    logManager:           LogManager,
    brokerTopicStats:     BrokerTopicStats,
    metadataCache:        MetadataCache,
    logDirFailureChannel: LogDirFailureChannel
  ) {
    this(config, time, logManager, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests
      ),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests
      ),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests
      ))
  }

  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[TopicPartition, Partition](valueFactory = Some(tp => {
    val par = new Partition(tp.topic, tp.partition, time, this)
    // modified by dcaoyuan
    par.getOrCreateReplica(localBrokerId)
    par
  }))
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile var highWatermarkCheckpoints = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, KafkaReaderWriter.HighWatermarkFilename), logDirFailureChannel))).toMap

  def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLocalLog(
    replicaId:           Int,
    fetchOnlyFromLeader: Boolean,
    readOnlyCommitted:   Boolean,
    fetchMaxBytes:       Int,
    hardMaxBytesLimit:   Boolean,
    readPartitionInfo:   Seq[(TopicPartition, PartitionData)],
    quota:               ReplicaQuota,
    isolationLevel:      IsolationLevel
  ): Seq[(TopicPartition, LogReadResult)] = {

    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      try {
        trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
          s"remaining response limit $limitBytes" +
          (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        // decide whether to only fetch from leader
        val localReplica = if (fetchOnlyFromLeader)
          getLeaderReplicaIfLocal(tp)
        else
          getReplicaOrException(tp)

        val initialHighWatermark = localReplica.highWatermark.messageOffset
        val lastStableOffset = if (isolationLevel == IsolationLevel.READ_COMMITTED)
          Some(localReplica.lastStableOffset.messageOffset)
        else
          None

        // decide whether to only fetch committed data (i.e. messages below high watermark)
        val maxOffsetOpt = if (readOnlyCommitted)
          Some(lastStableOffset.getOrElse(initialHighWatermark))
        else
          None

        /* Read the LogOffsetMetadata prior to performing the read from the log.
         * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
         * Using the log end offset after performing the read can lead to a race condition
         * where data gets appended to the log immediately after the replica has consumed from it
         * This can cause a replica to always be out of sync.
         */
        val initialLogEndOffset = localReplica.logEndOffset.messageOffset
        val initialLogStartOffset = localReplica.logStartOffset
        val fetchTimeMs = time.milliseconds
        val logReadInfo = localReplica.log match {
          case Some(log) =>
            val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)

            // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
            val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage, isolationLevel)

            // If the partition is being throttled, simply return an empty set.
            if (shouldLeaderThrottle(quota, tp, replicaId))
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
            else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete)
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            else fetch

          case None =>
            error(s"Leader for partition $tp does not have a local log")
            FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
        }

        LogReadResult(
          info = logReadInfo,
          highWatermark = initialHighWatermark,
          leaderLogStartOffset = initialLogStartOffset,
          leaderLogEndOffset = initialLogEndOffset,
          followerLogStartOffset = followerLogStartOffset,
          fetchTimeMs = fetchTimeMs,
          readSize = partitionFetchSize,
          lastStableOffset = lastStableOffset,
          exception = None
        )
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e @ (_: UnknownTopicOrPartitionException |
          _: NotLeaderForPartitionException |
          _: ReplicaNotAvailableException |
          _: KafkaStorageException |
          _: OffsetOutOfRangeException) =>
          LogReadResult(
            info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = -1L,
            leaderLogStartOffset = -1L,
            leaderLogEndOffset = -1L,
            followerLogStartOffset = -1L,
            fetchTimeMs = -1L,
            readSize = partitionFetchSize,
            lastStableOffset = None,
            exception = Some(e)
          )
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()
          error(s"Error processing fetch operation on partition $tp, offset $offset", e)
          LogReadResult(
            info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = -1L,
            leaderLogStartOffset = -1L,
            leaderLogEndOffset = -1L,
            followerLogStartOffset = -1L,
            fetchTimeMs = -1L,
            readSize = partitionFetchSize,
            lastStableOffset = None,
            exception = Some(e)
          )
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
    readPartitionInfo.foreach {
      case (tp, fetchInfo) =>
        val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
        val recordBatchSize = readResult.info.records.sizeInBytes
        // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
        if (recordBatchSize > 0)
          minOneMessage = false
        limitBytes = math.max(0, limitBytes - recordBatchSize)
        result += (tp -> readResult)
    }
    result
  }

  /**
   * Append the messages to the local replica logs
   */
  def appendToLocalLog(
    internalTopicsAllowed: Boolean,
    isFromClient:          Boolean,
    entriesPerPartition:   Map[TopicPartition, MemoryRecords],
    requiredAcks:          Short
  ): Map[TopicPartition, LogAppendResult] = {
    trace("Append [%s] to local log ".format(entriesPerPartition))
    entriesPerPartition.map {
      case (topicPartition, records) =>
        brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
        brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

        // reject appending to internal topics if it is not allowed
        if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
          (topicPartition, LogAppendResult(
            LogAppendInfo.UnknownLogAppendInfo,
            Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))
          ))
        } else {
          try {
            // force to maybrPut partition
            getOrCreatePartition(topicPartition)

            val partitionOpt = getPartition(topicPartition)
            val info = partitionOpt match {
              case Some(partition) =>
                if (partition eq KafkaReaderWriter.OfflinePartition)
                  throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory on broker $localBrokerId")
                partition.appendRecordsToLeader(records, isFromClient, requiredAcks)

              case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
                .format(topicPartition, localBrokerId))
            }

            val numAppendedMessages =
              if (info.firstOffset == -1L || info.lastOffset == -1L)
                0
              else
                info.lastOffset - info.firstOffset + 1

            // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
            brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
            brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
            brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
            brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

            trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
              .format(records.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
            (topicPartition, LogAppendResult(info))
          } catch {
            // NOTE: Failed produce requests metric is not incremented for known exceptions
            // it is supposed to indicate un-expected failures of a broker in handling a produce request
            case e @ (
              _: UnknownTopicOrPartitionException |
              _: NotLeaderForPartitionException |
              _: RecordTooLargeException |
              _: RecordBatchTooLargeException |
              _: CorruptRecordException |
              _: KafkaStorageException |
              _: InvalidTimestampException) =>
              (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
            case t: Throwable =>
              val logStartOffset = getPartition(topicPartition) match {
                case Some(partition) =>
                  partition.logStartOffset
                case _ =>
                  -1
              }
              brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
              brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
              error("Error processing append operation on partition %s".format(topicPartition), t)
              (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset), Some(t)))
          }
        }
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map {
      case (topicPartition, requestedOffset) =>
        // reject delete records operation on internal topics
        if (Topic.isInternal(topicPartition.topic)) {
          (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
        } else {
          try {
            // force to maybrPut partition
            getOrCreatePartition(topicPartition)

            val partition = getPartition(topicPartition) match {
              case Some(p) =>
                if (p eq KafkaReaderWriter.OfflinePartition)
                  throw new KafkaStorageException("Partition %s is in an offline log directory on broker %d".format(topicPartition, localBrokerId))
                p
              case None =>
                throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d".format(topicPartition, localBrokerId))
            }
            val convertedOffset =
              if (requestedOffset == DeleteRecordsRequest.HIGH_WATERMARK) {
                partition.leaderReplicaIfLocal match {
                  case Some(leaderReplica) =>
                    leaderReplica.highWatermark.messageOffset
                  case None =>
                    throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
                      .format(topicPartition, localBrokerId))
                }
              } else
                requestedOffset
            if (convertedOffset < 0)
              throw new OffsetOutOfRangeException(s"The offset $convertedOffset for partition $topicPartition is not valid")

            val lowWatermark = partition.deleteRecordsOnLeader(convertedOffset)
            (topicPartition, LogDeleteRecordsResult(convertedOffset, lowWatermark))
          } catch {
            // NOTE: Failed produce requests metric is not incremented for known exceptions
            // it is supposed to indicate un-expected failures of a broker in handling a produce request
            case e @ (_: UnknownTopicOrPartitionException |
              _: NotLeaderForPartitionException |
              _: OffsetOutOfRangeException |
              _: PolicyViolationException |
              _: KafkaStorageException |
              _: NotEnoughReplicasException) =>
              (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
            case t: Throwable =>
              error("Error processing delete records operation on partition %s".format(topicPartition), t)
              (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
          }
        }
    }
  }

  // --- helpers

  def getReplicaOrException(topicPartition: TopicPartition): Replica = {
    // force to maybrPut partition
    getOrCreatePartition(topicPartition)

    getPartition(topicPartition) match {
      case Some(partition) =>
        if (partition eq KafkaReaderWriter.OfflinePartition)
          throw new KafkaStorageException(s"Replica $localBrokerId is in an offline log directory for partition $topicPartition")
        else
          partition.getReplica(localBrokerId).getOrElse(
            throw new ReplicaNotAvailableException(s"Replica $localBrokerId is not available for partition $topicPartition")
          )
      case None =>
        throw new ReplicaNotAvailableException(s"Replica $localBrokerId is not available for partition $topicPartition")
    }
  }

  def getLeaderReplicaIfLocal(topicPartition: TopicPartition): Replica = {
    // force to maybrPut partition
    getOrCreatePartition(topicPartition)

    val partitionOpt = getPartition(topicPartition)
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException(s"Partition $topicPartition doesn't exist on $localBrokerId")
      case Some(partition) =>
        if (partition eq KafkaReaderWriter.OfflinePartition)
          throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory on broker $localBrokerId")
        else partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica
          case None =>
            throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
        }
    }
  }

  def getOrCreatePartition(topicPartition: TopicPartition): Partition =
    allPartitions.getAndMaybePut(topicPartition)

  def getPartition(topicPartition: TopicPartition): Option[Partition] =
    Option(allPartitions.get(topicPartition))

  def getReplica(topicPartition: TopicPartition, replicaId: Int): Option[Replica] =
    nonOfflinePartition(topicPartition).flatMap(_.getReplica(replicaId))

  def getReplica(tp: TopicPartition): Option[Replica] = getReplica(tp, localBrokerId)

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    getReplica(topicPartition).flatMap(_.log) match {
      case Some(log) => Some(log.dir.getParent)
      case None      => None
    }
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, topicPartition: TopicPartition, replicaId: Int): Boolean = {
    val isReplicaInSync = nonOfflinePartition(topicPartition).exists { partition =>
      partition.getReplica(replicaId).exists(partition.inSyncReplicas.contains)
    }
    quota.isThrottled(topicPartition) && quota.isQuotaExceeded && !isReplicaInSync
  }

  def nonOfflinePartition(topicPartition: TopicPartition): Option[Partition] =
    getPartition(topicPartition).filter(_ ne KafkaReaderWriter.OfflinePartition)

  /**
   * Try to complete some delayed produce requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for acks = -1)
   * 2. A follower replica's fetch operation is received (for acks > 1)
   */
  def tryCompleteDelayedProduce(key: DelayedOperationKey) {
    val completed = delayedProducePurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed fetch requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for regular fetch)
   * 2. A new message set is appended to the local log (for follower fetch)
   */
  def tryCompleteDelayedFetch(key: DelayedOperationKey) {
    val completed = delayedFetchPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d fetch requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed DeleteRecordsRequest with the request key;
   * this needs to be triggered when the partition low watermark has changed
   */
  def tryCompleteDelayedDeleteRecords(key: DelayedOperationKey) {
    val completed = delayedDeleteRecordsPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d DeleteRecordsRequest.".format(key.keyLabel, completed))
  }

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(partitions: Set[TopicPartition]): Map[String, LogDirInfo] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.dir.getParent)

    config.logDirs.toSet.map { logDir: String =>
      val absolutePath = new File(logDir).getAbsolutePath
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        logsByDir.get(absolutePath) match {
          case Some(logs) =>
            val replicaInfos = logs.filter(log =>
              partitions.contains(log.topicPartition)).map(log => log.topicPartition -> new ReplicaInfo(log.size, getLogEndOffsetLag(log.topicPartition), false)).toMap

            (absolutePath, new LogDirInfo(Errors.NONE, replicaInfos.asJava))
          case None =>
            (absolutePath, new LogDirInfo(Errors.NONE, Map.empty[TopicPartition, ReplicaInfo].asJava))
        }

      } catch {
        case e: KafkaStorageException =>
          (absolutePath, new LogDirInfo(Errors.KAFKA_STORAGE_ERROR, Map.empty[TopicPartition, ReplicaInfo].asJava))
        case t: Throwable =>
          error(s"Error while describing replica in dir $absolutePath", t)
          (absolutePath, new LogDirInfo(Errors.forException(t), Map.empty[TopicPartition, ReplicaInfo].asJava))
      }
    }.toMap
  }

  def getLogEndOffset(topicPartition: TopicPartition): Long = {
    getReplica(topicPartition) match {
      case Some(replica) =>
        replica.log.get.logEndOffset
      case None =>
        // return -1L to indicate that the LEO is not available if broker is neither follower or leader of this partition
        -1L
    }
  }

  def getLogEndOffsetLag(topicPartition: TopicPartition): Long = {
    getReplica(topicPartition) match {
      case Some(replica) =>
        math.max(replica.highWatermark.messageOffset - replica.log.get.logEndOffset, 0)
      case None =>
        // return -1L to indicate that the LEO lag is not available if broker is neither follower or leader of this partition
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  def stopReplica(topicPartition: TopicPartition, deletePartition: Boolean) = {
    //stateChangeLogger.trace(s"Handling stop replica (delete=$deletePartition) for partition $topicPartition")

    if (deletePartition) {
      val removedPartition = allPartitions.remove(topicPartition)
      if (removedPartition eq KafkaReaderWriter.OfflinePartition)
        throw new KafkaStorageException(s"Partition $topicPartition is on an offline disk")

      if (removedPartition != null) {
        val topicHasPartitions = allPartitions.values.exists(partition => topicPartition.topic == partition.topic)
        if (!topicHasPartitions)
          brokerTopicStats.removeMetrics(topicPartition.topic)
        // this will delete the local log. This call may throw exception if the log is on offline directory
        removedPartition.delete()
      } else {
        //stateChangeLogger.trace(s"Ignoring stop replica (delete=$deletePartition) for partition $topicPartition as replica doesn't exist on broker")
      }

      // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
      // This could happen when topic is being deleted while broker is down and recovers.
      if (logManager.getLog(topicPartition).isDefined)
        logManager.asyncDelete(topicPartition)
      //if (logManager.getLog(topicPartition, isFuture = true).isDefined)
      //  logManager.asyncDelete(topicPartition, isFuture = true)
    }
    //stateChangeLogger.trace(s"Finished handling stop replica (delete=$deletePartition) for partition $topicPartition")
  }

}
