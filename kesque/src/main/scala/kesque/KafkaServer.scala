package kesque

import java.io.File
import java.io.IOException
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import kafka.common.GenerateBrokerIdException
import kafka.common.InconsistentBrokerIdException
import kafka.log.CleanerConfig
import kafka.log.LogConfig
import kafka.log.LogManager
import kafka.server.BrokerMetadataCheckpoint
import kafka.server.BrokerShuttingDown
import kafka.server.BrokerState
import kafka.server.BrokerTopicStats
import kafka.server.KafkaConfig
import kafka.server.LogDirFailureChannel
import kafka.server.MetadataCache
import kafka.server.NotRunning
import kafka.utils.Exit
import kafka.utils.KafkaScheduler
import kafka.utils.Logging
import org.apache.kafka.common.utils.OperatingSystem
import org.apache.kafka.common.utils.Time
import scala.collection.JavaConverters._
import scala.collection.mutable
import sun.misc.Signal
import sun.misc.SignalHandler

object KafkaServer {

  /**
   * @see kafka.server.KafkaServerStartable and kafka.Kafka
   * val configFile = System.getProperty("user.home") + "/myapps.kafka/config" + "server.properties"
   */
  def start(props: Properties): KafkaServer = {
    val config = KafkaConfig.fromProps(props, false)
    val kafkaServer = new KafkaServer(config)

    // register signal handler to log termination due to SIGTERM, SIGHUP and SIGINT (control-c)
    registerLoggingSignalHandler()

    kafkaServer.startup()
    kafkaServer
  }

  /**
   * Shutdown may be better to process outside by user, for example, by akka actor's postStop
   * Attach shutdown handler to catch terminating signals as well as normal termination
   */
  private def addShutdownHook(kafkaServer: KafkaServer) {
    Runtime.getRuntime().addShutdownHook(new Thread("kafka-shutdown-hook") {
      override def run() {
        try {
          kafkaServer.shutdown()
        } catch {
          case _: Throwable =>
            //fatal("Halting Kafka.")
            // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
            Exit.halt(1)
        }
      }
    })
  }

  private def registerLoggingSignalHandler(): Unit = {
    val jvmSignalHandlers = new ConcurrentHashMap[String, SignalHandler]().asScala
    val handler = new SignalHandler() {
      override def handle(signal: Signal) {
        //info(s"Terminating process due to signal $signal")
        jvmSignalHandlers.get(signal.getName).foreach(_.handle(signal))
      }
    }
    def registerHandler(signalName: String) {
      val oldHandler = Signal.handle(new Signal(signalName), handler)
      if (oldHandler != null)
        jvmSignalHandlers.put(signalName, oldHandler)
    }

    if (!OperatingSystem.IS_WINDOWS) {
      registerHandler("TERM")
      registerHandler("INT")
      registerHandler("HUP")
    }
  }

  // Copy the subset of properties that are relevant to Logs
  // I'm listing out individual properties here since the names are slightly different in each Config class...
  private[kesque] def copyKafkaConfigToLog(kafkaConfig: KafkaConfig): java.util.Map[String, Object] = {
    val logProps = new java.util.HashMap[String, Object]()
    logProps.put(LogConfig.SegmentBytesProp, kafkaConfig.logSegmentBytes)
    logProps.put(LogConfig.SegmentMsProp, kafkaConfig.logRollTimeMillis)
    logProps.put(LogConfig.SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(LogConfig.SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(LogConfig.FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
    logProps.put(LogConfig.FlushMsProp, kafkaConfig.logFlushIntervalMs)
    logProps.put(LogConfig.RetentionBytesProp, kafkaConfig.logRetentionBytes)
    logProps.put(LogConfig.RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(LogConfig.MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
    logProps.put(LogConfig.IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
    logProps.put(LogConfig.DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(LogConfig.MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
    logProps.put(LogConfig.FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(LogConfig.CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
    logProps.put(LogConfig.MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
    logProps.put(LogConfig.CompressionTypeProp, kafkaConfig.compressionType)
    logProps.put(LogConfig.UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(LogConfig.PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
    logProps.put(LogConfig.MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(LogConfig.MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
    logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs: java.lang.Long)
    logProps
  }

}

class KafkaServer(val config: KafkaConfig, time: Time = Time.SYSTEM) extends Logging {

  val brokerState: BrokerState = new BrokerState

  var logDirFailureChannel: LogDirFailureChannel = null
  var logManager: LogManager = null
  var readerWriter: KafkaReaderWriter = null
  var metadataCache: MetadataCache = null

  var kafkaScheduler: KafkaScheduler = null

  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))).toMap

  private var _brokerTopicStats: BrokerTopicStats = null

  private[kesque] def brokerTopicStats = _brokerTopicStats

  def startup() {
    try {
      info("starting")

      val (brokerId, initialOfflineDirs) = getBrokerIdAndOfflineDirs
      config.brokerId = brokerId

      /* start scheduler */
      kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
      kafkaScheduler.startup()

      _brokerTopicStats = new BrokerTopicStats()

      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

      /* start log manager */
      logManager = createLogManager(config, initialOfflineDirs, Seq(), brokerState, kafkaScheduler, time, brokerTopicStats, logDirFailureChannel)
      logManager.startup()

      metadataCache = new MetadataCache(config.brokerId)
      readerWriter = createReaderWriter()
    } catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        //shutdown()
        throw e
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    try {
      info("shutting down")

      // To ensure correct behavior under concurrent calls, we need to check `shutdownLatch` first since it gets updated
      // last in the `if` block. If the order is reversed, we could shutdown twice or leave `isShuttingDown` set to
      // `true` at the end of this method.
      //if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
      //CoreUtils.swallow(controlledShutdown(), this)
      brokerState.newState(BrokerShuttingDown)

      // Stop socket server to stop accepting any more connections and requests.
      // Socket server will be shutdown towards the end of the sequence.
      kafkaScheduler.shutdown()

      //if (readerWriter != null)
      //  readerWriter.shutdown()

      if (logManager != null)
        logManager.shutdown()

      if (brokerTopicStats != null)
        brokerTopicStats.close()

      brokerState.newState(NotRunning)

      //startupComplete.set(false)
      //isShuttingDown.set(false)
      //CoreUtils.swallow(AppInfoParser.unregisterAppInfo(jmxPrefix, config.brokerId.toString, metrics), this)
      //shutdownLatch.countDown()
      info("shut down completed")
      //}
    } catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        //isShuttingDown.set(false)
        throw e
    }
  }

  protected def createReaderWriter(): KafkaReaderWriter =
    new KafkaReaderWriter(config, time, logManager, brokerTopicStats, metadataCache, logDirFailureChannel)

  /**
   * Generates new brokerId if enabled or reads from meta.properties based on following conditions
   * <ol>
   * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
   * <li> stored broker.id in meta.properties doesn't match in all the log.dirs throws InconsistentBrokerIdException
   * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
   * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
   * <ol>
   *
   * The log directories whose meta.properties can not be accessed due to IOException will be returned to the caller
   *
   * @return A 2-tuple containing the brokerId and a sequence of offline log directories.
   */
  private def getBrokerIdAndOfflineDirs: (Int, Seq[String]) = {
    var brokerId = config.brokerId
    val brokerIdSet = mutable.HashSet[Int]()
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    for (logDir <- config.logDirs) {
      try {
        val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
        brokerMetadataOpt.foreach { brokerMetadata =>
          brokerIdSet.add(brokerMetadata.brokerId)
        }
      } catch {
        case e: IOException =>
          offlineDirs += logDir
          error(s"Fail to read $brokerMetaPropsFile under log directory $logDir", e)
      }
    }

    if (brokerIdSet.size > 1)
      throw new InconsistentBrokerIdException(
        s"Failed to match broker.id across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
          s"or partial data was manually copied from another broker. Found $brokerIdSet"
      )
    else if (brokerId >= 0 && brokerIdSet.size == 1 && brokerIdSet.last != brokerId)
      throw new InconsistentBrokerIdException(
        s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerIdSet.last} in meta.properties. " +
          s"If you moved your data, make sure your configured broker.id matches. " +
          s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs)."
      )
    else if (brokerIdSet.isEmpty && brokerId < 0 && config.brokerIdGenerationEnable) // generate a new brokerId from Zookeeper
      brokerId = generateBrokerId
    else if (brokerIdSet.size == 1) // pick broker.id from meta.properties
      brokerId = brokerIdSet.last

    (brokerId, offlineDirs)
  }

  /**
   * Return a sequence id generated by updating the broker sequence id path in ZK.
   * Users can provide brokerId in the config. To avoid conflicts between ZK generated
   * sequence id and configured brokerId, we increment the generated sequence id by KafkaConfig.MaxReservedBrokerId.
   */
  private def generateBrokerId: Int = {
    try {
      1 //zkClient.generateBrokerSequenceId() + config.maxReservedBrokerId
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }

  private def createLogManager(
    config:               KafkaConfig,
    initialOfflineDirs:   Seq[String],
    allTopics:            Seq[String],
    brokerState:          BrokerState,
    kafkaScheduler:       KafkaScheduler,
    time:                 Time,
    brokerTopicStats:     BrokerTopicStats,
    logDirFailureChannel: LogDirFailureChannel
  ): LogManager = {
    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
    val defaultLogConfig = LogConfig(defaultProps)

    val topicConfigs = allTopics.map { topic =>
      topic -> LogConfig.fromProps(defaultProps, new Properties())
    }.toMap

    // read the log configurations from zookeeper
    val cleanerConfig = CleanerConfig(
      numThreads = config.logCleanerThreads,
      dedupeBufferSize = config.logCleanerDedupeBufferSize,
      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
      ioBufferSize = config.logCleanerIoBufferSize,
      maxMessageSize = config.messageMaxBytes,
      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
      backOffMs = config.logCleanerBackoffMs,
      enableCleaner = config.logCleanerEnable
    )

    new LogManager(
      logDirs = config.logDirs.map(new File(_).getAbsoluteFile),
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      topicConfigs = topicConfigs,
      defaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      ioThreads = config.numRecoveryThreadsPerDataDir,
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      retentionCheckMs = config.logCleanupIntervalMs,
      maxPidExpirationMs = config.transactionIdExpirationMs,
      scheduler = kafkaScheduler,
      brokerState = brokerState,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      time = time
    )
  }

}