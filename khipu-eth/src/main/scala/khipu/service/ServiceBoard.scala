package khipu.service

import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.cluster.Cluster
import akka.event.Logging
import akka.stream.ActorMaterializer
import java.io.File
import java.security.SecureRandom
import kesque.Kesque
import khipu.Hash
import khipu.Khipu
import khipu.NodeStatus
import khipu.ServerStatus
import khipu.blockchain.sync.HostService
import khipu.domain.BlockHeader
import khipu.domain.Blockchain
import khipu.ledger.Ledger
import khipu.network.ForkResolver
import khipu.network.p2p.MessageDecoder
import khipu.network.p2p.messages.Versions
import khipu.network.p2p.messages.PV62.BlockBody
import khipu.network.rlpx.KnownNodesService.KnownNodesServiceConfig
import khipu.network.rlpx.PeerManager
import khipu.network.rlpx.discovery.DiscoveryConfig
import khipu.store.Storages
import khipu.store.datasource.KesqueDataSource
import khipu.store.datasource.SharedLeveldbDataSources
import khipu.util
import khipu.util.BlockchainConfig
import khipu.util.MiningConfig
import khipu.util.PruningConfig
import khipu.util.TxPoolConfig
import khipu.util.cache.CachingSettings
import khipu.util.cache.sync.LfuCache
import khipu.validators.BlockHeaderValidator
import khipu.validators.BlockValidator
import khipu.validators.OmmersValidator
import khipu.validators.SignedTransactionValidator
import khipu.validators.SignedTransactionValidatorImpl
import khipu.validators.Validators
import org.spongycastle.crypto.params.ECPublicKeyParameters
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Per host singleton instance with ActorSystem context
 *
 * We'd like to use the name ServiceBoard instead of ServiceBoardExternsion,
 * so we can reach it via ServiceBoard(system) instead of longer name ServiceBoardExternsion(system)
 */
object ServiceBoard extends ExtensionId[ServiceBoardExtension] with ExtensionIdProvider {
  override def lookup = ServiceBoard
  override def createExtension(system: ExtendedActorSystem) = new ServiceBoardExtension(system)
}

class ServiceBoardExtension(system: ExtendedActorSystem) extends Extension {
  import system.dispatcher

  private val log = Logging(system, this.getClass)

  private val boardConfig = system.settings.config.getConfig("khipu.service-board")
  private val role: Option[String] = boardConfig.getString("role") match {
    case "" => None
    case r  => Some(r)
  }

  /**
   * Returns true if this member is not tagged with the role configured for the mediator.
   */
  def isTerminated: Boolean = Cluster(system).isTerminated || !role.forall(Cluster(system).selfRoles.contains)

  implicit val materializer = ActorMaterializer()(system)

  val config = util.Config.config

  val secureRandomAlgo = if (config.hasPath("secure-random-algo")) Some(config.getString("secure-random-algo")) else None
  val secureRandom = secureRandomAlgo.map(SecureRandom.getInstance(_)).getOrElse(new SecureRandom())
  val nodeKeyFile = util.Config.nodeKeyFile
  val nodeKey = khipu.loadAsymmetricCipherKeyPair(nodeKeyFile, secureRandom)
  log.info(s"nodeKey at $nodeKeyFile: $nodeKey")

  val nodeId = khipu.toHexString(nodeKey.getPublic.asInstanceOf[ECPublicKeyParameters].getQ.getEncoded(false).drop(1))
  log.info(s"nodeId is $nodeId")

  val messageDecoder = MessageDecoder
  val protocolVersion = Versions.PV63

  val blockchainConfig = BlockchainConfig(config)

  val forkResolverOpt = if (blockchainConfig.customGenesisFileOpt.isDefined) None else Some(new ForkResolver.DAOForkResolver(blockchainConfig))

  val storages = new Storages.DefaultStorages with SharedLeveldbDataSources {
    implicit protected val system = ServiceBoardExtension.this.system

    val pruningMode = PruningConfig(config).mode

    private lazy val defaultCachingSettings = CachingSettings(system)
    private lazy val cacheCfg = util.CacheConfig(config)

    private lazy val khipuPath = new File(classOf[Khipu].getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile.getParentFile
    private lazy val configDir = new File(khipuPath, "conf")
    private lazy val kafkaConfigFile = new File(configDir, "kafka.server.properties")
    private lazy val kafkaProps = {
      val props = org.apache.kafka.common.utils.Utils.loadProps(kafkaConfigFile.getAbsolutePath)
      props.put("log.dirs", util.Config.kesqueDir)
      props
    }
    lazy val kesque = new Kesque(kafkaProps)
    log.info(s"Kesque started using config file: $kafkaConfigFile")
    private val futureTables = Future.sequence(List(
      Future(kesque.getTable(Array(KesqueDataSource.account))),
      Future(kesque.getTable(Array(KesqueDataSource.storage))),
      Future(kesque.getTable(Array(KesqueDataSource.evmcode))),
      Future(kesque.getTimedTable(Array(
        KesqueDataSource.header,
        KesqueDataSource.body,
        KesqueDataSource.td,
        KesqueDataSource.receipts
      ), 1024000))
    ))
    private val List(accountTable, storageTable, evmcodeTable, blockTable) = Await.result(futureTables, Duration.Inf)
    //private val headerTable = kesque.getTimedTable(Array(KesqueDataSource.header), 1024000)
    //private val bodyTable = kesque.getTable(Array(KesqueDataSource.body), 1024000)
    //private val tdTable = kesque.getTable(Array(KesqueDataSource.td), 1024000)
    //private val receiptTable = kesque.getTable(Array(KesqueDataSource.receipts), 1024000)

    lazy val accountNodeDataSource = new KesqueDataSource(accountTable, KesqueDataSource.account)
    lazy val storageNodeDataSource = new KesqueDataSource(storageTable, KesqueDataSource.storage)
    lazy val evmCodeDataSource = new KesqueDataSource(evmcodeTable, KesqueDataSource.evmcode)

    lazy val blockHeadersDataSource = new KesqueDataSource(blockTable, KesqueDataSource.header)
    lazy val blockBodiesDataSource = new KesqueDataSource(blockTable, KesqueDataSource.body)
    lazy val totalDifficultiesDataSource = new KesqueDataSource(blockTable, KesqueDataSource.td)
    lazy val receiptsDataSource = new KesqueDataSource(blockTable, KesqueDataSource.receipts)

    protected lazy val nodeKeyValueCache = {
      val lfuCacheSettings = defaultCachingSettings.lfuCacheSettings
        .withInitialCapacity(10000)
        .withMaxCapacity(cacheCfg.cacheSize)
      val cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings)
      LfuCache[Hash, Array[Byte]](cachingSettings)
    }

    protected lazy val blockHeadersCache = {
      val lfuCacheSettings = defaultCachingSettings.lfuCacheSettings
        .withInitialCapacity(2000)
        .withMaxCapacity(5000)
      val cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings)
      LfuCache[Hash, BlockHeader](cachingSettings)
    }

    protected lazy val blockBodiesCache = {
      val lfuCacheSettings = defaultCachingSettings.lfuCacheSettings
        .withInitialCapacity(2000)
        .withMaxCapacity(5000)
      val cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings)
      LfuCache[Hash, BlockBody](cachingSettings)
    }

    protected lazy val blockNumberCache = {
      val lfuCacheSettings = defaultCachingSettings.lfuCacheSettings
        .withInitialCapacity(20000)
        .withMaxCapacity(1000000)
      val cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings)
      LfuCache[Long, Hash](cachingSettings)
    }

  }

  val db = util.Config.Db

  // There should be only one instance, instant it here or a standalone singleton service
  val blockchain: Blockchain = Blockchain(storages)

  val validators = new Validators {
    val blockValidator: BlockValidator = BlockValidator
    val blockHeaderValidator: BlockHeaderValidator.I = new BlockHeaderValidator(blockchainConfig)
    val ommersValidator: OmmersValidator.I = new OmmersValidator(blockchainConfig)
    val signedTransactionValidator: SignedTransactionValidator = new SignedTransactionValidatorImpl(blockchainConfig)
  }

  val ledger: Ledger.I = new Ledger(blockchain, blockchainConfig)(system)

  val miningConfig = MiningConfig(config)

  val txPoolConfig = TxPoolConfig(config)

  val knownNodesServiceConfig = KnownNodesServiceConfig(config)
  val discoveryConfig = DiscoveryConfig(config)

  val networkConfig = util.Config.Network

  val nodeStatus = NodeStatus(
    key = nodeKey,
    serverStatus = ServerStatus.Listening(networkConfig.Server.listenAddress),
    discoveryStatus = ServerStatus.Listening(discoveryConfig.listenAddress)
  )

  val peerConfiguration = util.Config.Network.peer
  // TODO knownNodesService
  val peerManage = system.actorOf(PeerManager.props(peerConfiguration), "peerManage")

  val hostService = new HostService(blockchain, peerConfiguration)

  log.info(s"serviceBoard is ready")
}

