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
import khipu.store.datasource.LmdbHeavyDataSource
import khipu.store.datasource.SharedLeveldbDataSources
import khipu.store.datasource.SharedLmdbDataSources
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
import khipu.validators.Validators
import org.apache.kafka.common.record.CompressionType
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags
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
  val dbConfig = util.Config.Db

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

  val storages = dbConfig.dbEngine match {
    case dbConfig.LMDB =>

      new Storages.DefaultStorages with SharedLmdbDataSources {
        implicit protected val system = ServiceBoardExtension.this.system

        val pruningMode = PruningConfig(config).mode

        private lazy val defaultCachingSettings = CachingSettings(system)
        private lazy val cacheCfg = util.CacheConfig(config)

        private lazy val home = {
          val h = new File(dbConfig.LmdbConfig.path)
          if (!h.exists) {
            h.mkdirs()
          }
          h
        }

        lazy val env = Env.create()
          .setMapSize(dbConfig.LmdbConfig.mapSize)
          .setMaxDbs(dbConfig.LmdbConfig.maxDbs)
          .setMaxReaders(dbConfig.LmdbConfig.maxReaders)
          .open(home, EnvFlags.MDB_NOTLS, EnvFlags.MDB_NORDAHEAD)

        // LMDB defines compile-time constant MDB_MAXKEYSIZE=511 bytes
        // Key sizes must be between 1 and mdb_env_get_maxkeysize() inclusive. 
        // The same applies to data sizes in databases with the MDB_DUPSORT flag. 
        // Other data items can in theory be from 0 to 0xffffffff bytes long. 
        // Thus, do not add DbiFlags.MDB_DUPSORT on evmcodeDb
        lazy val accountDb = env.openDbi(dbConfig.account, DbiFlags.MDB_CREATE) // DbiFlags.MDB_DUPSORT
        lazy val storageDb = env.openDbi(dbConfig.storage, DbiFlags.MDB_CREATE) // DbiFlags.MDB_DUPSORT
        lazy val evmcodeDb = env.openDbi(dbConfig.evmcode, DbiFlags.MDB_CREATE)

        lazy val blockHeaderDb = env.openDbi(dbConfig.header, DbiFlags.MDB_CREATE)
        lazy val blockBodyDb = env.openDbi(dbConfig.body, DbiFlags.MDB_CREATE)
        lazy val reaceiptsDb = env.openDbi(dbConfig.receipts, DbiFlags.MDB_CREATE)
        lazy val totalDifficultyDb = env.openDbi(dbConfig.td, DbiFlags.MDB_CREATE)

        lazy val accountNodeDataSource = new LmdbHeavyDataSource(dbConfig.account, env, accountDb, cacheCfg.cacheSize, isKeyTheValueHash = true)
        lazy val storageNodeDataSource = new LmdbHeavyDataSource(dbConfig.storage, env, storageDb, cacheCfg.cacheSize, isKeyTheValueHash = true)
        lazy val evmCodeDataSource = new LmdbHeavyDataSource(dbConfig.evmcode, env, evmcodeDb)

        lazy val blockHeaderDataSource = new LmdbHeavyDataSource(dbConfig.header, env, blockHeaderDb, isWithTimestamp = true)
        lazy val blockBodyDataSource = new LmdbHeavyDataSource(dbConfig.body, env, blockBodyDb)
        lazy val receiptsDataSource = new LmdbHeavyDataSource(dbConfig.receipts, env, reaceiptsDb)
        lazy val totalDifficultyDataSource = new LmdbHeavyDataSource(dbConfig.td, env, totalDifficultyDb)

        protected lazy val nodeKeyValueCache = {
          val lfuCacheSettings = defaultCachingSettings.lfuCacheSettings
            .withInitialCapacity(10000)
            .withMaxCapacity(cacheCfg.cacheSize)
          val cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings)
          LfuCache[Hash, Array[Byte]](cachingSettings)
        }

        protected lazy val blockHeaderCache = {
          val lfuCacheSettings = defaultCachingSettings.lfuCacheSettings
            .withInitialCapacity(2000)
            .withMaxCapacity(5000)
          val cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings)
          LfuCache[Hash, BlockHeader](cachingSettings)
        }

        protected lazy val blockBodyCache = {
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

    case dbConfig.KESQUE =>

      new Storages.DefaultStorages with SharedLeveldbDataSources {
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
        // block size evalution: https://etherscan.io/chart/blocksize, https://ethereum.stackexchange.com/questions/1106/is-there-a-limit-for-transaction-size/1110#1110
        // trie node size evalution:
        //   LeafNode - 256bytes(key) + value ~ 256 + value
        //   ExtensionNode - 256bytes(key) + 256bytes(hash) ~ 512
        //   BranchNode - 32bytes (children) + (256bytes(key) + value) (terminator with k-v) ~ 288 + value
        // account trie node size evalution: account value - 4x256bytes ~ 288 + 1024
        // storage trie node size evalution: storage valye - 256bytes ~ 288 + 256 
        private val futureTables = Future.sequence(List(
          Future(kesque.getTable(Array(dbConfig.account), 4096, CompressionType.NONE, cacheCfg.cacheSize)),
          Future(kesque.getTable(Array(dbConfig.storage), 4096, CompressionType.NONE, cacheCfg.cacheSize)),
          Future(kesque.getTable(Array(dbConfig.evmcode), 24576)),
          Future(kesque.getTimedTable(Array(
            dbConfig.header,
            dbConfig.body,
            dbConfig.receipts,
            dbConfig.td
          ), 102400))
        ))
        private val List(accountTable, storageTable, evmcodeTable, blockTable) = Await.result(futureTables, Duration.Inf)
        //private val headerTable = kesque.getTimedTable(Array(KesqueDataSource.header), 1024000)
        //private val bodyTable = kesque.getTable(Array(KesqueDataSource.body), 1024000)
        //private val tdTable = kesque.getTable(Array(KesqueDataSource.td), 1024000)
        //private val receiptTable = kesque.getTable(Array(KesqueDataSource.receipts), 1024000)

        lazy val accountNodeDataSource = new KesqueDataSource(accountTable, dbConfig.account)
        lazy val storageNodeDataSource = new KesqueDataSource(storageTable, dbConfig.storage)
        lazy val evmCodeDataSource = new KesqueDataSource(evmcodeTable, dbConfig.evmcode)

        lazy val blockHeaderDataSource = new KesqueDataSource(blockTable, dbConfig.header)
        lazy val blockBodyDataSource = new KesqueDataSource(blockTable, dbConfig.body)
        lazy val receiptsDataSource = new KesqueDataSource(blockTable, dbConfig.receipts)
        lazy val totalDifficultyDataSource = new KesqueDataSource(blockTable, dbConfig.td)

        protected lazy val nodeKeyValueCache = {
          val lfuCacheSettings = defaultCachingSettings.lfuCacheSettings
            .withInitialCapacity(10000)
            .withMaxCapacity(cacheCfg.cacheSize)
          val cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings)
          LfuCache[Hash, Array[Byte]](cachingSettings)
        }

        protected lazy val blockHeaderCache = {
          val lfuCacheSettings = defaultCachingSettings.lfuCacheSettings
            .withInitialCapacity(2000)
            .withMaxCapacity(5000)
          val cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings)
          LfuCache[Hash, BlockHeader](cachingSettings)
        }

        protected lazy val blockBodyCache = {
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
  }

  // There should be only one instance, instant it here or a standalone singleton service
  val blockchain: Blockchain = Blockchain(storages)

  val validators = new Validators {
    val blockValidator: BlockValidator = BlockValidator
    val blockHeaderValidator: BlockHeaderValidator.I = new BlockHeaderValidator(blockchainConfig)
    val ommersValidator: OmmersValidator.I = new OmmersValidator(blockchainConfig)
    val signedTransactionValidator = new SignedTransactionValidator(blockchainConfig)
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

