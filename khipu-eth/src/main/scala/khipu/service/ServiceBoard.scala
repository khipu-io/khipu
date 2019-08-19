package khipu.service

import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.cluster.Cluster
import akka.event.Logging
import akka.stream.ActorMaterializer
import java.io.File
import java.io.PrintWriter
import java.security.SecureRandom
import khipu.NodeStatus
import khipu.ServerStatus
import khipu.blockchain.sync.HostService
import khipu.config.BlockchainConfig
import khipu.config.DbConfig
import khipu.config.KhipuConfig
import khipu.config.LmdbConfig
import khipu.config.MiningConfig
import khipu.config.TxPoolConfig
import khipu.crypto
import khipu.domain.Blockchain
import khipu.ledger.Ledger
import khipu.network.ForkResolver
import khipu.network.p2p.MessageDecoder
import khipu.network.p2p.messages.Versions
import khipu.network.rlpx.KnownNodesService.KnownNodesServiceConfig
import khipu.network.rlpx.PeerManager
import khipu.network.rlpx.discovery.DiscoveryConfig
import khipu.store.Storages
import khipu.store.datasource.LmdbDataSources
import khipu.validators.BlockHeaderValidator
import khipu.validators.BlockValidator
import khipu.validators.OmmersValidator
import khipu.validators.SignedTransactionValidator
import khipu.validators.Validators
import org.spongycastle.crypto.AsymmetricCipherKeyPair
import org.spongycastle.crypto.params.ECPublicKeyParameters
import scala.concurrent.duration._
import scala.io.Source

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

  val config = KhipuConfig.config
  val dbConfig = new DbConfig(config.getConfig("db"))

  val secureRandomAlgo = if (config.hasPath("secure-random-algo")) Some(config.getString("secure-random-algo")) else None
  val secureRandom = secureRandomAlgo.map(SecureRandom.getInstance(_)).getOrElse(new SecureRandom())
  val nodeKeyFile = KhipuConfig.nodeKeyFile
  val nodeKey = loadAsymmetricCipherKeyPair(nodeKeyFile, secureRandom)
  log.info(s"nodeKey at $nodeKeyFile: $nodeKey")

  val nodeId = khipu.toHexString(nodeKey.getPublic.asInstanceOf[ECPublicKeyParameters].getQ.getEncoded(false).drop(1))
  log.info(s"nodeId is $nodeId")

  val messageDecoder = MessageDecoder
  val protocolVersion = Versions.PV63

  val blockchainConfig = BlockchainConfig(config)

  val forkResolverOpt = if (blockchainConfig.customGenesisFileOpt.isDefined) None else Some(new ForkResolver.DAOForkResolver(blockchainConfig))

  val storages = dbConfig.dbEngine match {
    case DbConfig.LMDB =>

      new Storages.DefaultStorages with LmdbDataSources {
        implicit protected val system = ServiceBoardExtension.this.system

        protected val config = ServiceBoardExtension.this.config
        protected val log = ServiceBoardExtension.this.log
        protected val lmdbConfig = new LmdbConfig(KhipuConfig.datadir, config.getConfig("db").getConfig("lmdb"))

        val unconfirmedDepth = KhipuConfig.Sync.blockResolveDepth
      }

    case DbConfig.KESQUE => null // disabled KESQUE

    //      new Storages.DefaultStorages with SharedLeveldbDataSources {
    //        implicit protected val system = ServiceBoardExtension.this.system
    //
    //        private lazy val defaultCachingSettings = CachingSettings(system)
    //        private lazy val cacheCfg = util.CacheConfig(config)
    //
    //        private lazy val khipuPath = new File(classOf[Khipu].getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile.getParentFile
    //        private lazy val configDir = new File(khipuPath, "conf")
    //        private lazy val kafkaConfigFile = new File(configDir, "kafka.server.properties")
    //        private lazy val kafkaProps = {
    //          val props = org.apache.kafka.common.utils.Utils.loadProps(kafkaConfigFile.getAbsolutePath)
    //          props.put("log.dirs", KhipuConfig.kesqueDir)
    //          props
    //        }
    //        lazy val kesque = new Kesque(kafkaProps)
    //        log.info(s"Kesque started using config file: $kafkaConfigFile")
    //        // block size evalution: https://etherscan.io/chart/blocksize, https://ethereum.stackexchange.com/questions/1106/is-there-a-limit-for-transaction-size/1110#1110
    //        // trie node size evalution:
    //        //   LeafNode - 256bytes(key) + value ~ 256 + value
    //        //   ExtensionNode - 256bytes(key) + 256bytes(hash) ~ 512
    //        //   BranchNode - 32bytes (children) + (256bytes(key) + value) (terminator with k-v) ~ 288 + value
    //        // account trie node size evalution: account value - 4x256bytes ~ 288 + 1024
    //        // storage trie node size evalution: storage valye - 256bytes ~ 288 + 256 
    //        private val futureTables = Future.sequence(List(
    //          Future(kesque.getTable(Array(dbConfig.account), 4096, CompressionType.NONE, cacheCfg.cacheSize)),
    //          Future(kesque.getTable(Array(dbConfig.storage), 4096, CompressionType.NONE, cacheCfg.cacheSize)),
    //          Future(kesque.getTable(Array(dbConfig.evmcode), 24576)),
    //          Future(kesque.getTimedTable(Array(
    //            dbConfig.header,
    //            dbConfig.body,
    //            dbConfig.receipts,
    //            dbConfig.td
    //          ), 102400))
    //        ))
    //        private val List(accountTable, storageTable, evmcodeTable, blockTable) = Await.result(futureTables, Duration.Inf)
    //        //private val headerTable = kesque.getTimedTable(Array(KesqueDataSource.header), 1024000)
    //        //private val bodyTable = kesque.getTable(Array(KesqueDataSource.body), 1024000)
    //        //private val tdTable = kesque.getTable(Array(KesqueDataSource.td), 1024000)
    //        //private val receiptTable = kesque.getTable(Array(KesqueDataSource.receipts), 1024000)
    //
    //        lazy val accountNodeDataSource = new KesqueDataSource(accountTable, dbConfig.account)
    //        lazy val storageNodeDataSource = new KesqueDataSource(storageTable, dbConfig.storage)
    //        lazy val evmCodeDataSource = new KesqueDataSource(evmcodeTable, dbConfig.evmcode)
    //
    //        lazy val blockHeaderDataSource = new KesqueDataSource(blockTable, dbConfig.header)
    //        lazy val blockBodyDataSource = new KesqueDataSource(blockTable, dbConfig.body)
    //        lazy val receiptsDataSource = new KesqueDataSource(blockTable, dbConfig.receipts)
    //        lazy val totalDifficultyDataSource = new KesqueDataSource(blockTable, dbConfig.td)
    //      }
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

  val networkConfig = KhipuConfig.Network

  val nodeStatus = NodeStatus(
    key = nodeKey,
    serverStatus = ServerStatus.Listening(networkConfig.Server.listenAddress),
    discoveryStatus = ServerStatus.Listening(discoveryConfig.listenAddress)
  )

  val peerConfiguration = KhipuConfig.Network.peer
  // TODO knownNodesService
  val peerManage = system.actorOf(PeerManager.props(peerConfiguration), "peerManage")

  val hostService = new HostService(blockchain, peerConfiguration)

  log.info(s"serviceBoard is ready")

  def loadAsymmetricCipherKeyPair(filePath: String, secureRandom: SecureRandom): AsymmetricCipherKeyPair = {
    val file = new File(filePath)
    if (!file.exists) {
      val keysValuePair = crypto.generateKeyPair(secureRandom)

      // write keys to file
      val (priv, _) = crypto.keyPairToByteArrays(keysValuePair)
      require(file.getParentFile.exists || file.getParentFile.mkdirs(), "Key's file parent directory creation failed")
      val writer = new PrintWriter(filePath)
      try {
        writer.write(khipu.toHexString(priv))
      } finally {
        writer.close()
      }

      keysValuePair
    } else {
      val reader = Source.fromFile(filePath)
      try {
        val privHex = reader.mkString
        crypto.keyPairFromPrvKey(khipu.hexDecode(privHex))
      } finally {
        reader.close()
      }
    }
  }

}

