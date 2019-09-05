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
import khipu.Khipu
import khipu.NodeStatus
import khipu.ServerStatus
import khipu.blockchain.sync.HostService
import khipu.config.BlockchainConfig
import khipu.config.DbConfig
import khipu.config.KhipuConfig
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
import khipu.storage.Storages
import khipu.storage.datasource.KesqueLmdbDataSources
import khipu.storage.datasource.KesqueRocksdbDataSources
import khipu.storage.datasource.LmdbDataSources
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
        protected val datadir = KhipuConfig.datadir

        val unconfirmedDepth = KhipuConfig.Sync.blockResolveDepth
      }

    case DbConfig.KESQUE_LMDB =>
      new Storages.DefaultStorages with KesqueLmdbDataSources {
        implicit protected val system = ServiceBoardExtension.this.system

        protected val config = ServiceBoardExtension.this.config
        protected val log = ServiceBoardExtension.this.log
        protected val datadir = KhipuConfig.datadir
        protected val khipuPath = new File(classOf[Khipu].getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile.getParentFile

        val unconfirmedDepth = KhipuConfig.Sync.blockResolveDepth

        log.info(s"Kesque started using config file: $kafkaConfigFile")
      }

    case DbConfig.KESQUE_ROCKSDB =>
      new Storages.DefaultStorages with KesqueRocksdbDataSources {
        implicit protected val system = ServiceBoardExtension.this.system

        protected val config = ServiceBoardExtension.this.config
        protected val log = ServiceBoardExtension.this.log
        protected val datadir = KhipuConfig.datadir
        protected val khipuPath = new File(classOf[Khipu].getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile.getParentFile

        val unconfirmedDepth = KhipuConfig.Sync.blockResolveDepth

        log.info(s"Kesque started using config file: $kafkaConfigFile")
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

  val networkConfig = KhipuConfig.Network

  val nodeStatus = NodeStatus(
    key = nodeKey,
    serverStatus = ServerStatus.Listening(networkConfig.Server.listenAddress),
    discoveryStatus = ServerStatus.Listening(discoveryConfig.listenAddress)
  )

  val peerConfig = KhipuConfig.Network.peer
  // TODO knownNodesService
  val peerManage = system.actorOf(PeerManager.props(peerConfig), "peerManage")

  val hostService = new HostService(blockchain, peerConfig)

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

