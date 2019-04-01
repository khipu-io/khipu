package khipu.util

import akka.util.ByteString
import com.typesafe.config.{ ConfigFactory, Config => TypesafeConfig }
import java.math.BigInteger
import java.net.InetSocketAddress
import khipu.UInt256
import khipu.domain.Address
import khipu.jsonrpc.JsonRpcController.JsonRpcConfig
import khipu.jsonrpc.http.JsonRpcHttpServer.JsonRpcHttpServerConfig
import khipu.network.rlpx.FastSyncHostConfiguration
import khipu.network.rlpx.PeerConfiguration
import khipu.network.rlpx.RLPxConfiguration
import khipu.store.datasource.LeveldbConfig
import khipu.store.trienode.ArchivePruning
import khipu.store.trienode.HistoryPruning
import khipu.store.trienode.PruningMode
import scala.concurrent.duration._
import scala.util.Try

object Config {

  val config = ConfigFactory.load().getConfig("khipu")

  val clientId = config.getString("client-id")
  val clientVersion = config.getString("client-version")
  val chainType = config.getString("chain-type")
  val datadir = config.getString("datadir") + "." + chainType
  val nodeKeyFile = datadir + "/" + config.getString("node-key-file")
  val keyStoreDir = datadir + "/" + config.getString("keystore-dir")
  val kesqueDir = datadir + "/" + config.getString("kesque-dir")

  val shutdownTimeout = config.getDuration("shutdown-timeout").toMillis.millis

  val secureRandomAlgo = if (config.hasPath("secure-random-algo")) Some(config.getString("secure-random-algo")) else None

  object Network {
    private val networkConfig = config.getConfig("network")

    val protocolVersion = networkConfig.getString("protocol-version")

    object Server {
      private val serverConfig = networkConfig.getConfig("server-address")

      val interface = serverConfig.getString("interface")
      val port = serverConfig.getInt("port")
      val listenAddress = new InetSocketAddress(interface, port)
    }

    val peer = new PeerConfiguration {
      private val peerConfig = networkConfig.getConfig("peer")

      val connectRetryDelay = peerConfig.getDuration("connect-retry-delay").toMillis.millis
      val connectMaxRetries = peerConfig.getInt("connect-max-retries")
      val disconnectPoisonPillTimeout = peerConfig.getDuration("disconnect-poison-pill-timeout").toMillis.millis
      val waitForHelloTimeout = peerConfig.getDuration("wait-for-hello-timeout").toMillis.millis
      val waitForStatusTimeout = peerConfig.getDuration("wait-for-status-timeout").toMillis.millis
      val waitForChainCheckTim = peerConfig.getDuration("wait-for-chain-check-timeout").toMillis.millis
      val waitForChainCheckTimeout = peerConfig.getDuration("wait-for-chain-check-timeout").toMillis.millis
      val maxIncomingPeers = peerConfig.getInt("max-incoming-peers")
      val maxPeers = math.max(peerConfig.getInt("max-peers"), maxIncomingPeers)
      val networkId = peerConfig.getInt("network-id")

      val rlpxConfiguration = new RLPxConfiguration {
        val waitForHandshakeTimeout = peerConfig.getDuration("wait-for-handshake-timeout").toMillis.millis
        val waitForTcpAckTimeout = peerConfig.getDuration("wait-for-tcp-ack-timeout").toMillis.millis
      }

      val fastSyncHostConfiguration = new FastSyncHostConfiguration {
        val maxBlocksHeadersPerMessage = peerConfig.getInt("max-blocks-headers-per-message")
        val maxBlocksBodiesPerMessage = peerConfig.getInt("max-blocks-bodies-per-message")
        val maxReceiptsPerMessage = peerConfig.getInt("max-receipts-per-message")
        val maxMptComponentsPerMessage = peerConfig.getInt("max-mpt-components-per-message")
      }
      override val updateNodesInitialDelay = peerConfig.getDuration("update-nodes-initial-delay").toMillis.millis
      override val updateNodesInterval = peerConfig.getDuration("update-nodes-interval").toMillis.millis
    }

    object Rpc extends JsonRpcHttpServerConfig with JsonRpcConfig {
      private val rpcConfig = networkConfig.getConfig("rpc")

      val enabled = rpcConfig.getBoolean("enabled")
      val interface = rpcConfig.getString("interface")
      val port = rpcConfig.getInt("port")

      val apis = {
        val providedApis = rpcConfig.getString("apis").split(",").map(_.trim.toLowerCase)
        val invalidApis = providedApis.diff(List("web3", "eth", "net", "personal"))
        require(invalidApis.isEmpty, s"Invalid RPC APIs specified: ${invalidApis.mkString(",")}")
        providedApis
      }
    }
  }

  object Sync {
    private val syncConfig = config.getConfig("sync")

    val doFastSync: Boolean = syncConfig.getBoolean("do-fast-sync")

    val peersScanInterval = syncConfig.getDuration("peers-scan-interval").toMillis.millis
    val blacklistDuration = syncConfig.getDuration("blacklist-duration").toMillis.millis
    val startRetryInterval = syncConfig.getDuration("start-retry-interval").toMillis.millis
    val syncRetryInterval = syncConfig.getDuration("sync-retry-interval").toMillis.millis
    val peerResponseTimeout = syncConfig.getDuration("peer-response-timeout").toMillis.millis
    val reportStatusInterval = syncConfig.getDuration("report-status-interval").toMillis.millis

    val maxConcurrentRequests = syncConfig.getInt("max-concurrent-requests")
    val blockHeadersPerRequest = syncConfig.getInt("block-headers-per-request")
    val blockBodiesPerRequest = syncConfig.getInt("block-bodies-per-request")
    val receiptsPerRequest = syncConfig.getInt("receipts-per-request")
    val nodesPerRequest = syncConfig.getInt("nodes-per-request")
    val minPeersToChooseTargetBlock = syncConfig.getInt("min-peers-to-choose-target-block")
    val targetBlockOffset = syncConfig.getInt("target-block-offset")
    val persistStateSnapshotInterval = syncConfig.getDuration("persist-state-snapshot-interval").toMillis.millis

    val checkForNewBlockInterval = syncConfig.getDuration("check-for-new-block-interval").toMillis.millis
    val blockResolveDepth = syncConfig.getInt("block-resolving-depth")
    val blockchainOnlyPeersPoolSize = syncConfig.getInt("fastsync-block-chain-only-peers-pool")

    val syncRequestTimeout = syncConfig.getDuration("sync-request-timeout").toMillis.millis
    val reimportFromBlockNumber = syncConfig.getLong("reimport-from-block-number")
  }

  trait DbConfig {
    val batchSize: Int
  }
  object Db extends DbConfig {

    private val dbConfig = config.getConfig("db")
    private val leveldbConfig = dbConfig.getConfig("leveldb")

    val batchSize = dbConfig.getInt("batch-size")

    object Leveldb extends LeveldbConfig {
      val path = datadir + "/" + leveldbConfig.getString("path")
      val createIfMissing = leveldbConfig.getBoolean("create-if-missing")
      val paranoidChecks = leveldbConfig.getBoolean("paranoid-checks")
      val verifyChecksums = leveldbConfig.getBoolean("verify-checksums")
      val cacheSize = leveldbConfig.getLong("cache-size")
    }
  }

}

object FilterConfig {
  def apply(etcClientConfig: TypesafeConfig): FilterConfig = {
    val filterConfig = etcClientConfig.getConfig("filter")

    new FilterConfig {
      val filterTimeout = filterConfig.getDuration("filter-timeout").toMillis.millis
      val filterManagerQueryTimeout = filterConfig.getDuration("filter-manager-query-timeout").toMillis.millis
    }
  }
}
trait FilterConfig {
  val filterTimeout: FiniteDuration
  val filterManagerQueryTimeout: FiniteDuration
}

object TxPoolConfig {
  def apply(etcClientConfig: com.typesafe.config.Config): TxPoolConfig = {
    val txPoolConfig = etcClientConfig.getConfig("txPool")

    new TxPoolConfig {
      val txPoolSize = txPoolConfig.getInt("tx-pool-size")
      val pendingTxManagerQueryTimeout = txPoolConfig.getDuration("pending-tx-manager-query-timeout").toMillis.millis
    }
  }
}
trait TxPoolConfig {
  val txPoolSize: Int
  val pendingTxManagerQueryTimeout: FiniteDuration
}

object MiningConfig {
  def apply(etcClientConfig: TypesafeConfig): MiningConfig = {
    val miningConfig = etcClientConfig.getConfig("mining")

    new MiningConfig {
      val coinbase = Address(miningConfig.getString("coinbase"))
      val blockCacheSize = miningConfig.getInt("block-cashe-size")
      val ommersPoolSize = miningConfig.getInt("ommers-pool-size")
      val activeTimeout = miningConfig.getDuration("active-timeout").toMillis.millis
      val ommerPoolQueryTimeout = miningConfig.getDuration("ommer-pool-query-timeout").toMillis.millis
    }
  }
}
trait MiningConfig {
  val ommersPoolSize: Int
  val blockCacheSize: Int
  val coinbase: Address
  val activeTimeout: FiniteDuration
  val ommerPoolQueryTimeout: FiniteDuration
}

object BlockchainConfig {
  def apply(clientConfig: TypesafeConfig): BlockchainConfig = {
    val blockchainConfig = clientConfig.getConfig("blockchain")

    new BlockchainConfig {
      val frontierBlockNumber = blockchainConfig.getLong("frontier-block-number")
      val homesteadBlockNumber = blockchainConfig.getLong("homestead-block-number")
      val eip150BlockNumber = blockchainConfig.getLong("eip150-block-number")
      val eip155BlockNumber = blockchainConfig.getLong("eip155-block-number")
      val eip160BlockNumber = blockchainConfig.getLong("eip160-block-number")
      val eip161BlockNumber = blockchainConfig.getLong("eip161-block-number")
      val eip161PatchBlockNumber = blockchainConfig.getLong("eip161-patch-block-number")
      val eip170BlockNumber = blockchainConfig.getLong("eip170-block-number")

      val eip140BlockNumber = blockchainConfig.getLong("eip140-block-number")
      val eip658BlockNumber = blockchainConfig.getLong("eip658-block-number")
      val eip213BlockNumber = blockchainConfig.getLong("eip213-block-number")
      val eip212BlockNumber = blockchainConfig.getLong("eip212-block-number")
      val eip198BlockNumber = blockchainConfig.getLong("eip198-block-number")
      val eip211BlockNumber = blockchainConfig.getLong("eip211-block-number")
      val eip214BlockNumber = blockchainConfig.getLong("eip214-block-number")
      val eip100BlockNumber = blockchainConfig.getLong("eip100-block-number")
      val eip649BlockNumber = blockchainConfig.getLong("eip649-block-number")
      val byzantiumBlockNumber = blockchainConfig.getLong("byzantium-block-number")
      val constantinopleBlockNumber = blockchainConfig.getLong("constantinople-block-number")

      val difficultyBombPauseBlockNumber = blockchainConfig.getLong("difficulty-bomb-pause-block-number")
      val difficultyBombContinueBlockNumber = blockchainConfig.getLong("difficulty-bomb-continue-block-number")

      val customGenesisFileOpt = Try(blockchainConfig.getString("custom-genesis-file")).toOption

      val daoForkBlockNumber = blockchainConfig.getLong("dao-fork-block-number")
      val daoForkBlockHash = ByteString(khipu.hexDecode(blockchainConfig.getString("dao-fork-block-hash")))
      val accountStartNonce = UInt256(blockchainConfig.getInt("account-start-nonce"))

      val chainId = khipu.hexDecode(blockchainConfig.getString("chain-id")).head

      val monetaryPolicyConfig = MonetaryPolicyConfig(blockchainConfig.getConfig("monetary-policy"))

      val isDebugTraceEnabled = blockchainConfig.getBoolean("debug-trace-enabled")
    }
  }
}
trait BlockchainConfig {
  def frontierBlockNumber: Long
  def homesteadBlockNumber: Long

  def eip150BlockNumber: Long
  def eip155BlockNumber: Long
  def eip160BlockNumber: Long
  def eip170BlockNumber: Long
  def eip161BlockNumber: Long
  def eip161PatchBlockNumber: Long

  def eip140BlockNumber: Long
  def eip658BlockNumber: Long
  def eip213BlockNumber: Long
  def eip212BlockNumber: Long
  def eip198BlockNumber: Long
  def eip211BlockNumber: Long
  def eip214BlockNumber: Long
  def eip100BlockNumber: Long
  def eip649BlockNumber: Long
  def byzantiumBlockNumber: Long
  def constantinopleBlockNumber: Long

  def difficultyBombPauseBlockNumber: Long
  def difficultyBombContinueBlockNumber: Long

  def customGenesisFileOpt: Option[String]

  def daoForkBlockNumber: Long
  def daoForkBlockHash: ByteString
  def accountStartNonce: UInt256

  def chainId: Byte

  def monetaryPolicyConfig: MonetaryPolicyConfig

  def isDebugTraceEnabled: Boolean
}

object MonetaryPolicyConfig {
  def apply(mpConfig: TypesafeConfig): MonetaryPolicyConfig = {
    MonetaryPolicyConfig(
      mpConfig.getLong("era-duration"),
      mpConfig.getDouble("reward-reduction-rate"),
      UInt256(new BigInteger(mpConfig.getString("first-era-block-reward"))),
      UInt256(new BigInteger(mpConfig.getString("byzantium-block-reward"))),
      UInt256(new BigInteger(mpConfig.getString("constantinople-block-reward")))
    )
  }
}
final case class MonetaryPolicyConfig(
    eraDuration:               Long,
    rewardRedutionRate:        Double,
    firstEraBlockReward:       UInt256,
    byzantiumBlockReward:      UInt256,
    constantinopleBlockReward: UInt256
) {
  require(
    rewardRedutionRate >= 0.0 && rewardRedutionRate <= 1.0,
    "reward-reduction-rate should be a value in range [0.0, 1.0]"
  )
}

object PruningConfig {
  def apply(etcClientConfig: com.typesafe.config.Config): PruningConfig = {
    val pruningConfig = etcClientConfig.getConfig("pruning")

    val pruningMode: PruningMode = pruningConfig.getString("mode") match {
      case "basic"   => HistoryPruning(pruningConfig.getInt("history"))
      case "archive" => ArchivePruning
    }

    new PruningConfig {
      override val mode: PruningMode = pruningMode
    }
  }
}
trait PruningConfig {
  val mode: PruningMode
}

object CacheConfig {
  def apply(clientConfig: TypesafeConfig): CacheConfig = {
    val cacheConfig = clientConfig.getConfig("cache")

    new CacheConfig {
      val cacheSize = cacheConfig.getInt("cache-size")
    }
  }
}

trait CacheConfig {
  val cacheSize: Int
}
