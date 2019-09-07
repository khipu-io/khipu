package khipu

import akka.Done
import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown
import akka.event.Logging
import akka.pattern.ask
import java.net.URI
import khipu.blockchain.data.GenesisDataLoader
import khipu.blockchain.sync.SyncService
import khipu.config.FilterConfig
import khipu.config.KhipuConfig
import khipu.entity.NodeEntity
import khipu.jsonrpc.EthService
import khipu.jsonrpc.FilterManager
import khipu.jsonrpc.JsonRpcController
import khipu.jsonrpc.NetService
import khipu.jsonrpc.NetService.NetServiceConfig
import khipu.jsonrpc.PersonalService
import khipu.jsonrpc.Web3Service
import khipu.jsonrpc.http.JsonRpcHttpServer
import khipu.keystore.KeyStore
import khipu.mining.BlockGenerator
import khipu.network.KnownNodesService
import khipu.network.OutgoingPeer
import khipu.network.Peer
import khipu.network.handshake.EtcHandshake
import khipu.network.rlpx.RLPx
import khipu.network.rlpx.auth.AuthHandshake
import khipu.network.rlpx.discovery.NodeDiscoveryService
import khipu.service.ServiceBoard
import khipu.transactions.PendingTransactionsService
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * DAO-fork in EthereumJ
 * 1. org.ethereum.config.blockchain.DaoHFConfig#hardForkTransfers will be executed
 *    when block.getNumber() == forkBlockNumber in
 *    org.ethereum.core.BlockchainImpl.applyBlock
 * 2. and also in org.ethereum.config.blockchain.AbstractDaoConfig#initDaoConfig,
 *    added a new ExtraDataPresenceRule(DAO_EXTRA_DATA) to headerValidators()
 *    when block.getNumber() == forkBlockNumber
 *
 * https://github.com/ethereum/go-ethereum/pull/2814
 * https://github.com/ethereum/go-ethereum/pull/2814/files
 *
 * DAO challenge after peer handshake:
 * eth/handler.go
 */
object Khipu {
  implicit val system = ActorSystem("khipu")

  private val log = Logging(system, this.getClass)

  def nodeDiscoveryService = NodeDiscoveryService.proxy(system)
  def syncService = SyncService.proxy(system)

  val serviceBoard = ServiceBoard(system)
  NodeEntity.startSharding(system)

  def main(args: Array[String]) {

    CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "close storage") { () =>
      import system.dispatcher
      Future {
        serviceBoard.storages.stop()
        log.info("Actor system is going to terminate...")
      } map { _ =>
        Done
      }
    }

    val genesisDataLoader = new GenesisDataLoader(serviceBoard.storages.dataSource, serviceBoard.blockchain, serviceBoard.blockchainConfig)
    genesisDataLoader.loadGenesisData()

    //new khipu.storage.HashedKeyValueStorage(serviceBoard.blockchain).loadAndSeekFromKafka(5000000)
    //new khipu.storage.HashedKeyValueStorage(serviceBoard.blockchain)(system).loadFromKafka()
    //new khipu.tools.BlockDump(serviceBoard.blockchain, serviceBoard.storages.kesque)(system).dump()
    //new khipu.tools.DataFinder()(system).find()

    log.info(s"Best block number is ${serviceBoard.storages.bestBlockNumber}")

    if (serviceBoard.discoveryConfig.discoveryEnabled) {
      startDiscoveryService()
    }
    startPendingTxService()
    startSyncService()
    startIncomingServer()

    syncService ! SyncService.StartSync

    startJsonRpcServer()
  }

  val bootstrap_nodes = Array(
    "enode://e809c4a2fec7daed400e5e28564e23693b23b2cc5a019b612505631bbe7b9ccf709c1796d2a3d29ef2b045f210caf51e3c4f5b6d3587d43ad5d6397526fa6179@174.112.32.157:30303",
    "enode://6e538e7c1280f0a31ff08b382db5302480f775480b8e68f8febca0ceff81e4b19153c6f8bf60313b93bef2cc34d34e1df41317de0ce613a201d1660a788a03e2@52.206.67.235:30303",
    "enode://5fbfb426fbb46f8b8c1bd3dd140f5b511da558cd37d60844b525909ab82e13a25ee722293c829e52cb65c2305b1637fa9a2ea4d6634a224d5f400bfe244ac0de@162.243.55.45:30303",
    "enode://42d8f29d1db5f4b2947cd5c3d76c6d0d3697e6b9b3430c3d41e46b4bb77655433aeedc25d4b4ea9d8214b6a43008ba67199374a9b53633301bca0cd20c6928ab@104.155.176.151:30303",
    "enode://814920f1ec9510aa9ea1c8f79d8b6e6a462045f09caa2ae4055b0f34f7416fca6facd3dd45f1cf1673c0209e0503f02776b8ff94020e98b6679a0dc561b4eba0@104.154.136.117:30303",
    "enode://72e445f4e89c0f476d404bc40478b0df83a5b500d2d2e850e08eb1af0cd464ab86db6160d0fde64bd77d5f0d33507ae19035671b3c74fec126d6e28787669740@104.198.71.200:30303",
    "enode://5cd218959f8263bc3721d7789070806b0adff1a0ed3f95ec886fb469f9362c7507e3b32b256550b9a7964a23a938e8d42d45a0c34b332bfebc54b29081e83b93@35.187.57.94:30303",
    "enode://39abab9d2a41f53298c0c9dc6bbca57b0840c3ba9dccf42aa27316addc1b7e56ade32a0a9f7f52d6c5db4fe74d8824bcedfeaecf1a4e533cacb71cf8100a9442@144.76.238.49:30303",
    "enode://f50e675a34f471af2438b921914b5f06499c7438f3146f6b8936f1faeb50b8a91d0d0c24fb05a66f05865cd58c24da3e664d0def806172ddd0d4c5bdbf37747e@144.76.238.49:30306",
    "enode://6dd3ac8147fa82e46837ec8c3223d69ac24bcdbab04b036a3705c14f3a02e968f7f1adfcdb002aacec2db46e625c04bf8b5a1f85bb2d40a479b3cc9d45a444af@104.237.131.102:30303",
    "enode://18a551bee469c2e02de660ab01dede06503c986f6b8520cb5a65ad122df88b17b285e3fef09a40a0d44f99e014f8616cf1ebc2e094f96c6e09e2f390f5d34857@47.90.36.129:30303",
    "enode://2521b2616f795f3eb21757b52908978783a5eb8c35850e5934015f713d00bb476370176264b5b678b88e4e14bed4196476627f7e079d67bf0c02622c0fe7d9d7@125.134.78.189:30303",
    "enode://3f5f14647126dc39323447e22cb548369e1377a4e789ad48a6dc6680df1a39a28c46d36e79165fa155fdfaa67337d9703e1f029d5a788306fdef2030669a3bba@50.64.94.28:30303",
    "enode://4fca8ecaf9bd12b805b2b99d2ed6a28e62223707d16c53dd746e2a70f067308b0d8d9a769412f9b323b3410083dfef3eeadd139fd6946535692c1b31467f1080@159.203.78.75:30303",
    "enode://57be9e74b22ff3ea1bd3fedeb2ced310387dd176866793e273c7712305d8e4677f8913c86f93dfa8810e1cdb4177e5f87112db8748199a7771baf8dced63688b@104.41.188.223:30303",
    "enode://94072bbbf1d3e5648afc573bbaf79b14a26ac48380f635fde32782631329263fe7a347251079f9abd3a2678f5bc5e672f8e6aff93a27f0f8f4e0e4f961dac68d@1.226.84.230:50303",
    "enode://98b863da48ab8bef2339a823d552f3619fd8e892425ae40c6812c6f7e4a0afb4f9591b012183e89a63bb01c5085d0e96aa5f0812652335fb0ac946d6aaf15881@118.178.57.121:30303",
    "enode://fba5a07e283d517a2680bcfc7aeb498ac2d246d756556a2ebd5edeb39496491c47a6d27e27f82833b7d7d12defc8de994de04bb58beb72472649f9a323006820@41.135.121.6:30303"
  ).map(new URI(_))

  def startOutgoings_test(nodes: Set[NodeDiscoveryService.Node]) {
    nodes.map(_.uri) foreach startOutgoing_test
  }

  val testUri1 = new URI("enode://83e15509e7803b7e448ec3072ac3e1cf3af505f383271b8a0ab19e8fe70a7511b23329f5371acb916fc930b9d73b4820bcae1c66f55a1854458a822bb1364a9f@127.0.0.1:9076")
  val testUri2 = new URI("enode://a0ad7ca75ce182b6ec45f96e71c45f8b05d1d7683005d896ee68c170929b436775bc62ce430b3b81c122a45cc926229f59feed8dbd283e2a2b9fb8e80172ea7c@vic:9076")
  def startOutgoing_test(uri: URI) {
    log.info(s"uri: $uri")

    import serviceBoard.materializer

    val peer = new OutgoingPeer(Peer.peerId(uri).get, uri)
    val authHandshake = AuthHandshake(serviceBoard.nodeKey, serviceBoard.secureRandom)
    val handshake = new EtcHandshake(serviceBoard.nodeStatus, serviceBoard.blockchain, serviceBoard.storages.appStateStorage, serviceBoard.peerConfig, serviceBoard.forkResolverOpt)
    RLPx.startOutgoing(peer, serviceBoard.messageDecoder, serviceBoard.protocolVersion, authHandshake, handshake)(system)
  }

  def startDiscoveryService() {
    NodeDiscoveryService.start(system, Some("entity"), serviceBoard.discoveryConfig, serviceBoard.storages.knownNodesStorage, serviceBoard.nodeStatus)
    NodeDiscoveryService.startProxy(system, Some("entity"))
  }

  def startKnonwNodesMaganer() {
    KnownNodesService.start(system, Some("entity"), serviceBoard.knownNodesServiceConfig, serviceBoard.storages.knownNodesStorage)
    KnownNodesService.startProxy(system, Some("entity"))
  }

  def startPendingTxService() {
    PendingTransactionsService.start(system, Some("entity"), serviceBoard.txPoolConfig)
    PendingTransactionsService.startProxy(system, Some("entity"))
  }

  def startSyncService() {
    SyncService.start(system, Some("entity"))
    SyncService.startProxy(system, Some("entity"))
  }

  def startIncomingServer() {
    import serviceBoard.materializer

    val authHandshake = AuthHandshake(serviceBoard.nodeKey, serviceBoard.secureRandom)
    val handshake = new EtcHandshake(serviceBoard.nodeStatus, serviceBoard.blockchain, serviceBoard.storages.appStateStorage, serviceBoard.peerConfig, serviceBoard.forkResolverOpt)
    RLPx.startIncoming(
      serviceBoard.networkConfig.Server.listenAddress,
      serviceBoard.messageDecoder,
      serviceBoard.protocolVersion,
      authHandshake, handshake
    )(system)
  }

  def startJsonRpcServer() {
    val jsonRpcHttpServerConfig = KhipuConfig.Network.Rpc

    if (jsonRpcHttpServerConfig.enabled) {

      val web3Service = new Web3Service()

      val netServiceConfig = NetServiceConfig(KhipuConfig.config)
      val netService = new NetService(serviceBoard.nodeStatus, netServiceConfig)

      val blockGenerator = new BlockGenerator(serviceBoard.blockchain, serviceBoard.blockchainConfig, serviceBoard.miningConfig, serviceBoard.ledger, serviceBoard.validators)

      val keyStore: KeyStore.I = new KeyStore(KhipuConfig.keyStoreDir, serviceBoard.secureRandom)

      val filterConfig = FilterConfig(KhipuConfig.config)
      val filterManager = system.actorOf(FilterManager.props(
        serviceBoard.blockchain,
        blockGenerator,
        serviceBoard.storages.appStateStorage,
        keyStore,
        filterConfig,
        serviceBoard.txPoolConfig
      ), "filter-manager")

      val ethService = new EthService(
        serviceBoard.blockchain,
        blockGenerator,
        serviceBoard.storages.appStateStorage,
        serviceBoard.miningConfig,
        serviceBoard.ledger,
        keyStore,
        filterManager,
        filterConfig,
        serviceBoard.blockchainConfig
      )

      val personalService = new PersonalService(
        keyStore,
        serviceBoard.blockchain,
        serviceBoard.storages.appStateStorage,
        serviceBoard.blockchainConfig,
        serviceBoard.txPoolConfig
      )

      val jsonRpcController = new JsonRpcController(web3Service, netService, ethService, personalService, KhipuConfig.Network.Rpc)
      val jsonRpcHttpServer = new JsonRpcHttpServer(jsonRpcController, jsonRpcHttpServerConfig)

      jsonRpcHttpServer.run()
    }
  }
}
class Khipu