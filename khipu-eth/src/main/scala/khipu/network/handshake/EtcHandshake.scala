package khipu.network.handshake

import akka.util.ByteString
import khipu.NodeStatus
import khipu.ServerStatus
import khipu.DataWord
import khipu.config.KhipuConfig
import khipu.domain.Blockchain
import khipu.network.ForkResolver
import khipu.network.p2p.Message
import khipu.network.p2p.MessageSerializable
import khipu.network.p2p.messages.CommonMessages.NewBlock
import khipu.network.p2p.messages.CommonMessages.Status
import khipu.network.p2p.messages.PV62
import khipu.network.p2p.messages.Versions
import khipu.network.p2p.messages.WireProtocol.{ Capability, Disconnect, Hello }
import khipu.network.rlpx.PeerConfiguration
import khipu.storage.AppStateStorage
import scala.concurrent.duration._

/**
 * cmd, core, eth, miner, params, tests: finalize the DAO fork
 * https://github.com/ethereum/go-ethereum/pull/2814
 *
 * This PR is meant to enforce the network split after passing the DAO hard-fork block number. It is done by requesting the DAO fork-block's header immediately after an eth handshake completes. If the DAO challenge isn't completed within 15 seconds, the connection is dropped.
 *
 * To complete the DAO challenge, the remote peer must reply with the requested header (every peer will do so, it's how the eth protocol works). A few things can happen:
 *
 * The returned header's extra-data conforms to the side we're on. Challenge completed.
 * The returned header's extra-data doesn't conform to our side. Challenge failed, peer dropped.
 * No header is returned, this complicates things:
 * We ourselves haven't reached the fork point, assume friendly (no way to challenge)
 * We've already reached the fork point, so we already have our fork block:
 * If peer's TD is smaller than this, we cannot check it's side, assume friendly
 * If peer's TD is higher than this, assume the packet is a reply to something else, wait further
 *
 * [info] 00:30:41 Auth handshake success for OutgoingPeer(enode://17aa022b9173e240b7db7b94a3d3b6dc0add3380c632d3aeb51e92eb25def4a5e464549d09ab08450ecedd297fd365d9ba210367d5e7bec4b5a7fe2ab09a9301@vic:30303) with reminingData 192
 * [info] 00:30:41 Handshake received Hello {
 * [info] p2pVersion: 5
 * [info] clientId: Geth/v1.7.2-unstable-605c2b26/linux-amd64/go1.7.6
 * [info] capabilities: Queue(Capability(eth,63), Capability(eth,62))
 * [info] listenPort: 0
 * [info] nodeId: 17aa022b9173e240b7db7b94a3d3b6dc0add3380c632d3aeb51e92eb25def4a5e464549d09ab08450ecedd297fd365d9ba210367d5e7bec4b5a7fe2ab09a9301
 * [info] } from 10.95.229.100:30303
 * [info] 00:30:41 Encoded khipu.network.p2p.messages.WireProtocol$Hello$HelloEnc in 7 ms, will send to 10.95.229.100:30303
 * [info] 00:30:41 Encoded khipu.network.p2p.messages.CommonMessages$Status$StatusEnc in 1 ms, will send to 10.95.229.100:30303
 * [info] 00:30:41 Handshake received Status {
 * [info] protocolVersion: 63
 * [info] networkId: 1
 * [info] totalDifficulty: 1128882086601077031550
 * [info] bestHash: 5e533383c40786d83b01cabfadf900214fc0199b3455073104ca08461e1223b5
 * [info] genesisHash: d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3
 * [info] } from 10.95.229.100:30303
 * [info] 00:30:41 Handshake received GetBlockHeaders{
 * [info] block: 1920000
 * [info] maxHeaders: 1
 * [info] skip: 0
 * [info] reverse: false
 * [info] }
 * [info]       from 10.95.229.100:30303
 * [info] 00:30:41 Encoded khipu.network.p2p.messages.PV62$GetBlockHeaders$GetBlockHeadersEnc in 1 ms, will send to 10.95.229.100:30303
 * [info] 00:30:41 Encoded khipu.network.p2p.messages.PV62$BlockHeaders$BlockHeadersEnc in 2 ms, will send to 10.95.229.100:30303
 * [info] 00:30:41 Handshake received BlockHeaders(Queue(BlockHeader {
 * [info] parentHash: a218e2c611f21232d857e3c8cecdcdf1f65f25a4477f98f6f47e4063807f2308
 * [info] ommersHash: 1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347
 * [info] beneficiary: bcdfc35b86bedf72f0cda046a3c16829a2ef41d1
 * [info] stateRoot: c5e389416116e3696cce82ec4533cce33efccb24ce245ae9546a4b8f0d5e9a75
 * [info] transactionsRoot: 7701df8e07169452554d14aadd7bfa256d4a1d0355c1d174ab373e3e2d0a3743
 * [info] receiptsRoot: 26cf9d9422e9dd95aedc7914db690b92bab6902f5221d62694a2fa5d065f534b
 * [info] logsBloom: 00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
 * [info] difficulty: 62413376722602,
 * [info] number: 1920000,
 * [info] gasLimit: 4712384,
 * [info] gasUsed: 84000,
 * [info] unixTimestamp: 1469020840,
 * [info] extraData: 64616f2d686172642d666f726b
 * [info] mixHash: 5b5acbf4bf305f948bd7be176047b20623e1417f75597341a059729165b92397
 * [info] nonce: bede87201de42426
 * [info] })) from 10.95.229.100:30303
 * [info] 00:30:41 HandshakeFailure to 10.95.229.100:30303 reason 3 when received khipu.network.p2p.messages.PV62$BlockHeaders
 * [info] 00:30:41 Encoded khipu.network.p2p.messages.WireProtocol$Disconnect$DisconnectEnc in 2 ms, will send to 10.95.229.100:30303
 * [info] 00:30:41 Handshake received Disconnect(Useless peer) from 10.95.229.100:30303
 *
 *
 */
object EtcHandshake {
  val P2PVersion = 4

  sealed trait Handshaking
  final case class HandshakeFailure(reason: Int) extends Handshaking
  final case class HandshakeSuccess(result: PeerInfo) extends Handshaking
  final case class NextMessage(messageToSend: MessageSerializable, timeout: FiniteDuration) extends Handshaking
  final case class Noop(info: String) extends Handshaking

  final case class PeerInfo(
      remoteStatus:    Status,
      totalDifficulty: DataWord,
      forkAccepted:    Boolean,
      maxBlockNumber:  Long
  ) {
    def withTotalDifficulty(totalDifficulty: DataWord): PeerInfo = copy(totalDifficulty = totalDifficulty)
    def withForkAccepted(forkAccepted: Boolean): PeerInfo = copy(forkAccepted = forkAccepted)
    def withMaxBlockNumber(maxBlockNumber: Long): PeerInfo = copy(maxBlockNumber = maxBlockNumber)

    /**
     * Processes the message and updates the total difficulty of the peer
     *
     * @param message to be processed
     * @param initialPeerInfo from before the message was processed
     * @return new peer info with the total difficulty updated
     */
    def maybeUpdateTotalDifficulty(message: Message): Option[PeerInfo] = message match {
      case NewBlock(_, totalDifficulty) if totalDifficulty.compareTo(this.totalDifficulty) != 0 => Some(withTotalDifficulty(totalDifficulty))
      case _ => None
    }

    /**
     * Processes the message and updates if the fork block was accepted from the peer
     *
     * @param message to be processed
     * @param initialPeerInfo from before the message was processed
     * @return new peer info with the fork block accepted value updated
     */
    def maybeUpdateForkAccepted(message: Message, forkResolver: Option[ForkResolver]): Option[PeerInfo] = message match {
      case PV62.BlockHeaders(blockHeaders) =>
        forkResolver flatMap { fResolver =>
          blockHeaders.find(_.number == fResolver.forkBlockNumber) flatMap { forkBlockHeader =>
            val newFork = fResolver.recognizeFork(forkBlockHeader)
            //log.debug("Received fork block header with fork: {}", newFork)
            if (this.forkAccepted != fResolver.isAccepted(newFork)) {
              Some(this.withForkAccepted(!this.forkAccepted))
            } else {
              None
            }
          }
        }
      case _ => None
    }

    /**
     * Processes the message and updates the max block number from the peer
     *
     * @param message to be processed
     * @param initialPeerInfo from before the message was processed
     * @return new peer info with the max block number updated
     */
    def maybeUpdateMaxBlock(message: Message): Option[PeerInfo] = message match {
      case m: PV62.BlockHeaders   => mayBeWithMaxBlockNumber(m.headers.map(_.number))
      case m: NewBlock            => mayBeWithMaxBlockNumber(Seq(m.block.header.number))
      case m: PV62.NewBlockHashes => mayBeWithMaxBlockNumber(m.hashes.map(_.number))
      case _                      => None
    }

    private def mayBeWithMaxBlockNumber(ns: Seq[Long]) = {
      val maxBlockNumber = ns.fold(0: Long) { case (a, b) => if (a > b) a else b }
      if (maxBlockNumber > this.maxBlockNumber) Some(this.withMaxBlockNumber(maxBlockNumber)) else None
    }
  }

}
final class EtcHandshake(
    nodeStatus:      NodeStatus,
    blockchain:      Blockchain,
    appStateStorage: AppStateStorage,
    peerConfig:      PeerConfiguration,
    forkResolverOpt: Option[ForkResolver]
) {
  import EtcHandshake._

  private var remoteStatus: Status = _

  def respondTo(message: Message): Handshaking = {
    message match {
      case hello: Hello                     => respondToHello(hello)
      case status: Status                   => respondToStatus(status)
      case headers: PV62.BlockHeaders       => respondToBlockHeaders(headers)
      case getHeaders: PV62.GetBlockHeaders => respondToGetBlockHeaders(getHeaders)
      case _                                => Noop("")
    }
  }

  def theHelloMessage: Hello = {
    val listenPort = nodeStatus.serverStatus match {
      case ServerStatus.Listening(address) => address.getPort
      case ServerStatus.NotListening       => 0
    }
    Hello(
      p2pVersion = P2PVersion,
      clientId = KhipuConfig.clientId,
      capabilities = List(
        Capability("eth", Versions.PV63.toByte)
      ),
      listenPort = listenPort,
      nodeId = ByteString(nodeStatus.nodeId)
    )
  }

  /**
   * For any eth/etc node to join a network there are 2 requirement one is to
   * have the same genesis block and other is to have the same networkid. Once
   * these requirements are satisfied, to join a network you have to know the
   * enodeid of the nodes you want to connect to.
   *
   * In case of open Ethereum networks like mainnet or testnet, you have a set
   * of bootnodes hardcoded into your client where you can connect and discover
   * other peers in the network. These bootnodes actually have a list of nodes
   * that are connected to it in the last 24 hrs and they give out that list to
   * you and then you can connect to those nodes. This is how you connect to
   * other nodes in a Ethereum public network.
   *
   * So if you want to create a new open network then create a genesis file,
   * select some networkid, start some bootnodes and share these details about
   * genesis file, networkid and list of bootnodes with the participants of
   * network and then they will be able to join your network.
   *
   * Current network ids:
   * 0: Olympic, Ethereum public pre-release testnet
   * 1: Frontier, Homestead, Metropolis, the Ethereum public main network
   * 1: Classic, the (un)forked public Ethereum Classic main network, chain ID 61
   * 1: Expanse, an alternative Ethereum implementation, chain ID 2
   * 2: Morden, the public Ethereum testnet, now Ethereum Classic testnet
   * 3: Ropsten, the public cross-client Ethereum testnet
   * 4: Rinkeby, the public Geth Ethereum testnet
   * 42: Kovan, the public Parity Ethereum testnet
   * 7762959: Musicoin, the music blockchain
   */
  def theStatusMessage: Status = {
    val bestBlockHeader = getBestBlockHeader()
    Status(
      protocolVersion = Versions.PV63,
      networkId = peerConfig.networkId,
      totalDifficulty = bestBlockHeader.difficulty,
      bestHash = bestBlockHeader.hash,
      genesisHash = blockchain.genesisHeader.hash
    )
  }

  def theGetBlockHeadersMessage(forkResolver: ForkResolver) = {
    PV62.GetBlockHeaders(Left(forkResolver.forkBlockNumber), maxHeaders = 1, skip = 0, reverse = false)
  }

  def respondToHello(message: Hello): Handshaking = {
    if (message.capabilities.contains(Capability("eth", Versions.PV63.toByte)))
      NextMessage(theStatusMessage, peerConfig.waitForStatusTimeout)
    else {
      //log.debug("Connected peer does not support eth {} protocol. Disconnecting.", Versions.PV63.toByte)
      HandshakeFailure(Disconnect.Reasons.IncompatibleP2pProtocolVersion)
    }
  }

  def respondToStatus(message: Status): Handshaking = {
    this.remoteStatus = message
    forkResolverOpt match {
      case Some(forkResolver) =>
        // will go to respondBlockheaders
        NextMessage(theGetBlockHeadersMessage(forkResolver), peerConfig.waitForChainCheckTimeout)
      case None =>
        HandshakeSuccess(PeerInfo(message, message.totalDifficulty, true, 0))
    }
  }

  def respondToBlockHeaders(message: PV62.BlockHeaders): Handshaking = {
    forkResolverOpt match {
      case Some(forkResolver) =>
        message.headers.find(_.number == forkResolver.forkBlockNumber) match {
          case Some(forkBlockHeader) =>
            val fork = forkResolver.recognizeFork(forkBlockHeader)
            //log.debug(s"Peer is running the $fork")

            if (forkResolver.isAccepted(fork)) {
              //log.debug("Fork is accepted")
              val peerInfo = PeerInfo(remoteStatus, remoteStatus.totalDifficulty, true, forkBlockHeader.number)
              HandshakeSuccess(peerInfo)
            } else {
              //log.debug("Fork is not accepted")
              HandshakeFailure(Disconnect.Reasons.UselessPeer)
            }

          case None =>
            //log.debug("Peer did not respond with fork block header")
            HandshakeSuccess(PeerInfo(remoteStatus, remoteStatus.totalDifficulty, false, 0))
        }
      case None =>
        // should not be here @see respondStatus
        Noop("")
    }
  }

  def respondToGetBlockHeaders(message: PV62.GetBlockHeaders) = {
    forkResolverOpt match {
      case Some(forkResolver) =>
        message match {
          case PV62.GetBlockHeaders(Left(number), nHeaders, _, _) =>
            //log.debug("Received request for fork block")
            if (nHeaders == 1 && number == forkResolver.forkBlockNumber) {
              blockchain.getBlockHeaderByNumber(number) match {
                case Some(header) =>
                  NextMessage(PV62.BlockHeaders(Seq(header)), 10.seconds)
                case None =>
                  NextMessage(PV62.BlockHeaders(Nil), 10.seconds)
              }
            } else {
              //log.debug(s"============== number($number) != ${forkResolver.forkBlockNumber} or numberHeaders($numHeaders) != 1")
              NextMessage(PV62.BlockHeaders(Nil), 10.seconds)
            }

          case PV62.GetBlockHeaders(Right(hash), nHeaders, _, _) =>
            if (nHeaders == 1) {
              blockchain.getBlockHeaderByHash(hash) match {
                case Some(header) if header.number == forkResolver.forkBlockNumber =>
                  NextMessage(PV62.BlockHeaders(Seq(header)), 10.seconds)
                case _ =>
                  NextMessage(PV62.BlockHeaders(Nil), 10.seconds)
              }
            } else {
              NextMessage(PV62.BlockHeaders(Nil), 10.seconds)
            }

          case _ =>
            //log.debug(s"============== None response to $message")
            NextMessage(PV62.BlockHeaders(Nil), 10.seconds)
        }

      case None =>
        // should not be here @see respondStatus
        Noop("")
    }
  }

  private def getBestBlockHeader() = {
    val bestBlockNumber = blockchain.storages.bestBlockNumber
    blockchain.getBlockHeaderByNumber(bestBlockNumber).getOrElse(blockchain.genesisHeader)
  }
}
