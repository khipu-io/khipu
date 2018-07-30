package khipu.jsonrpc

import akka.actor.ActorSystem
import akka.util.Timeout
import khipu.jsonrpc.NetService.NetServiceConfig
import khipu.NodeStatus
import khipu.ServerStatus.{ Listening, NotListening }
import khipu.network.rlpx.PeerManager
import khipu.service.ServiceBoard
import khipu.util.Config
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object NetService {
  final case class VersionRequest()
  final case class VersionResponse(value: String)

  final case class ListeningRequest()
  final case class ListeningResponse(value: Boolean)

  final case class PeerCountRequest()
  final case class PeerCountResponse(value: Int)

  final case class NetServiceConfig(peerManagerTimeout: FiniteDuration)

  object NetServiceConfig {
    def apply(etcClientConfig: com.typesafe.config.Config): NetServiceConfig = {
      val netServiceConfig = etcClientConfig.getConfig("network.rpc.net")
      NetServiceConfig(
        peerManagerTimeout = netServiceConfig.getDuration("peer-manager-timeout").toMillis.millis
      )
    }
  }
}

class NetService(nodeStatus: NodeStatus, config: NetServiceConfig)(implicit system: ActorSystem) {
  import NetService._

  val serviceBoarder = ServiceBoard(system)
  def peerManager = serviceBoarder.peerManage

  def version(req: VersionRequest): ServiceResponse[VersionResponse] = {
    Future.successful(Right(VersionResponse(Config.Network.protocolVersion)))
  }

  def listening(req: ListeningRequest): ServiceResponse[ListeningResponse] = {
    Future.successful {
      Right(
        nodeStatus.serverStatus match {
          case _: Listening => ListeningResponse(true)
          case NotListening => ListeningResponse(false)
        }
      )
    }
  }

  def peerCount(req: PeerCountRequest): ServiceResponse[PeerCountResponse] = {
    import akka.pattern.ask
    implicit val timeout = Timeout(config.peerManagerTimeout)

    (peerManager ? PeerManager.GetPeers).mapTo[PeerManager.Peers].map { peers =>
      Right(PeerCountResponse(peers.handshaked.size))
    }
  }

}
