package khipu.network.rlpx

import akka.actor.{ Actor, ActorLogging, Props, Scheduler, ActorSystem, ActorRef, PoisonPill }
import akka.cluster.client.ClusterClientReceptionist
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.cluster.singleton.ClusterSingletonProxy
import akka.cluster.singleton.ClusterSingletonProxySettings
import com.typesafe.config.Config
import java.net.URI
import khipu.storage.KnownNodesStorage
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object KnownNodesService {
  def props(config: KnownNodesServiceConfig, knownNodesStorage: KnownNodesStorage): Props =
    Props(classOf[KnownNodesService], config, knownNodesStorage)

  val name = "knownNodesService"
  val managerName = "khipuSingleton-" + name
  val managerPath = "/user/" + managerName
  val path = managerPath + "/" + name
  val proxyName = "khipuSingletonProxy-" + name
  val proxyPath = "/user/" + proxyName

  def start(system: ActorSystem, role: Option[String],
            config:            KnownNodesServiceConfig,
            knownNodesStorage: KnownNodesStorage): ActorRef = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role).withSingletonName(name)
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(config, knownNodesStorage),
        terminationMessage = PoisonPill,
        settings = settings
      ), name = managerName
    )
  }

  def startProxy(system: ActorSystem, role: Option[String]): ActorRef = {
    val settings = ClusterSingletonProxySettings(system).withRole(role).withSingletonName(name)
    val proxy = system.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = managerPath,
        settings = settings
      ), name = proxyName
    )
    ClusterClientReceptionist(system).registerService(proxy)
    proxy
  }

  def proxy(system: ActorSystem) = system.actorSelection(proxyPath)

  final case class AddKnownNode(uri: URI)
  final case class RemoveKnownNode(uri: URI)
  final case class KnownNodes(nodes: Set[URI])
  case object GetKnownNodes

  private case object PersistChanges

  object KnownNodesServiceConfig {
    def apply(etcClientConfig: Config): KnownNodesServiceConfig = {
      val knownNodesServiceConfig = etcClientConfig.getConfig("network.known-nodes")
      KnownNodesServiceConfig(
        persistInterval = knownNodesServiceConfig.getDuration("persist-interval").toMillis.millis,
        maxPersistedNodes = knownNodesServiceConfig.getInt("max-persisted-nodes")
      )
    }
  }
  final case class KnownNodesServiceConfig(persistInterval: FiniteDuration, maxPersistedNodes: Int)
}
class KnownNodesService(
  config:               KnownNodesService.KnownNodesServiceConfig,
  knownNodesStorage:    KnownNodesStorage,
  externalSchedulerOpt: Option[Scheduler]                         = None
)
    extends Actor with ActorLogging {

  import KnownNodesService._

  private def scheduler = externalSchedulerOpt getOrElse context.system.scheduler

  private var knownNodes: Set[URI] = knownNodesStorage.getKnownNodes
  private var toAdd: Set[URI] = Set.empty
  private var toRemove: Set[URI] = Set.empty

  scheduler.schedule(config.persistInterval, config.persistInterval, self, PersistChanges)

  override def receive: Receive = {
    case AddKnownNode(uri) =>
      if (!knownNodes.contains(uri)) {
        knownNodes += uri
        toAdd += uri
        toRemove -= uri
      }

    case RemoveKnownNode(uri) =>
      if (knownNodes.contains(uri)) {
        knownNodes -= uri
        toAdd -= uri
        toRemove += uri
      }

    case GetKnownNodes =>
      sender() ! KnownNodes(knownNodes)

    case PersistChanges =>
      persistChanges()
  }

  private def persistChanges(): Unit = {
    log.debug(s"Persisting ${knownNodes.size} known nodes.")
    if (knownNodes.size > config.maxPersistedNodes) {
      val toAbandon = knownNodes.take(knownNodes.size - config.maxPersistedNodes)
      toRemove ++= toAbandon
      toAdd --= toAbandon
    }
    if (toAdd.nonEmpty || toRemove.nonEmpty) {
      knownNodesStorage.updateKnownNodes(
        toAdd = toAdd,
        toRemove = toRemove
      )
      toAdd = Set.empty
      toRemove = Set.empty
    }
  }

}

