package khipu.transactions

import akka.actor.{ Actor, ActorRef, ActorLogging, Props, ActorSystem, PoisonPill }
import akka.cluster.client.ClusterClientReceptionist
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.cluster.singleton.ClusterSingletonProxy
import akka.cluster.singleton.ClusterSingletonProxySettings
import akka.pattern.ask
import akka.util.Timeout
import khipu.BroadcastTransactions
import khipu.ProcessedTransactions
import khipu.blockchain.sync
import khipu.config.TxPoolConfig
import khipu.domain.SignedTransaction
import khipu.network.PeerEntity
import khipu.network.p2p.messages.CommonMessages.SignedTransactions
import scala.concurrent.duration._

object PendingTransactionsService {
  def props(txPoolConfig: TxPoolConfig): Props =
    Props(classOf[PendingTransactionsService], txPoolConfig)

  val name = "pendingTxService"
  val managerName = "khipuSingleton-" + name
  val managerPath = "/user/" + managerName
  val path = managerPath + "/" + name
  val proxyName = "khipuSingletonProxy-" + name
  val proxyPath = "/user/" + proxyName

  def start(system: ActorSystem, role: Option[String],
            txPoolConfig: TxPoolConfig): ActorRef = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role).withSingletonName(name)
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = props(txPoolConfig),
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

  final case class AddTransactions(signedTransactions: List[SignedTransaction])
  final case class AddOrOverrideTransaction(signedTransaction: SignedTransaction)

  case object GetPendingTransactions
  final case class PendingTransactionsResponse(pendingTransactions: Seq[PendingTransaction])

  final case class RemoveTransactions(signedTransactions: Seq[SignedTransaction])
  final case class PendingTransaction(stx: SignedTransaction, addTimestamp: Long)
}
final class PendingTransactionsService(txPoolConfig: TxPoolConfig) extends Actor with ActorLogging {
  import context.dispatcher
  import PendingTransactionsService._

  /**
   * stores all pending transactions
   */
  private var pendingTransactions: List[PendingTransaction] = Nil

  val mediator = DistributedPubSub(context.system).mediator

  implicit val timeout = Timeout(3.seconds)

  override def receive: Receive = {
    case AddTransactions(signedTransactions) =>
      addTransactions(signedTransactions)

    case AddOrOverrideTransaction(newStx) =>
      addOrOverrideTransaction(newStx)

    case GetPendingTransactions =>
      sender() ! PendingTransactionsResponse(pendingTransactions)

    case RemoveTransactions(signedTransactions) =>
      val stxHashs = signedTransactions.map(_.hash).toSet
      pendingTransactions = pendingTransactions.filterNot(ptx => stxHashs.contains(ptx.stx.hash))
      mediator ! Publish(sync.TxTopic, ProcessedTransactions(signedTransactions))

    case PeerEntity.MessageFromPeer(peerId, SignedTransactions(signedTransactions)) =>
      addTransactions(signedTransactions.toList)
  }

  private def addTransactions(signedTransactions: List[SignedTransaction]) {
    val transactionsToAdd = signedTransactions.filterNot(stx => pendingTransactions.contains(stx.hash))
    if (transactionsToAdd.nonEmpty) {
      pendingTransactions = (transactionsToAdd.map(PendingTransaction(_, System.currentTimeMillis)) ::: pendingTransactions).take(txPoolConfig.txPoolSize)

      broadcastNewTransactions(transactionsToAdd)
    }
  }

  private def addOrOverrideTransaction(newTx: SignedTransaction) {
    val txsWithoutObsoletes = pendingTransactions.filterNot { ptx =>
      ptx.stx.sender == newTx.sender && ptx.stx.tx.nonce == newTx.tx.nonce
    }

    pendingTransactions = (PendingTransaction(newTx, System.currentTimeMillis) :: txsWithoutObsoletes).take(txPoolConfig.txPoolSize)

    broadcastNewTransactions(List(newTx))
  }

  private def broadcastNewTransactions(signedTransactions: Seq[SignedTransaction]) = {
    val ptxHashs = pendingTransactions.map(_.stx.hash).toSet
    val txsToNotify = signedTransactions.filter(tx => ptxHashs.contains(tx.hash)) // signed transactions that are still pending
    if (txsToNotify.nonEmpty) {
      mediator ! Publish(sync.TxTopic, BroadcastTransactions(txsToNotify))
    }
  }
}
