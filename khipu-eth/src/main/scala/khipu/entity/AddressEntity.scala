package khipu.entity

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Timers
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion
import akka.util.ByteString
import java.time.LocalDateTime
import khipu.ActiveCheckTick
import khipu.ActiveCheckTickKey
import khipu.Command
import khipu.Hash
import khipu.DataWord
import khipu.domain.Account
import khipu.ledger.TrieStorage
import khipu.service.ServiceBoard
import khipu.store.trienode.NodeKeyValueStorage
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

/**
 * Each actor instance occupies about extra ~300 bytes, 10k actors will be ~2.86M
 * 1 millis will be ~286M, 100 millis will be 28G
 *
 * Message entitis keep in memory when refer count > 0, and will exit from memory
 * when refer count == 0 or inactive more than
 */
object AddressEntity {
  def props(nodeStorage: NodeKeyValueStorage) = Props(classOf[AddressEntity], nodeStorage)

  val typeName: String = "addressEntity"

  private val extractEntityId: ShardRegion.ExtractEntityId = {
    case cmd: Command => (cmd.id, cmd)
  }

  private val extractShardId: ShardRegion.ExtractShardId = {
    case cmd: Command => (cmd.id.hashCode % 100).toString
  }

  def startSharding(nodeStorage: NodeKeyValueStorage)(implicit system: ActorSystem) =
    ClusterSharding(system).start(typeName, props(nodeStorage), ClusterShardingSettings(system), extractEntityId, extractShardId)

  def startShardingProxy(implicit system: ActorSystem) =
    ClusterSharding(system).startProxy(typeName, None, extractEntityId, extractShardId)

  private case object ExpiredTickKey
  private case object ExpiredTick

  final case class CreateAccount(id: String, account: Account) extends Command

  final case class GetAccount(id: String) extends Command
  final case class GetCode(id: String) extends Command
  final case class GetStorage(id: String) extends Command

  final case class UpdateAccount(id: String, account: Account) extends Command
  final case class UpdateCode(id: String, code: ByteString) extends Command
  final case class UpdateStorage(id: String, storage: Map[DataWord, DataWord]) extends Command

  final case class Commit(id: String) extends Command
  final case class Rollback(id: String) extends Command
}

final class AddressEntity(nodeStorage: NodeKeyValueStorage) extends Actor with Timers with ActorLogging {
  import AddressEntity._
  import context.dispatcher

  private val inactiveInterval = context.system.settings.config.getInt("khipu.entity.node.inactive")

  private val serviceBoard = ServiceBoard(context.system)
  //def storeService = serviceBoard.storeService
  val blockchain = serviceBoard.blockchain

  private var persistedAccount: Option[Account] = None
  private var persistedCode: Option[ByteString] = None
  private var persistedStorage: Option[Map[DataWord, DataWord]] = None

  private var account: Option[Account] = None
  private var code: Option[ByteString] = None
  private var storage: Option[Map[DataWord, DataWord]] = None

  private var isChanged = false

  private var isPersistent = false

  private var referCount = 0
  private var isSavingOrSaved = false

  private var lastActiveTime = LocalDateTime.now()

  private val loading = Promise[Unit]
  private lazy val load: Future[Unit] = {
    Future(())
    //storeService.selectMessage(longId) map {
    //  case Some((msg, isDurable, referCount)) =>
    //    this.account = msg
    //    this.isPersistent = isDurable
    //    this.referCount = referCount
    //    this.isSavingOrSaved = true
    //    setTTL(msg.ttl)
    //
    //  case None =>
    //} andThen {
    //  case Success(_) =>
    //  case Failure(e) => log.error(e, e.getMessage)
    //}
  }
  private def promiseLoad(): Future[Unit] = {
    if (!loading.isCompleted) {
      loading tryCompleteWith load
    }
    loading.future
  }

  private lazy val longId = self.path.name.toLong

  override def postStop() {
    log.debug(s"$longId stopped")
    super.postStop()
  }

  def receive = ready

  def ready: Receive = {
    case AddressEntity.CreateAccount(_, account) =>
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      this.account = Some(account)
      isSavingOrSaved = false

      loading.trySuccess(())

      commander ! true

    case AddressEntity.GetAccount(_) =>
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      promiseLoad() map { _ =>
        commander ! (account orElse persistedAccount)
      }

    case AddressEntity.GetCode(_) =>
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      account.map(x => x.codeHash).flatMap(blockchain.getEvmcodeByHash).getOrElse(ByteString.empty)

      promiseLoad() map { _ =>
        commander ! (code orElse persistedCode)
      }

    case AddressEntity.GetStorage(_) =>
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      val stateRoot = account.map(_.stateRoot).getOrElse(Account.EMPTY_STATE_ROOT_HASH)

      promiseLoad() map { _ =>
        commander ! (storage orElse persistedStorage)
      }

    case AddressEntity.Commit(_) =>
      // do presist
      account.map { x => persistedAccount = Some(x) }
      account = None

      code.map { x => persistedCode = Some(x) }
      code = None

      storage.map { x => persistedStorage = Some(x) }
      storage = None

    case AddressEntity.Rollback(_) =>
      account = None
      code = None
      storage = None

    case ActiveCheckTick =>
      if (LocalDateTime.now().minusSeconds(inactiveInterval).isAfter(lastActiveTime)) {
        val persist = if (!isSavingOrSaved && persistedAccount != null) {
          isSavingOrSaved = true
          //storeService.insertMessage(longId, account.header, account.body, account.exchange, account.routingKey, isPersistent, referCount, account.ttl)
        } else {
          Future.successful(())
        }

        self ! PoisonPill
      }
  }

}