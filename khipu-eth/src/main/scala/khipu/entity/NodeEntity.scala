package khipu.entity

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.actor.Timers
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion
import akka.util.ByteString
import khipu.ActiveCheckTick
import khipu.ActiveCheckTickKey
import java.math.BigInteger
import java.time.LocalDateTime
import khipu.Command
import scala.concurrent.duration._

/**
 * Each actor instance occupies about extra ~300 bytes, 10k actors will be ~2.86M
 * 1 millis will be ~286M, 100 millis will be 28G
 *
 * Message entitis keep in memory when refer count > 0, and will exit from memory
 * when refer count == 0 or inactive more than
 */
object NodeEntity {
  def props() = Props(classOf[NodeEntity])

  val typeName: String = "nodeEntity"

  private val extractEntityId: ShardRegion.ExtractEntityId = {
    case cmd: Command => (cmd.id, cmd)
  }

  private val extractShardId: ShardRegion.ExtractShardId = {
    case cmd: Command => (new BigInteger(khipu.hexDecode(cmd.id)).longValue % 1000).toString
  }

  def startSharding(implicit system: ActorSystem) =
    ClusterSharding(system).start(typeName, props(), ClusterShardingSettings(system), extractEntityId, extractShardId)

  def startShardingProxy(implicit system: ActorSystem) =
    ClusterSharding(system).startProxy(typeName, None, extractEntityId, extractShardId)

  private case object ExpiredTickKey
  private case object ExpiredTick

  final case class GetNode(id: String) extends Command

  final case class CreateNode(id: String, node: Array[Byte]) extends Command
  final case class UpdateNode(id: String, node: Array[Byte]) extends Command
  final case class DeleteNode(id: String) extends Command

  final case class Commit(id: String) extends Command
  final case class Rollback(id: String) extends Command
}

final class NodeEntity() extends Actor with Stash with Timers with ActorLogging {
  import NodeEntity._
  import context.dispatcher

  private val inactiveInterval = context.system.settings.config.getInt("khipu.entity.node.inactive")

  private lazy val id = ByteString(khipu.hexDecode(self.path.name)) // hash of this node

  private var node: Option[Array[Byte]] = None
  private var referCount = 0
  private var isDeleted = false

  private var lastActiveTime = LocalDateTime.now()

  timers.startTimerWithFixedDelay(ActiveCheckTickKey, ActiveCheckTick, 5.minutes)

  override def postStop() {
    log.debug(s"${self.path.name} stopped")
    super.postStop()
  }

  def receive = ready

  def ready: Receive = {
    case NodeEntity.GetNode(_) =>
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      // TODO if is deleted
      commander ! node

    case NodeEntity.UpdateNode(hash, node) =>
      log.debug(s"$hash going to update node")
      val commander = sender()
      lastActiveTime = LocalDateTime.now()

      this.node = Some(node)
      referCount += 1
      isDeleted = false

      commander ! true

    case NodeEntity.DeleteNode(hash) =>
      log.debug(s"$hash going to delete node")
      val commander = sender()

      referCount -= 1
      isDeleted = true

      // TODO there maybe a followed message sent to this actor just after stop it, 
      // which, may cause the message to be lost. We have to find correct way
      // to perform delete. Maybe self ! PoisonPill before commander ! true is enough
      //self ! PoisonPill
      commander ! true

    case ActiveCheckTick =>
      if (LocalDateTime.now().minusSeconds(inactiveInterval).isAfter(lastActiveTime)) {
        self ! PoisonPill
      }
  }

}