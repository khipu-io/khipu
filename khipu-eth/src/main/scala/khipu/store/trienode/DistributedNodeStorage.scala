package khipu.store.trienode

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.cluster.sharding.ClusterSharding
import akka.util.Timeout
import khipu.Hash
import khipu.entity.NodeEntity
import scala.concurrent.Await
import scala.concurrent.duration._

final class DistributedNodeStorage(source: NodeStorage)(implicit system: ActorSystem) extends NodeKeyValueStorage {
  type This = DistributedNodeStorage

  import system.dispatcher
  implicit val timeout: Timeout = 60.seconds

  private def nodeSharding = ClusterSharding(system).shardRegion(NodeEntity.typeName)

  def tableName = ""
  def count = -1

  // TODO return Future
  override def get(key: Hash): Option[Array[Byte]] = {
    val f = (nodeSharding ? NodeEntity.GetNode(key.hexString)).mapTo[Option[Array[Byte]]]
    Await.result(f, timeout.duration) match {
      case None =>
        source.get(key) match {
          case some @ Some(value) =>
            nodeSharding ! NodeEntity.UpdateNode(key.hexString, value)
            //val f = (nodeSharding ? NodeEntity.UpdateNode(key.hexString, value)).mapTo[Boolean]
            //Await.result(f, timeout.duration)
            some
          case None => None
        }
      case some => some
    }
  }

  // TODO return Future
  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): DistributedNodeStorage = {
    // TODO pruning mode to delete from source
    source.update(Nil, toUpsert)

    val fs1 = toRemove map { key =>
      nodeSharding ! NodeEntity.DeleteNode(key.hexString)
      //(nodeSharding ? NodeEntity.DeleteNode(key.hexString)).mapTo[Boolean]
    }
    val fs2 = toUpsert map {
      case (key, value) =>
        nodeSharding ! NodeEntity.UpdateNode(key.hexString, value)
      //(nodeSharding ? NodeEntity.UpdateNode(key.hexString, value)).mapTo[Boolean]
    }
    //val fs = Future.sequence(fs1 ++ fs2)
    //Await.result(fs, timeout.duration)
    this
  }
}