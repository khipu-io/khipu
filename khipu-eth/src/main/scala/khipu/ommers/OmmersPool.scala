package khipu.ommers

import akka.actor.{ Actor, Props }
import khipu.config.MiningConfig
import khipu.domain.{ BlockHeader, Blockchain }

/**
 * The term "ommer" is a gender-neutral alternative to aunt / uncle
 */
object OmmersPool {
  def props(blockchain: Blockchain, miningConfig: MiningConfig): Props =
    Props(classOf[OmmersPool], blockchain, miningConfig)

  final case class AddOmmers(ommers: List[BlockHeader])
  final case class RemoveOmmers(ommers: List[BlockHeader])
  final case class GetOmmers(blockNumber: Long)
  final case class Ommers(headers: List[BlockHeader])
}
class OmmersPool(blockchain: Blockchain, miningConfig: MiningConfig) extends Actor {
  import OmmersPool._

  var ommersPool = List[BlockHeader]()

  val ommerGenerationLimit: Int = 6 //Stated on section 11.1, eq. (143) of the YP
  val ommerSizeLimit: Int = 2

  override def receive: Receive = {
    case AddOmmers(ommers) =>
      ommersPool = (ommers ++ ommersPool).take(miningConfig.ommersPoolSize).distinct

    case RemoveOmmers(ommers) =>
      val toDelete = ommers.map(_.hash).toSet
      ommersPool = ommersPool.filter(b => !toDelete.contains(b.hash))

    case GetOmmers(blockNumber) =>
      val ommers = ommersPool.filter { b =>
        val generationDifference = blockNumber - b.number
        generationDifference > 0 && generationDifference <= ommerGenerationLimit
      }.filter { b =>
        blockchain.getBlockHeaderByHash(b.parentHash).isDefined
      }.take(ommerSizeLimit)
      sender() ! OmmersPool.Ommers(ommers)
  }
}

