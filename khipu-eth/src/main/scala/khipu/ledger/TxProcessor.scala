package khipu.ledger

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.Terminated
import akka.routing.ActorRefRoutee
import akka.routing.RoundRobinRoutingLogic
import akka.routing.Router
import khipu.Hash
import khipu.domain.Block
import khipu.domain.BlockHeader
import khipu.domain.SignedTransaction
import khipu.validators.SignedTransactionValidator
import khipu.validators.Validators
import khipu.vm.EvmConfig

object TxProcessor {
  def props(ledger: Ledger) = Props(classOf[TxProcessor], ledger)

  trait Work
  final case class ExecuteWork(world: BlockWorldState, stx: SignedTransaction, blockHeader: BlockHeader, stxValidator: SignedTransactionValidator, evmCfg: EvmConfig) extends Work
  final case class PreValidateWork(block: Block, validatingBlocks: Map[Hash, Block], validators: Validators) extends Work
}
final class TxProcessor(ledger: Ledger) extends Actor with ActorLogging {
  import TxProcessor._

  private var router = {
    val routees = Vector.fill(1000) {
      val r = context.actorOf(TxProcessWorker.props(ledger))
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  def receive = {
    case w: Work =>
      router.route(w, sender())
    case Terminated(a) =>
      router = router.removeRoutee(a)
      val r = context.actorOf(TxProcessWorker.props(ledger))
      context watch r
      router = router.addRoutee(r)
  }
}

object TxProcessWorker {
  def props(ledger: Ledger) = Props(classOf[TxProcessWorker], ledger).withDispatcher("khipu-txprocess-affinity-pool-dispatcher")
}
final class TxProcessWorker(ledger: Ledger) extends Actor with ActorLogging {

  def receive = {
    case TxProcessor.ExecuteWork(world, stx, blockHeader, stxValidator, evmCfg) =>
      val start = System.currentTimeMillis
      val result = ledger.validateAndExecuteTransaction(stx, blockHeader, stxValidator, evmCfg)(world)
      val elapsed = System.currentTimeMillis - start
      sender() ! (result, elapsed)

    case TxProcessor.PreValidateWork(block, validatingBlocks, validators) =>
      sender() ! ledger.validateBlockBeforeExecution(block, validatingBlocks, validators)
  }
}