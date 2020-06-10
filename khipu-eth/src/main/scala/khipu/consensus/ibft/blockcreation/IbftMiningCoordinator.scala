package khipu.consensus.ibft.blockcreation

import org.hyperledger.besu.consensus.ibft.IbftEventQueue;
import org.hyperledger.besu.consensus.ibft.IbftExecutors;
import org.hyperledger.besu.consensus.ibft.IbftProcessor;
import org.hyperledger.besu.consensus.ibft.ibftevent.NewChainHead;
import org.hyperledger.besu.consensus.ibft.statemachine.IbftController;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Wei;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import khipu.domain.Address
import khipu.domain.BlockHeader

object IbftMiningCoordinator {
  private trait State
  private object State {
    case object IDLE extends State
    case object RUNNING extends State
    case object STOPPED extends State
  }

}
class IbftMiningCoordinator(
    ibftExecutors:       IbftExecutors,
    controller:          IbftController,
    ibftProcessor:       IbftProcessor,
    blockCreatorFactory: IbftBlockCreatorFactory,
    blockchain:          Blockchain,
    eventQueue:          IbftEventQueue
) extends MiningCoordinator with BlockAddedObserver {
  import IbftMiningCoordinator._

  private var blockAddedObserverId: Long = _
  private val state: AtomicReference[State] = new AtomicReference[State](State.IDLE)

  override def start() {
    if (state.compareAndSet(State.IDLE, State.RUNNING)) {
      ibftExecutors.start();
      blockAddedObserverId = blockchain.observeBlockAdded(this);
      controller.start();
      ibftExecutors.executeIbftProcessor(ibftProcessor);
    }
  }

  override def stop() {
    if (state.compareAndSet(State.RUNNING, State.STOPPED)) {
      blockchain.removeObserver(blockAddedObserverId);
      ibftProcessor.stop();
      // Make sure the processor has stopped before shutting down the executors
      try {
        ibftProcessor.awaitStop();
      } catch {
        case e: InterruptedException =>
          //LOG.debug("Interrupted while waiting for IbftProcessor to stop.", e);
          Thread.currentThread().interrupt();
      }
      ibftExecutors.stop();
    }
  }

  @throws(classOf[InterruptedException])
  override def awaitStop() {
    ibftExecutors.awaitStop();
  }

  override def enable() = true;

  override def disable() = false;

  override def isMining() = true;

  override def getMinTransactionGasPrice(): Wei = blockCreatorFactory.getMinTransactionGasPrice();

  override def setExtraData(extraData: ByteString) {
    blockCreatorFactory.setExtraData(extraData);
  }

  override def getCoinbase(): Option[Address] = {
    Option(blockCreatorFactory.getLocalAddress());
  }

  override def createBlock(
    parentHeader: BlockHeader,
    transactions: List[Transaction],
    ommers:       List[BlockHeader]
  ): Option[Block] = {
    // One-off block creation has not been implemented
    None
  }

  override def onBlockAdded(event: BlockAddedEvent) {
    if (event.isNewCanonicalHead()) {
      //LOG.trace("New canonical head detected");
      eventQueue.add(new NewChainHead(event.getBlock().getHeader()));
    }
  }
}
