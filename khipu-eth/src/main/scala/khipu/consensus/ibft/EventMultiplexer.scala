package khipu.consensus.ibft

import khipu.consensus.ibft.ibftevent.BlockTimerExpiry
import khipu.consensus.ibft.ibftevent.IbftEvent
import khipu.consensus.ibft.ibftevent.IbftEvents.Type
import khipu.consensus.ibft.ibftevent.IbftReceivedMessageEvent
import khipu.consensus.ibft.ibftevent.NewChainHead
import khipu.consensus.ibft.ibftevent.RoundExpiry
import khipu.consensus.ibft.statemachine.IbftController

class EventMultiplexer(ibftController: IbftController) {

  def handleIbftEvent(ibftEvent: IbftEvent) {
    try {
      ibftEvent.getType match {
        case Type.MESSAGE =>
          val rxEvent = ibftEvent.asInstanceOf[IbftReceivedMessageEvent]
          ibftController.handleMessageEvent(rxEvent)
        case Type.ROUND_EXPIRY =>
          val roundExpiryEvent = ibftEvent.asInstanceOf[RoundExpiry]
          ibftController.handleRoundExpiry(roundExpiryEvent)
        case Type.NEW_CHAIN_HEAD =>
          val newChainHead = ibftEvent.asInstanceOf[NewChainHead]
          ibftController.handleNewBlockEvent(newChainHead)
        case Type.BLOCK_TIMER_EXPIRY =>
          val blockTimerExpiry = ibftEvent.asInstanceOf[BlockTimerExpiry]
          ibftController.handleBlockTimerExpiry(blockTimerExpiry)
        case _ =>
          throw new RuntimeException("Illegal event in queue.")
      }
    } catch {
      case e: Exception =>
      //LOG.error("State machine threw exception while processing event {" + ibftEvent + "}", e);
    }
  }

}
