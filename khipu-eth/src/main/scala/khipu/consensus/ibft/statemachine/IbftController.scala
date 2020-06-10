package khipu.consensus.ibft.statemachine

import khipu.consensus.ibft.ConsensusRoundIdentifier
import khipu.consensus.ibft.ConsensusRoundIdentifier
import khipu.consensus.ibft.ibftevent.BlockTimerExpiry
import khipu.consensus.ibft.ibftevent.IbftReceivedMessageEvent
import khipu.consensus.ibft.ibftevent.NewChainHead
import khipu.consensus.ibft.ibftevent.RoundExpiry
import khipu.consensus.ibft.messagewrappers.IbftMessage
import khipu.consensus.ibft.payload.Authored
import khipu.domain.BlockHeader
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class IbftController(
    blockchain:                    Blockchain,
    ibftFinalState:                IbftFinalState,
    ibftBlockHeightManagerFactory: IbftBlockHeightManagerFactory,
    gossiper:                      Gossiper,
    duplicateMessageTracker:       MessageTracker,
    futureMessageBuffer:           FutureMessageBuffer,
    sychronizerUpdater:            SynchronizerUpdater
) {

  private var currentHeightManager: BlockHeightManager = _

  private val started = new AtomicBoolean(false);

  def start() {
    if (started.compareAndSet(false, true)) {
      startNewHeightManager(blockchain.getChainHeadHeader());
    }
  }

  def handleMessageEvent(msg: IbftReceivedMessageEvent) {
    val data = msg.getMessage().getData();
    if (!duplicateMessageTracker.hasSeenMessage(data)) {
      duplicateMessageTracker.addSeenMessage(data);
      handleMessage(msg.getMessage());
    } else {
      //LOG.trace("Discarded duplicate message");
    }
  }

  private def handleMessage(message: Message) {
    val messageData = message.getData();
    messageData.getCode() {
      case IbftV2.PROPOSAL =>
        consumeMessage(
          message,
          ProposalMessageData.fromMessageData(messageData).decode(),
          currentHeightManager :: handleProposalPayload
        );

      case IbftV2.PREPARE =>
        consumeMessage(
          message,
          PrepareMessageData.fromMessageData(messageData).decode(),
          currentHeightManager :: handlePreparePayload
        );

      case IbftV2.COMMIT =>
        consumeMessage(
          message,
          CommitMessageData.fromMessageData(messageData).decode(),
          currentHeightManager :: handleCommitPayload
        );

      case IbftV2.ROUND_CHANGE =>
        consumeMessage(
          message,
          RoundChangeMessageData.fromMessageData(messageData).decode(),
          currentHeightManager :: handleRoundChangePayload
        );

      case _ =>
        throw new IllegalArgumentException(
          String.format(
            "Received message with messageCode=%d does not conform to any recognised IBFT message structure",
            message.getData().getCode()
          )
        );
    }
  }

  private def consumeMessage[P <: IbftMessage[_]](
    message: Message, ibftMessage: P, handleMessage: Consumer[P]
  ) {
    //LOG.trace("Received IBFT {} message", ibftMessage.getClass().getSimpleName());
    if (processMessage(ibftMessage, message)) {
      gossiper.send(message);
      handleMessage.accept(ibftMessage);
    }
  }

  def handleNewBlockEvent(newChainHead: NewChainHead) {
    val newBlockHeader = newChainHead.getNewChainHeadHeader();
    val currentMiningParent = currentHeightManager.getParentBlockHeader();
    //LOG.debug(
    //    "New chain head detected (block number={})," + " currently mining on top of {}.",
    //    newBlockHeader.getNumber(),
    //    currentMiningParent.getNumber());
    if (newBlockHeader.getNumber() < currentMiningParent.getNumber()) {
      //  LOG.trace(
      //      "Discarding NewChainHead event, was for previous block height. chainHeight={} eventHeight={}",
      //      currentMiningParent.getNumber(),
      //      newBlockHeader.getNumber());
      return ;
    }

    if (newBlockHeader.getNumber() == currentMiningParent.getNumber()) {
      if (newBlockHeader.getHash().equals(currentMiningParent.getHash())) {
        //LOG.trace(
        //    "Discarding duplicate NewChainHead event. chainHeight={} newBlockHash={} parentBlockHash={}",
        //    newBlockHeader.getNumber(),
        //    newBlockHeader.getHash(),
        //    currentMiningParent.getHash());
      } else {
        //LOG.error(
        //    "Subsequent NewChainHead event at same block height indicates chain fork. chainHeight={}",
        //    currentMiningParent.getNumber());
      }
      return ;
    }
    startNewHeightManager(newBlockHeader);
  }

  def handleBlockTimerExpiry(blockTimerExpiry: BlockTimerExpiry) {
    val roundIndentifier = blockTimerExpiry.getRoundIndentifier();
    if (isMsgForCurrentHeight(roundIndentifier)) {
      currentHeightManager.handleBlockTimerExpiry(roundIndentifier);
    } else {
      //LOG.trace(
      //    "Block timer event discarded as it is not for current block height chainHeight={} eventHeight={}",
      //    currentHeightManager.getChainHeight(),
      //    roundIndentifier.getSequenceNumber());
    }
  }

  def handleRoundExpiry(roundExpiry: RoundExpiry) {
    if (isMsgForCurrentHeight(roundExpiry.getView())) {
      currentHeightManager.roundExpired(roundExpiry);
    } else {
      //LOG.trace(
      //    "Round expiry event discarded as it is not for current block height chainHeight={} eventHeight={}",
      //    currentHeightManager.getChainHeight(),
      //    roundExpiry.getView().getSequenceNumber());
    }
  }

  private def startNewHeightManager(parentHeader: BlockHeader) {
    currentHeightManager = ibftBlockHeightManagerFactory.create(parentHeader);
    val newChainHeight = currentHeightManager.getChainHeight();
    futureMessageBuffer.retrieveMessagesForHeight(newChainHeight).forEach(this :: handleMessage);
  }

  private def processMessage(msg: IbftMessage[_], rawMsg: Message): Boolean = {
    val msgRoundIdentifier = msg.getRoundIdentifier();
    if (isMsgForCurrentHeight(msgRoundIdentifier)) {
      return isMsgFromKnownValidator(msg) && ibftFinalState.isLocalNodeValidator();
    } else if (isMsgForFutureChainHeight(msgRoundIdentifier)) {
      //LOG.trace("Received message for future block height round={}", msgRoundIdentifier);
      futureMessageBuffer.addMessage(msgRoundIdentifier.getSequenceNumber(), rawMsg);
      // Notify the synchronizer the transmitting peer must have the parent block to the received
      // message's target height.
      sychronizerUpdater.updatePeerChainState(msgRoundIdentifier.getSequenceNumber() - 1L, rawMsg.getConnection());
    } else {
      //LOG.trace(
      //    "IBFT message discarded as it is from a previous block height messageType={} chainHeight={} eventHeight={}",
      //    msg.getMessageType(),
      //    currentHeightManager.getChainHeight(),
      //    msgRoundIdentifier.getSequenceNumber());
    }
    return false;
  }

  private def isMsgFromKnownValidator(msg: Authored): Boolean = {
    ibftFinalState.getValidators().contains(msg.getAuthor());
  }

  private def isMsgForCurrentHeight(roundIdentifier: ConsensusRoundIdentifier): Boolean = {
    roundIdentifier.sequence == currentHeightManager.getChainHeight();
  }

  private def isMsgForFutureChainHeight(roundIdentifier: ConsensusRoundIdentifier): Boolean = {
    roundIdentifier.sequence > currentHeightManager.getChainHeight();
  }
}
