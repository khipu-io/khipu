package khipu.consensus.ibft.statemachine

import khipu.consensus.ibft.IbftBlockHashing
import khipu.consensus.ibft.IbftExtraData
import khipu.consensus.ibft.RoundTimer
import org.hyperledger.besu.consensus.ibft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.IbftBlockHashing;
import org.hyperledger.besu.consensus.ibft.IbftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.ibft.IbftBlockInterface;
import org.hyperledger.besu.consensus.ibft.IbftExtraData;
import org.hyperledger.besu.consensus.ibft.IbftHelpers;
import org.hyperledger.besu.consensus.ibft.RoundTimer;
import org.hyperledger.besu.consensus.ibft.blockcreation.IbftBlockCreator;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Commit;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.network.IbftMessageTransmitter;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangeCertificate;
import org.hyperledger.besu.crypto.NodeKey;
import org.hyperledger.besu.crypto.SECP256K1.Signature;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MinedBlockObserver;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModuleException;
import org.hyperledger.besu.util.Subscribers;

import java.util.Optional;


class IbftRound(
      roundState: RoundState ,
      blockCreator: IbftBlockCreator ,
      protocolContext: ProtocolContext ,
      blockImporter: BlockImporter ,
       observers: Subscribers[MinedBlockObserver],
      nodeKey: NodeKey ,
      messageFactory: MessageFactory , // used only to create stored local msgs
      transmitter: IbftMessageTransmitter ,
      roundTimer: RoundTimer ) {


  roundTimer.startTimer(getRoundIdentifier());

  def getRoundIdentifier():ConsensusRoundIdentifier = {
    return roundState.getRoundIdentifier();
  }

  def createAndSendProposalMessage(headerTimeStampSeconds:Long ) {
    val block = blockCreator.createBlock(headerTimeStampSeconds);
    val extraData = IbftExtraData.decode(block.getHeader());
    LOG.debug("Creating proposed block. round={}", roundState.getRoundIdentifier());
    LOG.trace(
        "Creating proposed block with extraData={} blockHeader={}", extraData, block.getHeader());
    updateStateWithProposalAndTransmit(block, Optional.empty());
  }

  def startRoundWith(roundChangeArtifacts: RoundChangeArtifacts , headerTimestamp: Long ) {
    val bestBlockFromRoundChange = roundChangeArtifacts.getBlock();

    val roundChangeCertificate = roundChangeArtifacts.getRoundChangeCertificate();
    Block blockToPublish;
    if (!bestBlockFromRoundChange.isPresent()) {
      LOG.debug("Sending proposal with new block. round={}", roundState.getRoundIdentifier());
      blockToPublish = blockCreator.createBlock(headerTimestamp);
    } else {
      LOG.debug(
          "Sending proposal from PreparedCertificate. round={}", roundState.getRoundIdentifier());
      blockToPublish =
          IbftBlockInterface.replaceRoundInBlock(
              bestBlockFromRoundChange.get(),
              getRoundIdentifier().getRoundNumber(),
              IbftBlockHeaderFunctions.forCommittedSeal());
    }

    updateStateWithProposalAndTransmit(blockToPublish, Optional.of(roundChangeCertificate));
  }

  private def updateStateWithProposalAndTransmit(block:Block , roundChangeCertificate: Option[RoundChangeCertificate] ) {
    final Proposal proposal;
    try {
      proposal = messageFactory.createProposal(getRoundIdentifier(), block, roundChangeCertificate);
    } catch {
      case e: SecurityModuleException =>
      LOG.warn("Failed to create a signed Proposal, waiting for next round.", e);
      return;
    }

    transmitter.multicastProposal(
        proposal.getRoundIdentifier(), proposal.getBlock(), proposal.getRoundChangeCertificate());
    updateStateWithProposedBlock(proposal);
  }

  def handleProposalMessage(msg:Proposal ) {
    LOG.debug("Received a proposal message. round={}", roundState.getRoundIdentifier());
    val block = msg.getBlock();

    if (updateStateWithProposedBlock(msg)) {
      LOG.debug("Sending prepare message. round={}", roundState.getRoundIdentifier());
      try {
        val localPrepareMessage = messageFactory.createPrepare(getRoundIdentifier(), block.getHash());
        peerIsPrepared(localPrepareMessage);
        transmitter.multicastPrepare(
            localPrepareMessage.getRoundIdentifier(), localPrepareMessage.getDigest());
      } catch {
        case e: SecurityModuleException =>
        LOG.warn("Failed to create a signed Prepare; {}", e.getMessage());
      }
    }
  }

  def handlePrepareMessage(msg: Prepare ) {
    LOG.debug("Received a prepare message. round={}", roundState.getRoundIdentifier());
    peerIsPrepared(msg);
  }

  handleCommitMessage( msg:Commit ) {
    LOG.debug("Received a commit message. round={}", roundState.getRoundIdentifier());
    peerIsCommitted(msg);
  }

  def constructPreparedRoundArtifacts(): Option[PreparedRoundArtifacts]  {
    return roundState.constructPreparedRoundArtifacts();
  }

  private def updateStateWithProposedBlock(msg:Proposal ): Boolean = {
    val wasPrepared = roundState.isPrepared();
    val wasCommitted = roundState.isCommitted();
    val blockAccepted = roundState.setProposedBlock(msg);

    if (blockAccepted) {
      val block = roundState.getProposedBlock().get();

      final Signature commitSeal;
      try {
        commitSeal = createCommitSeal(block);
      } catch {
        case e:SecurityModuleException =>
        LOG.warn("Failed to construct commit seal; {}", e.getMessage());
        return blockAccepted;
      }

      // There are times handling a proposed block is enough to enter prepared.
      if (wasPrepared != roundState.isPrepared()) {
        LOG.debug("Sending commit message. round={}", roundState.getRoundIdentifier());
        transmitter.multicastCommit(getRoundIdentifier(), block.getHash(), commitSeal);
      }

      // can automatically add _our_ commit message to the roundState
      // cannot create a prepare message here, as it may be _our_ proposal, and thus we cannot also
      // prepare
      try {
        val localCommitMessage =
            messageFactory.createCommit(
                roundState.getRoundIdentifier(), msg.getBlock().getHash(), commitSeal);
        roundState.addCommitMessage(localCommitMessage);
      } catch {
        case e: SecurityModuleException =>
        LOG.warn("Failed to create signed Commit message; {}", e.getMessage());
        return blockAccepted;
      }

      // It is possible sufficient commit seals are now available and the block should be imported
      if (wasCommitted != roundState.isCommitted()) {
        importBlockToChain();
      }
    }

    return blockAccepted;
  }

  private def peerIsPrepared(msg:Prepare ) {
    val wasPrepared = roundState.isPrepared();
    roundState.addPrepareMessage(msg);
    if (wasPrepared != roundState.isPrepared()) {
      LOG.debug("Sending commit message. round={}", roundState.getRoundIdentifier());
      final Block block = roundState.getProposedBlock().get();
      try {
        transmitter.multicastCommit(getRoundIdentifier(), block.getHash(), createCommitSeal(block));
        // Note: the local-node's commit message was added to RoundState on block acceptance
        // and thus does not need to be done again here.
      } catch {
        case e:SecurityModuleException =>
        LOG.warn("Failed to construct a commit seal: {}", e.getMessage());
      }
    }
  }

  private def peerIsCommitted(msg: Commit ) {
    val wasCommitted = roundState.isCommitted();
    roundState.addCommitMessage(msg);
    if (wasCommitted != roundState.isCommitted()) {
      importBlockToChain();
    }
  }

  private def importBlockToChain() {
    val blockToImport = IbftHelpers.createSealedBlock(
            roundState.getProposedBlock().get(), roundState.getCommitSeals());

    final long blockNumber = blockToImport.getHeader().getNumber();
    final IbftExtraData extraData = IbftExtraData.decode(blockToImport.getHeader());
    LOG.info(
        "Importing block to chain. round={}, hash={}",
        getRoundIdentifier(),
        blockToImport.getHash());
    LOG.trace("Importing block with extraData={}", extraData);
    final boolean result =
        blockImporter.importBlock(protocolContext, blockToImport, HeaderValidationMode.FULL);
    if (!result) {
      LOG.error(
          "Failed to import block to chain. block={} extraData={} blockHeader={}",
          blockNumber,
          extraData,
          blockToImport.getHeader());
    } else {
      notifyNewBlockListeners(blockToImport);
    }
  }

  private def createCommitSeal(block: Block ):Signature =  {
    val proposedHeader = block.getHeader();
    val extraData = IbftExtraData.decode(proposedHeader);
    val commitHash = IbftBlockHashing.calculateDataHashForCommittedSeal(proposedHeader, extraData);
    return nodeKey.sign(commitHash);
  }

  private def  notifyNewBlockListeners(block:Block ) {
    observers.forEach(obs -> obs.blockMined(block));
  }
}
