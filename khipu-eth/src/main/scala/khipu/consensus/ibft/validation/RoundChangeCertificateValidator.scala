package khipu.consensus.ibft.validation

import khipu.consensus.ibft.IbftBlockInterface
import khipu.consensus.ibft.IbftHelpers
import org.hyperledger.besu.consensus.ibft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.IbftBlockHeaderFunctions;
import org.hyperledger.besu.consensus.ibft.IbftBlockInterface;
import org.hyperledger.besu.consensus.ibft.IbftHelpers;
import org.hyperledger.besu.consensus.ibft.payload.PreparedCertificate;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangeCertificate;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.ibft.payload.SignedData;
import org.hyperledger.besu.consensus.ibft.validation.RoundChangePayloadValidator.MessageValidatorForHeightFactory;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.Block;

import java.util.Collection;
import java.util.Optional;

class RoundChangeCertificateValidator(
    validators:              Collection[Address],
    messageValidatorFactory: MessageValidatorForHeightFactory,
    chainHeight:             Long
) {

  private val quorum = IbftHelpers.calculateRequiredValidatorQuorum(validators.size());

  def validateRoundChangeMessagesAndEnsureTargetRoundMatchesRoot(
    expectedRound: ConsensusRoundIdentifier, roundChangeCert: RoundChangeCertificate
  ): Boolean = {

    val roundChangeMsgs = roundChangeCert.getRoundChangePayloads();

    if (hasDuplicateAuthors(roundChangeMsgs)) {
      return false;
    }

    if (roundChangeMsgs.size() < quorum) {
      //LOG.info("Invalid RoundChangeCertificate, insufficient RoundChange messages.");
      return false;
    }

    if (!roundChangeCert.getRoundChangePayloads().stream()
      .allMatch(p -> p.getPayload().getRoundIdentifier().equals(expectedRound))) {
      //LOG.info(
      //    "Invalid RoundChangeCertificate, not all embedded RoundChange messages have a "
      //        + "matching target round.");
      return false;
    }

    val roundChangeValidator = new RoundChangePayloadValidator(
      messageValidatorFactory,
      validators,
      IbftHelpers.prepareMessageCountForQuorum(quorum),
      chainHeight
    );

    if (!roundChangeCert.getRoundChangePayloads().stream()
      .allMatch(roundChangeValidator :: validateRoundChange)) {
      //LOG.info("Invalid RoundChangeCertificate, embedded RoundChange message failed validation.");
      return false;
    }

    return true;
  }

  private def hasDuplicateAuthors(roundChangeMsgs: Collection[SignedData[RoundChangePayload]]): Boolean = {
    val distinctAuthorCount = roundChangeMsgs.stream().map(SignedData :: getAuthor).distinct().count();

    if (distinctAuthorCount != roundChangeMsgs.size()) {
      //LOG.info("Invalid RoundChangeCertificate, multiple RoundChanges from the same author.");
      return true;
    }
    return false;
  }

  def validateProposalMessageMatchesLatestPrepareCertificate(roundChangeCert: RoundChangeCertificate, proposedBlock: Block): Boolean = {

    val roundChangePayloads = roundChangeCert.getRoundChangePayloads();

    val latestPreparedCertificate = IbftHelpers.findLatestPreparedCertificate(roundChangePayloads);

    if (!latestPreparedCertificate.isDefined) {
      //LOG.debug(
      //    "No round change messages have a preparedCertificate, any valid block may be proposed.");
      return true;
    }

    // Need to check that if we substitute the LatestPrepareCert round number into the supplied
    // block that we get the SAME hash as PreparedCert.
    val currentBlockWithOldRound = IbftBlockInterface.replaceRoundInBlock(
      proposedBlock,
      latestPreparedCertificate
        .get()
        .getProposalPayload()
        .getPayload()
        .getRoundIdentifier()
        .getRoundNumber(),
      IbftBlockHeaderFunctions.forCommittedSeal()
    );

    if (!currentBlockWithOldRound
      .getHash()
      .equals(latestPreparedCertificate.get().getProposalPayload().getPayload().getDigest())) {
      //LOG.info(
      //    "Invalid RoundChangeCertificate, block in latest RoundChange does not match proposed block.");
      return false;
    }

    return true;
  }
}
