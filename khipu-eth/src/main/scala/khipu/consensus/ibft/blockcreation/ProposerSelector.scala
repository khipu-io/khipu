package khipu.consensus.ibft.blockcreation

import static com.google.common.base.Preconditions.checkArgument;

import khipu.consensus.ibft.ConsensusRoundIdentifier
import khipu.domain.Address
import org.hyperledger.besu.consensus.common.BlockInterface;
import org.hyperledger.besu.consensus.common.ValidatorProvider;
import org.hyperledger.besu.consensus.common.VoteTallyCache;
import org.hyperledger.besu.consensus.ibft.ConsensusRoundIdentifier;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * Responsible for determining which member of the validator pool should propose the next block
 * (i.e. send the Proposal message).
 *
 * <p>It does this by extracting the previous block's proposer from the ProposerSeal (stored in the
 * Blocks ExtraData) then iterating through the validator list (stored in {@link
 * ValidatorProvider}), such that each new round for the given height is serviced by a different
 * validator.
 * 
   * If set changeEachBlock, will cause the proposer to change on successful addition of a block. Otherwise, the
   * previously successful proposer will propose the next block as well.
 */
class ProposerSelector(
      blockchain: Blockchain ,
      blockInterface: BlockInterface ,
      changeEachBlock: Boolean,
      voteTallyCache:VoteTallyCache ) {


  /**
   * Determines which validator should be acting as the proposer for a given sequence/round.
   *
   * @param roundIdentifier Identifies the chain height and proposal attempt number.
   * @return The address of the node which is to propose a block for the provided Round.
   */
  def selectProposerForRound(roundIdentifier:ConsensusRoundIdentifier ): Address =  {
    checkArgument(roundIdentifier.round >= 0)
    checkArgument(roundIdentifier.sequence > 0)

    val prevBlockNumber = roundIdentifier.sequence - 1;
    val maybeParentHeader = blockchain.getBlockHeader(prevBlockNumber);
    if (!maybeParentHeader.isDefined) {
      //LOG.trace("Unable to determine proposer for requested block {}", prevBlockNumber);
      throw new RuntimeException("Unable to determine past proposer");
    }

    val blockHeader = maybeParentHeader.get();
    val prevBlockProposer = blockInterface.getProposerOfBlock(blockHeader);
    val validatorsForRound = voteTallyCache.getVoteTallyAfterBlock(blockHeader).getValidators()

    if (!validatorsForRound.contains(prevBlockProposer)) {
      handleMissingProposer(prevBlockProposer, validatorsForRound, roundIdentifier)
    } else {
      handleWithExistingProposer(prevBlockProposer, validatorsForRound, roundIdentifier)
    }
  }

  /**
   * If the proposer of the previous block is missing, the validator with an Address above the
   * previous will become the next validator for the first round of the next block.
   *
   * <p>And validators will change from there.
   */
  private def handleMissingProposer(
      prevBlockProposer: Address ,
      validatorsForRound:Collection[Address] ,
      roundIdentifier: ConsensusRoundIdentifier ): Address =  {
    val validatorSet = new TreeSet[Address](validatorsForRound);
    val latterValidators = validatorSet.tailSet(prevBlockProposer, false);
    val nextProposer = if (latterValidators.isEmpty) {
      // i.e. prevBlockProposer was at the end of the validator list, so the right validator for
      // the start of this round is the first.
      validatorSet.first()
    } else {
      // Else, use the first validator after the dropped entry.
      latterValidators.first()
    }
    calculateRoundSpecificValidator(nextProposer, validatorsForRound, roundIdentifier.round);
  }

  /**
   * If the previous Proposer is still a validator - determine what offset should be applied for the
   * given round - factoring in a proposer change on the new block.
   */
  private def handleWithExistingProposer(
      prevBlockProposer: Address ,
      validatorsForRound:Collection[Address],
      roundIdentifier: ConsensusRoundIdentifier ): Address =  {
    var indexOffsetFromPrevBlock = roundIdentifier.round
    if (changeEachBlock) {
      indexOffsetFromPrevBlock += 1
    }
    calculateRoundSpecificValidator(prevBlockProposer, validatorsForRound, indexOffsetFromPrevBlock)
  }

  /**
   * Given Round 0 of the given height should start from given proposer (baseProposer) - determine
   * which validator should be used given the indexOffset.
   */
  private def calculateRoundSpecificValidator(
      baseProposer:Address ,
      validatorsForRound: Collection[Address] ,
      indexOffset: Int) : Address=  {
    val currentValidatorList = new ArrayList[Address](validatorsForRound);
    val prevProposerIndex = currentValidatorList.indexOf(baseProposer);
    val roundValidatorIndex = (prevProposerIndex + indexOffset) % currentValidatorList.size();
    currentValidatorList.get(roundValidatorIndex);
  }
}
