package khipu.consensus.common

import java.util.Collection
import khipu.domain.Address
import khipu.domain.BlockHeader

trait BlockInterface {

  def getProposerOfBlock(header: BlockHeader): Address;

  //def getProposerOfBlock(header: BlockHeader ): Address 

  def extractVoteFromHeader(header: BlockHeader): Option[ValidatorVote]

  def validatorsInBlock(header: BlockHeader): Collection[Address]
}
