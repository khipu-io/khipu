package khipu.consensus.common

import khipu.domain.Address

import java.util.Collection;
import java.util.List;
import java.util.Map;

class IbftValidatorOverrides(overriddenValidators: Map[Long, List[Address]]) {
  def getForBlock(blockNumber: Long): Option[Collection[Address]] = {
    Option(overriddenValidators.get(blockNumber))
  }
}
