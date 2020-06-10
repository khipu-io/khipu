package khipu.consensus.common

import java.util.Collection
import khipu.domain.Address

trait ValidatorProvider {
  // Returns the current list of validators
  def getValidators(): Collection[Address]
}
