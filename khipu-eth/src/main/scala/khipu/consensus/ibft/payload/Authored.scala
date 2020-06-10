package khipu.consensus.ibft.payload

import khipu.domain.Address

trait Authored {
  def getAuthor(): Address
}
