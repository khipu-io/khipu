package khipu.vm

import khipu.EvmWord

/**
 * Account's storage representation. Implementation should be immutable and only keep track of changes to the storage
 */
trait Storage[S <: Storage[S]] {
  def store(offset: EvmWord, value: EvmWord): S
  def load(offset: EvmWord): EvmWord
  def getOriginalValue(address: EvmWord): Option[EvmWord]
}
