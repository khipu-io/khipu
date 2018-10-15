package khipu.vm

import khipu.UInt256

/**
 * Account's storage representation. Implementation should be immutable and only keep track of changes to the storage
 */
trait Storage[S <: Storage[S]] {
  def store(offset: UInt256, value: UInt256): S
  def load(offset: UInt256): UInt256
}
