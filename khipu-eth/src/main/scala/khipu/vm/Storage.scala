package khipu.vm

import khipu.DataWord

/**
 * Account's storage representation. Implementation should be immutable and only keep track of changes to the storage
 */
trait Storage[S <: Storage[S]] {
  def store(offset: DataWord, value: DataWord): S
  def load(offset: DataWord): DataWord
  def getOriginalValue(address: DataWord): DataWord
}
