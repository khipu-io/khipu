package khipu.crypto.zksnark

/**
 * Interface of abstract finite field
 *
 * @author Mikhail Kalinin
 * @since 05.09.2017
 */
trait Field[T] {
  def add(o: T): T
  def mul(o: T): T
  def sub(o: T): T
  def squared(): T
  def dbl(): T
  def inverse(): T
  def negate(): T
  def isZero: Boolean
  def isValid: Boolean
}
