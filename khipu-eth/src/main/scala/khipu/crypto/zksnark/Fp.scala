package khipu.crypto.zksnark

import java.math.BigInteger

import khipu.crypto.zksnark.Params.P

/**
 * Arithmetic in F_p, p = 21888242871839275222246405745257275088696311157297823662689037894645226208583
 *
 * @author Mikhail Kalinin
 * @since 01.09.2017
 */
object Fp {
  val ZERO = new Fp(BigInteger.ZERO)
  val _1 = new Fp(BigInteger.ONE)
  val NON_RESIDUE = new Fp(new BigInteger("21888242871839275222246405745257275088696311157297823662689037894645226208582"))

  val _2_INV = new Fp(BigInteger.valueOf(2).modInverse(P))

  def create(v: Array[Byte]): Fp = {
    new Fp(new BigInteger(1, v))
  }

  def create(v: BigInteger): Fp = {
    new Fp(v)
  }
}
class Fp(val v: BigInteger) extends Field[Fp] {

  def add(o: Fp) = new Fp(v.add(o.v).mod(P))
  def mul(o: Fp) = new Fp(v.multiply(o.v).mod(P))
  def sub(o: Fp) = new Fp(v.subtract(o.v).mod(P))
  def squared() = new Fp(v.multiply(v).mod(P))
  def dbl = new Fp(v.add(v).mod(P))
  def inverse = new Fp(v.modInverse(P))
  def negate = new Fp(v.negate().mod(P))
  def isZero = v.compareTo(BigInteger.ZERO) == 0

  /**
   * Checks if provided value is a valid Fp member
   */
  def isValid = {
    v.compareTo(P) < 0
  }

  def mul(o: Fp2): Fp2 = new Fp2(o.a.mul(this), o.b.mul(this))

  def bytes: Array[Byte] = {
    v.toByteArray()
  }

  override def equals(o: Any): Boolean = {
    o match {
      case fp: Fp =>
        !(if (v != null) v.compareTo(fp.v) != 0 else fp.v != null)

      case _ => false
    }
  }

  override def toString = v.toString
}
