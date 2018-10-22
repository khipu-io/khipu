package khipu.crypto.zksnark

import java.math.BigInteger

/**
 * Arithmetic in F_p2 <br/>
 * <br/>
 *
 * "p" equals 21888242871839275222246405745257275088696311157297823662689037894645226208583,
 * elements of F_p2 are represented as a polynomials "a * i + b" modulo "i^2 + 1" from the ring F_p[i] <br/>
 * <br/>
 *
 * Field arithmetic is ported from <a href="https://github.com/scipr-lab/libff/blob/master/libff/algebra/fields/fp2.tcc">libff</a> <br/>
 *
 * @author Mikhail Kalinin
 * @since 01.09.2017
 */
object Fp2 {
  val ZERO = new Fp2(Fp.ZERO, Fp.ZERO)
  val _1 = new Fp2(Fp._1, Fp.ZERO)
  val NON_RESIDUE = new Fp2(BigInteger.valueOf(9), BigInteger.ONE)

  val FROBENIUS_COEFFS_B = Array[Fp](
    new Fp(BigInteger.ONE),
    new Fp(new BigInteger("21888242871839275222246405745257275088696311157297823662689037894645226208582"))
  )

  def create(aa: BigInteger, bb: BigInteger): Fp2 = {
    val a = Fp.create(aa)
    val b = Fp.create(bb)
    new Fp2(a, b)
  }

  def create(aa: Array[Byte], bb: Array[Byte]): Fp2 = {
    val a = Fp.create(aa)
    val b = Fp.create(bb)
    new Fp2(a, b)
  }

}
final case class Fp2(a: Fp, b: Fp) extends Field[Fp2] {
  def this(a: BigInteger, b: BigInteger) = this(new Fp(a), new Fp(b))

  def squared(): Fp2 = {

    // using Complex squaring

    val ab = a.mul(b)

    val ra = a.add(b).mul(b.mul(Fp.NON_RESIDUE).add(a))
      .sub(ab).sub(ab.mul(Fp.NON_RESIDUE)) // ra = (a + b)(a + NON_RESIDUE * b) - ab - NON_RESIDUE * b
    val rb = ab.dbl()

    return new Fp2(ra, rb)
  }

  def mul(o: Fp2): Fp2 = {

    val aa = a.mul(o.a)
    val bb = b.mul(o.b)

    val ra = bb.mul(Fp.NON_RESIDUE).add(aa) // ra = a1 * a2 + NON_RESIDUE * b1 * b2
    val rb = a.add(b).mul(o.a.add(o.b)).sub(aa).sub(bb) // rb = (a1 + b1)(a2 + b2) - a1 * a2 - b1 * b2

    new Fp2(ra, rb)
  }

  def add(o: Fp2): Fp2 = {
    return new Fp2(a.add(o.a), b.add(o.b))
  }

  def sub(o: Fp2): Fp2 = {
    return new Fp2(a.sub(o.a), b.sub(o.b))
  }

  def dbl: Fp2 = {
    return this.add(this)
  }

  def inverse(): Fp2 = {

    val t0 = a.squared()
    val t1 = b.squared()
    val t2 = t0.sub(Fp.NON_RESIDUE.mul(t1))
    val t3 = t2.inverse()

    val ra = a.mul(t3) // ra = a * t3
    val rb = b.mul(t3).negate() // rb = -(b * t3)

    return new Fp2(ra, rb)
  }

  def negate(): Fp2 = {
    return new Fp2(a.negate(), b.negate())
  }

  def isZero = {
    this.equals(Fp2.ZERO)
  }

  def isValid = {
    a.isValid && b.isValid
  }

  override def equals(any: Any): Boolean = {
    any match {
      case that: Fp2 =>
        (this eq that) || {
          if (if (this.a ne null) !this.a.equals(that.a) else that.a ne null) {
            false
          } else {
            !(if (this.b ne null) !this.b.equals(that.b) else that.b ne null)
          }
        }
      case _ => false
    }
  }

  def frobeniusMap(power: Int): Fp2 = {
    val ra = a
    val rb = Fp2.FROBENIUS_COEFFS_B(power % 2).mul(b)

    new Fp2(ra, rb)
  }

  def mulByNonResidue(): Fp2 = Fp2.NON_RESIDUE.mul(this)

  override def toString() = {
    String.format("%si + %s", a.toString, b.toString)
  }
}
