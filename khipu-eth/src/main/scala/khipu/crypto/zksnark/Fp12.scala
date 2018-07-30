package khipu.crypto.zksnark

import java.math.BigInteger

/**
 * Arithmetic in Fp_12 <br/>
 * <br/>
 *
 * "p" equals 21888242871839275222246405745257275088696311157297823662689037894645226208583, <br/>
 * elements of Fp_12 are represented with 2 elements of {@link Fp6} <br/>
 * <br/>
 *
 * Field arithmetic is ported from <a href="https://github.com/scipr-lab/libff/blob/master/libff/algebra/fields/fp12_2over3over2.tcc">libff</a>
 *
 * @author Mikhail Kalinin
 * @since 02.09.2017
 */
object Fp12 {
  val ZERO = new Fp12(Fp6.ZERO, Fp6.ZERO)
  val _1 = new Fp12(Fp6._1, Fp6.ZERO)

  val FROBENIUS_COEFFS_B = Array[Fp2](

    new Fp2(
      BigInteger.ONE,
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("8376118865763821496583973867626364092589906065868298776909617916018768340080"),
      new BigInteger("16469823323077808223889137241176536799009286646108169935659301613961712198316")
    ),

    new Fp2(
      new BigInteger("21888242871839275220042445260109153167277707414472061641714758635765020556617"),
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("11697423496358154304825782922584725312912383441159505038794027105778954184319"),
      new BigInteger("303847389135065887422783454877609941456349188919719272345083954437860409601")
    ),

    new Fp2(
      new BigInteger("21888242871839275220042445260109153167277707414472061641714758635765020556616"),
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("3321304630594332808241809054958361220322477375291206261884409189760185844239"),
      new BigInteger("5722266937896532885780051958958348231143373700109372999374820235121374419868")
    ),

    new Fp2(
      new BigInteger("21888242871839275222246405745257275088696311157297823662689037894645226208582"),
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("13512124006075453725662431877630910996106405091429524885779419978626457868503"),
      new BigInteger("5418419548761466998357268504080738289687024511189653727029736280683514010267")
    ),

    new Fp2(
      new BigInteger("2203960485148121921418603742825762020974279258880205651966"),
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("10190819375481120917420622822672549775783927716138318623895010788866272024264"),
      new BigInteger("21584395482704209334823622290379665147239961968378104390343953940207365798982")
    ),

    new Fp2(
      new BigInteger("2203960485148121921418603742825762020974279258880205651967"),
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("18566938241244942414004596690298913868373833782006617400804628704885040364344"),
      new BigInteger("16165975933942742336466353786298926857552937457188450663314217659523851788715")
    )
  )
}
final case class Fp12(a: Fp6, b: Fp6) extends Field[Fp12] {
  def squared(): Fp12 = {
    val ab = a.mul(b)

    val ra = a.add(b).mul(a.add(b.mulByNonResidue())).sub(ab).sub(ab.mulByNonResidue())
    val rb = ab.add(ab)

    new Fp12(ra, rb)
  }

  def dbl: Fp12 = {
    null
  }

  def mulBy024(ell0: Fp2, ellVW: Fp2, ellVV: Fp2): Fp12 = {
    var z0 = a.a
    var z1 = a.b
    var z2 = a.c
    var z3 = b.a
    var z4 = b.b
    var z5 = b.c

    val x0 = ell0
    val x2 = ellVV
    val x4 = ellVW

    val d0 = z0.mul(x0)
    val d2 = z2.mul(x2)
    val d4 = z4.mul(x4)
    val t2 = z0.add(z4)
    var t1 = z0.add(z2)
    val s0 = z1.add(z3).add(z5)

    // For z.a_.a_ = z0.
    var s1 = z1.mul(x2)
    var t3 = s1.add(d4)
    var t4 = Fp6.NON_RESIDUE.mul(t3).add(d0)
    z0 = t4

    // For z.a_.b_ = z1
    t3 = z5.mul(x4)
    s1 = s1.add(t3)
    t3 = t3.add(d2)
    t4 = Fp6.NON_RESIDUE.mul(t3)
    t3 = z1.mul(x0)
    s1 = s1.add(t3)
    t4 = t4.add(t3)
    z1 = t4

    // For z.a_.c_ = z2
    var t0 = x0.add(x2)
    t3 = t1.mul(t0).sub(d0).sub(d2)
    t4 = z3.mul(x4)
    s1 = s1.add(t4)
    t3 = t3.add(t4)

    // For z.b_.a_ = z3 (z3 needs z2)
    t0 = z2.add(z4)
    z2 = t3
    t1 = x2.add(x4)
    t3 = t0.mul(t1).sub(d2).sub(d4)
    t4 = Fp6.NON_RESIDUE.mul(t3)
    t3 = z3.mul(x0)
    s1 = s1.add(t3)
    t4 = t4.add(t3)
    z3 = t4

    // For z.b_.b_ = z4
    t3 = z5.mul(x2)
    s1 = s1.add(t3)
    t4 = Fp6.NON_RESIDUE.mul(t3)
    t0 = x0.add(x4)
    t3 = t2.mul(t0).sub(d0).sub(d4)
    t4 = t4.add(t3)
    z4 = t4

    // For z.b_.c_ = z5.
    t0 = x0.add(x2).add(x4)
    t3 = s0.mul(t0).sub(s1)
    z5 = t3

    new Fp12(new Fp6(z0, z1, z2), new Fp6(z3, z4, z5))
  }

  def add(o: Fp12): Fp12 = new Fp12(a.add(o.a), b.add(o.b))

  def mul(o: Fp12): Fp12 = {
    val a2 = o.a
    val b2 = o.b
    val a1 = a
    val b1 = b

    val a1a2 = a1.mul(a2)
    val b1b2 = b1.mul(b2)

    val ra = a1a2.add(b1b2.mulByNonResidue())
    val rb = a1.add(b1).mul(a2.add(b2)).sub(a1a2).sub(b1b2)

    new Fp12(ra, rb)
  }

  def sub(o: Fp12): Fp12 = {
    new Fp12(a.sub(o.a), b.sub(o.b))
  }

  def inverse(): Fp12 = {
    val t0 = a.squared()
    val t1 = b.squared()
    val t2 = t0.sub(t1.mulByNonResidue())
    val t3 = t2.inverse()

    val ra = a.mul(t3)
    val rb = b.mul(t3).negate()

    new Fp12(ra, rb)
  }

  def negate(): Fp12 = {
    new Fp12(a.negate(), b.negate())
  }

  def isZero: Boolean = {
    this.equals(Fp12.ZERO)
  }

  def isValid: Boolean = {
    a.isValid && b.isValid
  }

  def frobeniusMap(power: Int): Fp12 = {
    val ra = a.frobeniusMap(power)
    val rb = b.frobeniusMap(power).mul(Fp12.FROBENIUS_COEFFS_B(power % 12))

    return new Fp12(ra, rb)
  }

  def cyclotomicSquared(): Fp12 = {

    var z0 = a.a
    var z4 = a.b
    var z3 = a.c
    var z2 = b.a
    var z1 = b.b
    var z5 = b.c

    // t0 + t1*y = (z0 + z1*y)^2 = a^2
    var tmp = z0.mul(z1)
    val t0 = z0.add(z1).mul(z0.add(Fp6.NON_RESIDUE.mul(z1))).sub(tmp).sub(Fp6.NON_RESIDUE.mul(tmp))
    val t1 = tmp.add(tmp)
    // t2 + t3*y = (z2 + z3*y)^2 = b^2
    tmp = z2.mul(z3)
    val t2 = z2.add(z3).mul(z2.add(Fp6.NON_RESIDUE.mul(z3))).sub(tmp).sub(Fp6.NON_RESIDUE.mul(tmp))
    val t3 = tmp.add(tmp)
    // t4 + t5*y = (z4 + z5*y)^2 = c^2
    tmp = z4.mul(z5)
    val t4 = z4.add(z5).mul(z4.add(Fp6.NON_RESIDUE.mul(z5))).sub(tmp).sub(Fp6.NON_RESIDUE.mul(tmp))
    val t5 = tmp.add(tmp)

    // for A

    // z0 = 3 * t0 - 2 * z0
    z0 = t0.sub(z0)
    z0 = z0.add(z0)
    z0 = z0.add(t0)
    // z1 = 3 * t1 + 2 * z1
    z1 = t1.add(z1)
    z1 = z1.add(z1)
    z1 = z1.add(t1)

    // for B

    // z2 = 3 * (xi * t5) + 2 * z2
    tmp = Fp6.NON_RESIDUE.mul(t5)
    z2 = tmp.add(z2)
    z2 = z2.add(z2)
    z2 = z2.add(tmp)

    // z3 = 3 * t4 - 2 * z3
    z3 = t4.sub(z3)
    z3 = z3.add(z3)
    z3 = z3.add(t4)

    // for C

    // z4 = 3 * t2 - 2 * z4
    z4 = t2.sub(z4)
    z4 = z4.add(z4)
    z4 = z4.add(t2)

    // z5 = 3 * t3 + 2 * z5
    z5 = t3.add(z5)
    z5 = z5.add(z5)
    z5 = z5.add(t3)

    new Fp12(new Fp6(z0, z4, z3), new Fp6(z2, z1, z5))
  }

  def cyclotomicExp(pow: BigInteger): Fp12 = {
    var res = Fp12._1
    var i = pow.bitLength() - 1
    while (i >= 0) {
      res = res.cyclotomicSquared()

      if (pow.testBit(i)) {
        res = res.mul(this)
      }
      i -= 1
    }

    res
  }

  def unitaryInverse(): Fp12 = {
    val ra = a
    val rb = b.negate()
    new Fp12(ra, rb)
  }

  def negExp(exp: BigInteger): Fp12 = {
    this.cyclotomicExp(exp).unitaryInverse()
  }

  override def equals(o: Any): Boolean = {
    o match {
      case fp12: Fp12 =>
        if (if (a != null) !a.equals(fp12.a) else fp12.a != null) return false
        return !(if (b != null) !b.equals(fp12.b) else fp12.b != null)
      case _ => false
    }
  }

  override def toString() = {
    String.format(
      "Fp12 (%s; %s)\n" +
        "     (%s; %s)\n" +
        "     (%s; %s)\n" +
        "     (%s; %s)\n" +
        "     (%s; %s)\n" +
        "     (%s; %s)\n",

      a.a.a, a.a.b,
      a.b.a, a.b.b,
      a.c.a, a.c.b,
      b.a.a, b.a.b,
      b.b.a, b.b.b,
      b.c.a, b.c.b
    )
  }

}
