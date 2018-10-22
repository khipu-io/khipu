package khipu.crypto.zksnark

import java.math.BigInteger

/**
 * Implementation of Barreto–Naehrig curve defined over abstract finite field. This curve is one of the keys to zkSNARKs. <br/>
 * This specific curve was introduced in
 * <a href="https://github.com/scipr-lab/libff#elliptic-curve-choices">libff</a>
 * and used by a proving system in
 * <a href="https://github.com/zcash/zcash/wiki/specification#zcash-protocol">ZCash protocol</a> <br/>
 * <br/>
 *
 * Curve equation: <br/>
 * Y^2 = X^3 + b, where "b" is a constant number belonging to corresponding specific field <br/>
 * Point at infinity is encoded as <code>(0, 0, 0)</code> <br/>
 * <br/>
 *
 * This curve has embedding degree 12 with respect to "r" (see {@link Params#R}), which means that "r" is a multiple of "p ^ 12 - 1",
 * this condition is important for pairing operation implemented in {@link PairingCheck}<br/>
 * <br/>
 *
 * Code of curve arithmetic has been ported from
 * <a href="https://github.com/scipr-lab/libff/blob/master/libff/algebra/curves/alt_bn128/alt_bn128_g1.cpp">libff</a> <br/>
 * <br/>
 *
 * Current implementation uses Jacobian coordinate system as
 * <a href="https://github.com/scipr-lab/libff/blob/master/libff/algebra/curves/alt_bn128/alt_bn128_g1.cpp">libff</a> does,
 * use {@link #toEthNotation()} to convert Jacobian coords to Ethereum encoding <br/>
 *
 * @author Mikhail Kalinin
 * @since 05.09.2017
 */
abstract class BN128[T <: Field[T]] protected (val x: T, val y: T, val z: T) {

  /**
   * Point at infinity in Ethereum notation: should return (0; 0; 0),
   * {@link #isZero()} method called for that point, also, returns {@code true}
   */
  protected def zero(): BN128[T]
  protected def instance(x: T, y: T, z: T): BN128[T]
  protected def b(): T
  protected def one(): T

  /**
   * Transforms given Jacobian to affine coordinates and then creates a point
   */
  def toAffine(): BN128[T] = {

    if (isZero) {
      val z = zero()
      return instance(z.x, one(), z.z) // (0; 1; 0)
    }

    val zInv = z.inverse()
    val zInv2 = zInv.squared()
    val zInv3 = zInv2.mul(zInv)

    val ax = x.mul(zInv2)
    val ay = y.mul(zInv3)

    return instance(ax, ay, one())
  }

  /**
   * Runs affine transformation and encodes point at infinity as (0; 0; 0)
   */
  def toEthNotation(): BN128[T] = {
    val affine = toAffine()

    // affine zero is (0; 1; 0), convert to Ethereum zero: (0; 0; 0)
    if (affine.isZero) {
      zero()
    } else {
      affine
    }
  }

  protected def isOnCurve: Boolean = {

    if (isZero) return true

    val z6 = z.squared().mul(z).squared()

    val left = y.squared() // y^2
    val right = x.squared().mul(x).add(b().mul(z6)) // x^3 + b * z^6
    left.equals(right)
  }

  def add(o: BN128[T]): BN128[T] = {

    if (this.isZero) return o // 0 + P = P
    if (o.isZero) return this // P + 0 = P

    val x1 = this.x
    val y1 = this.y
    val z1 = this.z
    val x2 = o.x
    val y2 = o.y
    val z2 = o.z

    // ported code is started from here
    // next calculations are done in Jacobian coordinates

    val z1z1 = z1.squared()
    val z2z2 = z2.squared()

    val u1 = x1.mul(z2z2)
    val u2 = x2.mul(z1z1)

    val z1Cubed = z1.mul(z1z1)
    val z2Cubed = z2.mul(z2z2)

    val s1 = y1.mul(z2Cubed) // s1 = y1 * Z2^3
    val s2 = y2.mul(z1Cubed) // s2 = y2 * Z1^3

    if (u1.equals(u2) && s1.equals(s2)) {
      return dbl // P + P = 2P
    }

    val h = u2.sub(u1) // h = u2 - u1
    val i = h.dbl().squared() // i = (2 * h)^2
    val j = h.mul(i) // j = h * i
    val r = s2.sub(s1).dbl() // r = 2 * (s2 - s1)
    val v = u1.mul(i) // v = u1 * i
    val zz = z1.add(z2).squared()
      .sub(z1.squared()).sub(z2.squared())

    val x3 = r.squared().sub(j).sub(v.dbl()) // x3 = r^2 - j - 2 * v
    val y3 = v.sub(x3).mul(r).sub(s1.mul(j).dbl()) // y3 = r * (v - x3) - 2 * (s1 * j)
    val z3 = zz.mul(h) // z3 = ((z1+z2)^2 - z1^2 - z2^2) * h = zz * h

    return instance(x3, y3, z3)
  }

  def mul(s: BigInteger): BN128[T] = {
    if (s.compareTo(BigInteger.ZERO) == 0) // P * 0 = 0
      return zero()

    if (isZero) return this // 0 * s = 0

    var res = zero()
    var i = s.bitLength - 1
    while (i >= 0) {
      res = res.dbl

      if (s.testBit(i)) {
        res = res.add(this)
      }
      i -= 1
    }

    return res
  }

  private def dbl: BN128[T] = {

    if (isZero) return this

    // ported code is started from here
    // next calculations are done in Jacobian coordinates with z = 1

    val a = x.squared() // a = x^2
    val b = y.squared() // b = y^2
    val c = b.squared() // c = b^2
    var d = x.add(b).squared().sub(a).sub(c)
    d = d.add(d) // d = 2 * ((x + b)^2 - a - c)
    val e = a.add(a).add(a) // e = 3 * a
    val f = e.squared() // f = e^2

    val x3 = f.sub(d.add(d)) // rx = f - 2 * d
    val y3 = e.mul(d.sub(x3)).sub(c.dbl().dbl().dbl()) // ry = e * (d - rx) - 8 * c
    val z3 = y.mul(z).dbl() // z3 = 2 * y * z

    instance(x3, y3, z3)
  }

  def isZero: Boolean = z.isZero

  protected def isValid: Boolean = {
    // check whether coordinates belongs to the Field
    if (!x.isValid || !y.isValid || !z.isValid) {
      return false
    }

    // check whether point is on the curve
    if (!isOnCurve) {
      return false
    }

    return true
  }

  override def toString = {
    String.format("(%s; %s; %s)", x.toString, y.toString, z.toString)
  }

  override def equals(any: Any): Boolean = {
    any match {
      case that: BN128[_] =>
        (this eq that) || {
          if (if (this.x ne null) !this.x.equals(that.x) else that.x ne null) {
            false
          } else if (if (this.y ne null) !this.y.equals(that.y) else that.y ne null) {
            false
          } else {
            !(if (this.z ne null) !this.z.equals(that.z) else that.z ne null)
          }
        }
      case _ => false
    }
  }
}
