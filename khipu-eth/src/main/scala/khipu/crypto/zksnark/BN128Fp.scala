package khipu.crypto.zksnark

import khipu.crypto.zksnark.Params.B_Fp

/**
 * Definition of {@link BN128} over F_p, where "p" equals {@link Params#P} <br/>
 *
 * Curve equation: <br/>
 * Y^2 = X^3 + b, where "b" equals {@link Params#B_Fp} <br/>
 *
 * @author Mikhail Kalinin
 * @since 21.08.2017
 */
object BN128Fp {
  // the point at infinity
  val ZERO = new BN128Fp(Fp.ZERO, Fp.ZERO, Fp.ZERO)

  /**
   * Checks whether x and y belong to Fp,
   * then checks whether point with (x; y) coordinates lays on the curve.
   *
   * Returns new point if all checks have been passed,
   * otherwise returns null
   */
  def create(xx: Array[Byte], yy: Array[Byte]): BN128[Fp] = {
    val x = Fp.create(xx)
    val y = Fp.create(yy)

    // check for point at infinity
    if (x.isZero && y.isZero) {
      return ZERO
    }

    val p = new BN128Fp(x, y, Fp._1)

    // check whether point is a valid one
    if (p.isValid) {
      p
    } else {
      null
    }
  }

}
class BN128Fp(x: Fp, y: Fp, z: Fp) extends BN128[Fp](x, y, z) {
  protected def zero: BN128[Fp] = BN128Fp.ZERO
  protected def instance(x: Fp, y: Fp, z: Fp): BN128[Fp] = new BN128Fp(x, y, z)
  protected def b(): Fp = B_Fp
  protected def one(): Fp = Fp._1
}
