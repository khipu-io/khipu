package khipu.crypto.zksnark

import java.math.BigInteger
import khipu.crypto.zksnark.Params.B_Fp2

/**
 * Definition of {@link BN128} over F_p2, where "p" equals {@link Params#P} <br/>
 *
 * Curve equation: <br/>
 * Y^2 = X^3 + b, where "b" equals {@link Params#B_Fp2} <br/>
 *
 * @author Mikhail Kalinin
 * @since 31.08.2017
 */
object BN128Fp2 {
  // the point at infinity
  val ZERO = new BN128Fp2(Fp2.ZERO, Fp2.ZERO, Fp2.ZERO)

  /**
   * Checks whether provided data are coordinates of a point on the curve,
   * then checks if this point is a member of subgroup of order "r"
   * and if checks have been passed it returns a point, otherwise returns null
   */
  def create(aa: Array[Byte], bb: Array[Byte], cc: Array[Byte], dd: Array[Byte]): BN128[Fp2] = {

    val x = Fp2.create(aa, bb)
    val y = Fp2.create(cc, dd)

    // check for point at infinity
    if (x.isZero && y.isZero) {
      return ZERO
    }

    val p = new BN128Fp2(x, y, Fp2._1)

    // check whether point is a valid one
    if (p.isValid) {
      p
    } else {
      null
    }
  }

  protected def apply(a: BigInteger, b: BigInteger, c: BigInteger, d: BigInteger): BN128Fp2 =
    new BN128Fp2(Fp2.create(a, b), Fp2.create(c, d), Fp2._1)
}

class BN128Fp2(x: Fp2, y: Fp2, z: Fp2) extends BN128[Fp2](x, y, z) {
  protected def zero(): BN128[Fp2] = BN128Fp2.ZERO
  protected def instance(x: Fp2, y: Fp2, z: Fp2): BN128[Fp2] = new BN128Fp2(x, y, z)
  protected def b(): Fp2 = B_Fp2
  protected def one(): Fp2 = Fp2._1
}
