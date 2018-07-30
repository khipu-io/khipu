package khipu.crypto.zksnark

import java.math.BigInteger

import khipu.crypto.zksnark.Params.R
import khipu.crypto.zksnark.Params.TWIST_MUL_BY_P_X
import khipu.crypto.zksnark.Params.TWIST_MUL_BY_P_Y

/**
 * Implementation of specific cyclic subgroup of points belonging to {@link BN128Fp2} <br/>
 * Members of this subgroup are passed as a second param to pairing input {@link PairingCheck#addPair(BN128G1, BN128G2)} <br/>
 * <br/>
 *
 * The order of subgroup is {@link Params#R} <br/>
 * Generator of subgroup G = <br/>
 * (11559732032986387107991004021392285783925812861821192530917403151452391805634 * i + <br/>
 *  10857046999023057135944570762232829481370756359578518086990519993285655852781, <br/>
 *  4082367875863433681332203403145435568316851327593401208105741076214120093531 * i + <br/>
 *  8495653923123431417604973247489272438418190587263600148770280649306958101930) <br/>
 * <br/>
 *
 * @author Mikhail Kalinin
 * @since 31.08.2017
 */
object BN128G2 {
  /**
   * Checks whether provided data are coordinates of a point belonging to subgroup,
   * if check has been passed it returns a point, otherwise returns null
   */
  def create(a: Array[Byte], b: Array[Byte], c: Array[Byte], d: Array[Byte]): BN128G2 = {
    val p = BN128Fp2.create(a, b, c, d)

    // fails if point is invalid
    if (p == null) {
      return null
    }

    // check whether point is a subgroup member
    if (!isGroupMember(p)) return null

    new BN128G2(p)
  }

  def FR_NEG_ONE = BigInteger.ONE.negate().mod(R)

  private def isGroupMember(p: BN128[Fp2]): Boolean = {
    val left = p.mul(FR_NEG_ONE).add(p)
    left.isZero // should satisfy condition: -1 * p + p == 0, where -1 belongs to F_r
  }

}
final class BN128G2(x: Fp2, y: Fp2, z: Fp2) extends BN128Fp2(x, y, z) {
  def this(p: BN128[Fp2]) = this(p.x, p.y, p.z)

  override def toAffine(): BN128G2 = new BN128G2(super.toAffine())

  def mulByP(): BN128G2 = {
    val rx = TWIST_MUL_BY_P_X.mul(x.frobeniusMap(1))
    val ry = TWIST_MUL_BY_P_Y.mul(y.frobeniusMap(1))
    val rz = z.frobeniusMap(1)

    new BN128G2(rx, ry, rz)
  }
}
