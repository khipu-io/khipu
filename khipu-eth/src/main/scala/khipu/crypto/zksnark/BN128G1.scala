package khipu.crypto.zksnark

/**
 * Implementation of specific cyclic subgroup of points belonging to {@link BN128Fp} <br/>
 * Members of this subgroup are passed as a first param to pairing input {@link PairingCheck#addPair(BN128G1, BN128G2)} <br/>
 *
 * Subgroup generator G = (1; 2)
 *
 * @author Mikhail Kalinin
 * @since 01.09.2017
 */
object BN128G1 {
  /**
   * Formally we have to do this check
   * but in our domain it's not necessary,
   * thus always return true
   */
  private def isGroupMember(p: BN128[Fp]) = true

  /**
   * Checks whether point is a member of subgroup,
   * returns a point if check has been passed and null otherwise
   */
  def create(x: Array[Byte], y: Array[Byte]): BN128G1 = {
    val p = BN128Fp.create(x, y)

    if (p == null) return null

    if (!isGroupMember(p)) return null

    new BN128G1(p)
  }

}
class BN128G1(p: BN128[Fp]) extends BN128Fp(p.x, p.y, p.z) {
  override def toAffine(): BN128G1 = new BN128G1(super.toAffine())
}
