package khipu.crypto.zksnark

import java.math.BigInteger

/**
 * Arithmetic in Fp_6 <br/>
 * <br/>
 *
 * "p" equals 21888242871839275222246405745257275088696311157297823662689037894645226208583, <br/>
 * elements of Fp_6 are represented with 3 elements of {@link Fp2} <br/>
 * <br/>
 *
 * Field arithmetic is ported from <a href="https://github.com/scipr-lab/libff/blob/master/libff/algebra/fields/fp6_3over2.tcc">libff</a>
 *
 * @author Mikhail Kalinin
 * @since 05.09.2017
 */
object Fp6 {
  val ZERO = new Fp6(Fp2.ZERO, Fp2.ZERO, Fp2.ZERO)
  val _1 = new Fp6(Fp2._1, Fp2.ZERO, Fp2.ZERO)
  val NON_RESIDUE = new Fp2(BigInteger.valueOf(9), BigInteger.ONE)

  val FROBENIUS_COEFFS_B = Array(

    new Fp2(
      BigInteger.ONE,
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("21575463638280843010398324269430826099269044274347216827212613867836435027261"),
      new BigInteger("10307601595873709700152284273816112264069230130616436755625194854815875713954")
    ),

    new Fp2(
      new BigInteger("21888242871839275220042445260109153167277707414472061641714758635765020556616"),
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("3772000881919853776433695186713858239009073593817195771773381919316419345261"),
      new BigInteger("2236595495967245188281701248203181795121068902605861227855261137820944008926")
    ),

    new Fp2(
      new BigInteger("2203960485148121921418603742825762020974279258880205651966"),
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("18429021223477853657660792034369865839114504446431234726392080002137598044644"),
      new BigInteger("9344045779998320333812420223237981029506012124075525679208581902008406485703")
    )
  )

  val FROBENIUS_COEFFS_C = Array(

    new Fp2(
      BigInteger.ONE,
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("2581911344467009335267311115468803099551665605076196740867805258568234346338"),
      new BigInteger("19937756971775647987995932169929341994314640652964949448313374472400716661030")
    ),

    new Fp2(
      new BigInteger("2203960485148121921418603742825762020974279258880205651966"),
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("5324479202449903542726783395506214481928257762400643279780343368557297135718"),
      new BigInteger("16208900380737693084919495127334387981393726419856888799917914180988844123039")
    ),

    new Fp2(
      new BigInteger("21888242871839275220042445260109153167277707414472061641714758635765020556616"),
      BigInteger.ZERO
    ),

    new Fp2(
      new BigInteger("13981852324922362344252311234282257507216387789820983642040889267519694726527"),
      new BigInteger("7629828391165209371577384193250820201684255241773809077146787135900891633097")
    )
  )
}
final case class Fp6(a: Fp2, b: Fp2, c: Fp2) extends Field[Fp6] {
  def squared(): Fp6 = {

    val s0 = a.squared()
    val ab = a.mul(b)
    val s1 = ab.dbl()
    val s2 = a.sub(b).add(c).squared()
    val bc = b.mul(c)
    val s3 = bc.dbl()
    val s4 = c.squared()

    val ra = s0.add(s3.mulByNonResidue())
    val rb = s1.add(s4.mulByNonResidue())
    val rc = s1.add(s2).add(s3).sub(s0).sub(s4)

    new Fp6(ra, rb, rc)
  }

  def dbl: Fp6 = {
    this.add(this)
  }

  def mul(o: Fp6): Fp6 = {
    val a1 = a
    val b1 = b
    val c1 = c
    val a2 = o.a
    val b2 = o.b
    val c2 = o.c

    val a1a2 = a1.mul(a2)
    val b1b2 = b1.mul(b2)
    val c1c2 = c1.mul(c2)

    val ra = a1a2.add(b1.add(c1).mul(b2.add(c2)).sub(b1b2).sub(c1c2).mulByNonResidue())
    val rb = a1.add(b1).mul(a2.add(b2)).sub(a1a2).sub(b1b2).add(c1c2.mulByNonResidue())
    val rc = a1.add(c1).mul(a2.add(c2)).sub(a1a2).add(b1b2).sub(c1c2)

    new Fp6(ra, rb, rc)
  }

  def mul(o: Fp2): Fp6 = {
    val ra = a.mul(o)
    val rb = b.mul(o)
    val rc = c.mul(o)

    new Fp6(ra, rb, rc)
  }

  def mulByNonResidue(): Fp6 = {
    val ra = Fp6.NON_RESIDUE.mul(c)
    val rb = a
    val rc = b

    new Fp6(ra, rb, rc)
  }

  def add(o: Fp6): Fp6 = {
    val ra = a.add(o.a)
    val rb = b.add(o.b)
    val rc = c.add(o.c)

    new Fp6(ra, rb, rc)
  }

  def sub(o: Fp6): Fp6 = {
    val ra = a.sub(o.a)
    val rb = b.sub(o.b)
    val rc = c.sub(o.c)

    new Fp6(ra, rb, rc)
  }

  def inverse(): Fp6 = {

    /* From "High-Speed Software Implementation of the Optimal Ate Pairing over Barreto-Naehrig Curves"; Algorithm 17 */

    val t0 = a.squared()
    val t1 = b.squared()
    val t2 = c.squared()
    val t3 = a.mul(b)
    val t4 = a.mul(c)
    val t5 = b.mul(c)
    val c0 = t0.sub(t5.mulByNonResidue())
    val c1 = t2.mulByNonResidue().sub(t3)
    val c2 = t1.sub(t4) // typo in paper referenced above. should be "-" as per Scott, but is "*"
    val t6 = a.mul(c0).add((c.mul(c1).add(b.mul(c2))).mulByNonResidue()).inverse()

    val ra = t6.mul(c0)
    val rb = t6.mul(c1)
    val rc = t6.mul(c2)

    new Fp6(ra, rb, rc)
  }

  def negate(): Fp6 = {
    new Fp6(a.negate(), b.negate(), c.negate())
  }

  def isZero: Boolean = {
    this.equals(Fp6.ZERO)
  }

  def isValid: Boolean = {
    a.isValid && b.isValid && c.isValid
  }

  def frobeniusMap(power: Int): Fp6 = {
    val ra = a.frobeniusMap(power)
    val rb = Fp6.FROBENIUS_COEFFS_B(power % 6).mul(b.frobeniusMap(power))
    val rc = Fp6.FROBENIUS_COEFFS_C(power % 6).mul(c.frobeniusMap(power))

    new Fp6(ra, rb, rc)
  }

  override def equals(o: Any): Boolean = {
    o match {
      case fp6: Fp6 =>
        if (if (a != null) !a.equals(fp6.a) else fp6.a != null) return false
        if (if (b != null) !b.equals(fp6.b) else fp6.b != null) return false
        return !(if (c != null) !c.equals(fp6.c) else fp6.c != null)
      case _ => false
    }
  }

}
