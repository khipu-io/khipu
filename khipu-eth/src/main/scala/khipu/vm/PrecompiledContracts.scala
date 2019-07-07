package khipu.vm

import akka.util.ByteString
import khipu.UInt256
import khipu.crypto
import khipu.crypto.ECDSASignature
import khipu.crypto.zksnark.BN128Fp
import khipu.crypto.zksnark.BN128G1
import khipu.crypto.zksnark.BN128G2
import khipu.crypto.zksnark.PairingCheck
import khipu.domain.Address
import khipu.util.BigIntUtil
import khipu.util.BytesUtil
import java.math.BigInteger
import scala.util.Try

object PrecompiledContracts {
  val EcDsaRecAddr = Address(1)
  val Sha256Addr = Address(2)
  val Rip160Addr = Address(3)
  val IdAddr = Address(4)
  val ModExpAddr = Address(5)
  val AltBN128AddAddr = Address(6)
  val AltBN128MulAddr = Address(7)
  val AltBN128PairingAddr = Address(8)

  def getContractForAddress(address: Address, config: EvmConfig): Option[PrecompiledContract] = address match {
    case EcDsaRecAddr                         => Some(ECRecovery)
    case Sha256Addr                           => Some(Sha256)
    case Rip160Addr                           => Some(Ripemp160)
    case IdAddr                               => Some(Identity)
    case ModExpAddr if config.eip198          => Some(ModExp)
    case AltBN128AddAddr if config.eip213     => Some(BN128Add)
    case AltBN128MulAddr if config.eip213     => Some(BN128Mul)
    case AltBN128PairingAddr if config.eip212 => Some(BN128Pairing)
    case _                                    => None
  }

  private def encodeRes(_w1: Array[Byte], _w2: Array[Byte]): ByteString = {
    val res = Array.ofDim[Byte](64)

    val w1 = BytesUtil.stripLeadingZeroes(_w1)
    val w2 = BytesUtil.stripLeadingZeroes(_w2)

    System.arraycopy(w1, 0, res, 32 - w1.length, w1.length)
    System.arraycopy(w2, 0, res, 64 - w2.length, w2.length)

    ByteString(res)
  }

  sealed trait PrecompiledContract {
    protected def gas(input: ByteString): Long
    protected def exec(input: ByteString): (Boolean, ByteString)

    def run[W <: WorldState[W, S], S <: Storage[S]](context: ProgramContext[W, S]): ProgramResult[W, S] = {
      val requiredGas = gas(context.env.input)

      val (returnData, error, gasRemaining) = if (requiredGas <= context.startGas) {
        exec(context.env.input) match {
          case (true, res)  => (res, None, context.startGas - requiredGas)
          case (false, res) => (res, Some(PrecompiledContractFailed), 0L)
        }
      } else {
        (ByteString(), Some(OutOfGas), 0L)
      }

      ProgramResult(
        returnData,
        gasRemaining,
        context.world,
        Vector(),
        0,
        context.initialAddressesToDelete,
        context.initialAddressesTouched,
        error,
        false,
        Set()
      )
    }
  }

  object ECRecovery extends PrecompiledContract {
    def gas(input: ByteString): Long = 3000

    def exec(input: ByteString): (Boolean, ByteString) = {
      val bytes = input.toArray
      val data = Array.ofDim[Byte](128) // auto filled with 0
      System.arraycopy(bytes, 0, data, 0, math.min(bytes.length, data.length))

      val h = Array.ofDim[Byte](32) // auto filled with 0
      val v = Array.ofDim[Byte](32) // auto filled with 0
      val r = Array.ofDim[Byte](32) // auto filled with 0
      val s = Array.ofDim[Byte](32) // auto filled with 0
      System.arraycopy(data, 0, h, 0, h.length)
      System.arraycopy(data, 32, v, 0, v.length)
      System.arraycopy(data, 64, r, 0, r.length)
      System.arraycopy(data, 96, s, 0, s.length)

      val recovered = Try { ECDSASignature.recoverPublicKey(ECDSASignature(r, s, v), h) } getOrElse None
      (true, recovered.map { bytes =>
        val hash = ByteString(crypto.kec256(bytes).slice(12, 32))
        BytesUtil.padLeft(hash, 32, 0.toByte)
      }.getOrElse(ByteString()))
    }

  }

  object Sha256 extends PrecompiledContract {
    def gas(input: ByteString): Long = {
      if (input == null) {
        60
      } else {
        60 + 12 * UInt256.wordsForBytes(input.size)
      }
    }

    def exec(_input: ByteString): (Boolean, ByteString) = {
      val input = if (_input == null) ByteString() else _input
      (true, crypto.sha256(input))
    }
  }

  object Ripemp160 extends PrecompiledContract {
    def gas(input: ByteString): Long = {
      if (input == null) {
        600
      } else {
        600 + 120 * UInt256.wordsForBytes(input.size)
      }
    }

    def exec(_input: ByteString): (Boolean, ByteString) = {
      val input = if (_input == null) ByteString() else _input
      (true, BytesUtil.padLeft(crypto.ripemd160(input), 32, 0.toByte))
    }
  }

  object Identity extends PrecompiledContract {
    def gas(input: ByteString): Long = {
      if (input == null) {
        15
      } else {
        15 + 3 * UInt256.wordsForBytes(input.size)
      }
    }

    def exec(input: ByteString): (Boolean, ByteString) =
      (true, input)
  }

  object ModExp extends PrecompiledContract {
    private val GQUAD_DIVISOR = BigInteger.valueOf(20)
    private val ARGS_OFFSET = 32 * 3 // addresses length part

    def gas(_input: ByteString): Long = {
      val input = if (_input == null) ByteString() else _input

      val baseLen = parseLen(input, 0)
      val expLen = parseLen(input, 1)
      val modLen = parseLen(input, 2)

      val expHighBytes = BytesUtil.parseBytes(input, BigIntUtil.addSafely(ARGS_OFFSET, baseLen), math.min(expLen, 32))

      val multComplexity = getMultComplexity(math.max(baseLen, modLen))
      val adjExpLen = getAdjustedExponentLength(expHighBytes, expLen)

      // use big numbers to stay safe in case of overflow
      val gas = BigInteger.valueOf(multComplexity)
        .multiply(BigInteger.valueOf(Math.max(adjExpLen, 1)))
        .divide(GQUAD_DIVISOR)

      if (BigIntUtil.isLessThan(gas, BigInteger.valueOf(Long.MaxValue))) gas.longValue else Long.MaxValue
    }

    def exec(_input: ByteString): (Boolean, ByteString) = {
      val input = if (_input == null) ByteString() else _input

      val baseLen = parseLen(input, 0)
      val expLen = parseLen(input, 1)
      val modLen = parseLen(input, 2)

      val base = parseArg(input, ARGS_OFFSET, baseLen)
      val exp = parseArg(input, BigIntUtil.addSafely(ARGS_OFFSET, baseLen), expLen)
      val mod = parseArg(input, BigIntUtil.addSafely(BigIntUtil.addSafely(ARGS_OFFSET, baseLen), expLen), modLen)

      // check if modulus is zero
      if (BigIntUtil.isZero(mod))
        return (true, ByteString())

      val res = BytesUtil.stripLeadingZeroes(base.modPow(exp, mod).toByteArray)

      // adjust result to the same length as the modulus has
      if (res.length < modLen) {
        val adjRes = Array.ofDim[Byte](modLen)
        System.arraycopy(res, 0, adjRes, modLen - res.length, res.length)
        (true, ByteString(adjRes))
      } else {
        (true, ByteString(res))
      }
    }

    private def getMultComplexity(x: Long): Long = {
      val x2 = x * x
      if (x <= 64) {
        x2
      } else if (x <= 1024) {
        x2 / 4 + 96 * x - 3072
      } else {
        x2 / 16 + 480 * x - 199680
      }
    }

    private def getAdjustedExponentLength(expHighBytes: Array[Byte], expLen: Long): Long = {
      val leadingZeros = BytesUtil.numberOfLeadingZeros(expHighBytes)
      var highestBit = 8 * expHighBytes.length - leadingZeros

      // set index basement to zero
      if (highestBit > 0) highestBit -= 1

      if (expLen <= 32) {
        highestBit
      } else {
        8 * (expLen - 32) + highestBit
      }
    }

    private def parseLen(data: ByteString, idx: Int): Int = {
      val bytes = BytesUtil.parseBytes(data, 32 * idx, 32)
      UInt256(bytes).intValueSafe
    }

    private def parseArg(data: ByteString, offset: Int, len: Int): BigInteger = {
      val bytes = BytesUtil.parseBytes(data, offset, len)
      BytesUtil.bytesToBigInteger(bytes)
    }
  }

  /**
   * Computes point addition on Barreto–Naehrig curve.
   * See {@link BN128Fp} for details<br/>
   * <br/>
   *
   * input data[]:<br/>
   * two points encoded as (x, y), where x and y are 32-byte left-padded integers,<br/>
   * if input is shorter than expected, it's assumed to be right-padded with zero bytes<br/>
   * <br/>
   *
   * output:<br/>
   * resulting point (x', y'), where x and y encoded as 32-byte left-padded integers<br/>
   *
   */
  object BN128Add extends PrecompiledContract {
    def gas(input: ByteString): Long = {
      500
    }

    def exec(_input: ByteString): (Boolean, ByteString) = {
      val input = if (_input == null) ByteString() else _input

      val x1 = BytesUtil.parseWord(input, 0)
      val y1 = BytesUtil.parseWord(input, 1)

      val x2 = BytesUtil.parseWord(input, 2)
      val y2 = BytesUtil.parseWord(input, 3)

      val p1 = BN128Fp.create(x1, y1)
      if (p1 == null)
        return (false, ByteString())

      val p2 = BN128Fp.create(x2, y2)
      if (p2 == null)
        return (false, ByteString())

      val res = p1.add(p2).toEthNotation()

      (true, encodeRes(res.x.bytes, res.y.bytes))
    }
  }

  /**
   * Computes multiplication of scalar value on a point belonging to Barreto–Naehrig curve.
   * See {@link BN128Fp} for details<br/>
   * <br/>
   *
   * input data[]:<br/>
   * point encoded as (x, y) is followed by scalar s, where x, y and s are 32-byte left-padded integers,<br/>
   * if input is shorter than expected, it's assumed to be right-padded with zero bytes<br/>
   * <br/>
   *
   * output:<br/>
   * resulting point (x', y'), where x and y encoded as 32-byte left-padded integers<br/>
   *
   */
  object BN128Mul extends PrecompiledContract {
    def gas(input: ByteString): Long = {
      40000
    }

    def exec(_input: ByteString): (Boolean, ByteString) = {
      val input = if (_input == null) ByteString() else _input

      val x = BytesUtil.parseWord(input, 0)
      val y = BytesUtil.parseWord(input, 1)

      val s = BytesUtil.parseWord(input, 2)

      val p = BN128Fp.create(x, y)
      if (p == null)
        return (false, ByteString())

      val res = p.mul(BigIntUtil.toBI(s)).toEthNotation()

      (true, encodeRes(res.x.bytes, res.y.bytes))
    }
  }

  /**
   * Computes pairing check. <br/>
   * See {@link PairingCheck} for details.<br/>
   * <br/>
   *
   * Input data[]: <br/>
   * an array of points (a1, b1, ... , ak, bk), <br/>
   * where "ai" is a point of {@link BN128Fp} curve and encoded as two 32-byte left-padded integers (x; y) <br/>
   * "bi" is a point of {@link BN128G2} curve and encoded as four 32-byte left-padded integers {@code (ai + b; ci + d)},
   * each coordinate of the point is a big-endian {@link Fp2} number, so {@code b} precedes {@code a} in the encoding:
   * {@code (b, a; d, c)} <br/>
   * thus each pair (ai, bi) has 192 bytes length, if 192 is not a multiple of {@code data.length} then execution fails <br/>
   * the number of pairs is derived from input length by dividing it by 192 (the length of a pair) <br/>
   * <br/>
   *
   * output: <br/>
   * pairing product which is either 0 or 1, encoded as 32-byte left-padded integer <br/>
   *
   */
  object BN128Pairing extends PrecompiledContract {
    private val PAIR_SIZE = 192

    def gas(input: ByteString): Long = {
      if (input == null) {
        100000
      } else {
        100000 + 80000 * (input.length / PAIR_SIZE)
      }
    }

    def exec(_input: ByteString): (Boolean, ByteString) = {
      val input = if (_input == null) ByteString() else _input

      // fail if input len is not a multiple of PAIR_SIZE
      if (input.length % PAIR_SIZE > 0)
        return (false, ByteString())

      val check = PairingCheck.create()

      // iterating over all pairs
      var offset = 0
      while (offset < input.length) {
        val pair = decodePair(input, offset)

        // fail if decoding has failed
        if (pair == null)
          return (false, ByteString())

        check.addPair(pair._1, pair._2)
        offset += PAIR_SIZE
      }

      check.run()
      val result = check.result()

      (true, ByteString(UInt256(result).bytes))
    }

    private def decodePair(in: ByteString, offset: Int): (BN128G1, BN128G2) = {
      val x = BytesUtil.parseWord(in, offset, 0)
      val y = BytesUtil.parseWord(in, offset, 1)

      val p1 = BN128G1.create(x, y)

      // fail if point is invalid
      if (p1 == null) return null

      // (b, a)
      val b = BytesUtil.parseWord(in, offset, 2)
      val a = BytesUtil.parseWord(in, offset, 3)

      // (d, c)
      val d = BytesUtil.parseWord(in, offset, 4)
      val c = BytesUtil.parseWord(in, offset, 5)

      val p2 = BN128G2.create(a, b, c, d)

      // fail if point is invalid
      if (p2 == null) return null

      (p1, p2)
    }
  }
}
