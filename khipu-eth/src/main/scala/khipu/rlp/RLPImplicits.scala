package khipu.rlp

import akka.util.ByteString
import java.math.BigInteger
import khipu.BigInt
import khipu.Hash
import khipu.rlp.RLP._
import khipu.util.BigIntUtil
import scala.annotation.switch

object RLPImplicits {

  implicit object byteEncDec extends RLPEncoder[Byte] with RLPDecoder[Byte] {
    override def encode(obj: Byte): RLPValue = RLPValue(byteToByteArray(obj))
    override def decode(rlp: RLPEncodeable): Byte = rlp match {
      case RLPValue(bytes) =>
        (bytes.length: @switch) match {
          case 0 => 0.toByte
          case 1 => (bytes(0) & 0xFF).toByte
          case _ => throw RLPException("src doesn't represent a byte")
        }
      case _ => throw RLPException("src is not an RLPValue")
    }
  }

  implicit object shortEncDec extends RLPEncoder[Short] with RLPDecoder[Short] {
    override def encode(obj: Short): RLPValue = RLPValue(shortToBigEndianMinLength(obj))
    override def decode(rlp: RLPEncodeable): Short = rlp match {
      case RLPValue(bytes) =>
        (bytes.length: @switch) match {
          case 0 => 0.toShort
          case 1 => (bytes(0) & 0xFF).toShort
          case 2 => (((bytes(0) & 0xFF) << 8) + (bytes(1) & 0xFF)).toShort
          case _ => throw RLPException("src doesn't represent a short")
        }
      case _ => throw RLPException("src is not an RLPValue")
    }
  }

  implicit object intEncDec extends RLPEncoder[Int] with RLPDecoder[Int] {
    override def encode(obj: Int): RLPValue = RLPValue(intToBigEndianMinLength(obj))
    override def decode(rlp: RLPEncodeable): Int = rlp match {
      case RLPValue(bytes) => bigEndianMinLengthToInt(bytes)
      case _               => throw RLPException("src is not an RLPValue")
    }
  }

  implicit object bigIntEncDec extends RLPEncoder[BigInt] with RLPDecoder[BigInt] {
    override def encode(obj: BigInt): RLPValue = RLPValue(
      if (obj.isZero) byteToByteArray(0) else BigIntUtil.toUnsignedByteArray(obj.toBigInteger)
    )
    override def decode(rlp: RLPEncodeable): BigInt = rlp match {
      case RLPValue(bytes) => new BigInt(bytes.foldLeft[BigInteger](BigInteger.ZERO) { (rec, byte) => (rec shiftLeft 8) add BigInteger.valueOf(byte & 0xFF) }) // TODO rewrite for BigInt
      case _               => throw RLPException("src is not an RLPValue")
    }
  }

  //Used for decoding and encoding positive (or 0) BigInts
  implicit object bigIntegerEncDec extends RLPEncoder[BigInteger] with RLPDecoder[BigInteger] {
    override def encode(obj: BigInteger): RLPValue = RLPValue(
      if (obj.compareTo(BigInteger.ZERO) == 0) byteToByteArray(0) else BigIntUtil.toUnsignedByteArray(obj)
    )
    override def decode(rlp: RLPEncodeable): BigInteger = rlp match {
      case RLPValue(bytes) => bytes.foldLeft[BigInteger](BigInteger.ZERO) { (rec, byte) => (rec shiftLeft 8) add BigInteger.valueOf(byte & 0xFF) }
      case _               => throw RLPException("src is not an RLPValue")
    }
  }

  //Used for decoding and encoding positive (or 0) longs
  implicit object longEncDec extends RLPEncoder[Long] with RLPDecoder[Long] {
    override def encode(obj: Long): RLPValue = bigIntegerEncDec.encode(BigInteger.valueOf(obj))
    override def decode(rlp: RLPEncodeable): Long = rlp match {
      case RLPValue(bytes) if bytes.length <= 8 => bigIntegerEncDec.decode(rlp).longValue
      case _                                    => throw RLPException("src is not an RLPValue")
    }
  }

  implicit object stringEncDec extends RLPEncoder[String] with RLPDecoder[String] {
    override def encode(obj: String): RLPValue = RLPValue(obj.getBytes)
    override def decode(rlp: RLPEncodeable): String = rlp match {
      case RLPValue(bytes) => new String(bytes)
      case _               => throw RLPException("src is not an RLPValue")
    }
  }

  implicit object byteArrayEncDec extends RLPEncoder[Array[Byte]] with RLPDecoder[Array[Byte]] {
    override def encode(obj: Array[Byte]): RLPValue = RLPValue(obj)
    override def decode(rlp: RLPEncodeable): Array[Byte] = rlp match {
      case RLPValue(bytes) => bytes
      case _               => throw RLPException("src is not an RLPValue")
    }
  }

  implicit object byteStringEncDec extends RLPEncoder[ByteString] with RLPDecoder[ByteString] {
    override def encode(obj: ByteString): RLPEncodeable = byteArrayEncDec.encode(obj.toArray)
    override def decode(rlp: RLPEncodeable): ByteString = ByteString(byteArrayEncDec.decode(rlp))
  }

  implicit object hashEncDec extends RLPEncoder[Hash] with RLPDecoder[Hash] {
    override def encode(obj: Hash): RLPEncodeable = byteArrayEncDec.encode(obj.bytes)
    override def decode(rlp: RLPEncodeable): Hash = Hash(byteArrayEncDec.decode(rlp))
  }

  implicit def seqEncDec[T]()(implicit enc: RLPEncoder[T], dec: RLPDecoder[T]): RLPEncoder[Seq[T]] with RLPDecoder[Seq[T]] = new RLPEncoder[Seq[T]] with RLPDecoder[Seq[T]] {
    override def encode(obj: Seq[T]): RLPEncodeable = RLPList(obj.map(enc.encode): _*)
    override def decode(rlp: RLPEncodeable): Seq[T] = rlp match {
      case l: RLPList => l.items.map(dec.decode)
      case _          => throw RLPException("src is not a Seq")
    }
  }

}
