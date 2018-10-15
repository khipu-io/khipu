package khipu.domain

import akka.util.ByteString
import khipu.crypto
import khipu.util.BytesUtil
import khipu.rlp
import khipu.trie.ByteArrayEncoder
import khipu.vm.UInt256

object Address {

  val Length = 20

  val hashedAddressEncoder = new ByteArrayEncoder[Address] {
    override def toBytes(addr: Address): Array[Byte] = crypto.kec256(addr.toArray)
  }

  def apply(bytes: ByteString): Address = {
    val len = bytes.length
    if (len == Length) {
      new Address(bytes)
    } else if (len > Length) {
      new Address(bytes.takeRight(Length))
    } else {
      new Address(BytesUtil.padLeft(bytes, Length, 0: Byte))
    }
  }

  def apply(addr: Long): Address = apply(UInt256(addr))
  def apply(uint: UInt256): Address = apply(uint.bytes)
  def apply(hexString: String): Address = {
    val bytes = khipu.hexDecode(hexString.replaceFirst("^0x", ""))
    require(bytes.length <= Length, s"Invalid address: $hexString")
    apply(bytes)
  }
  def apply(bytes: Array[Byte]): Address = apply(ByteString(bytes))
}

final class Address private (val bytes: ByteString) {
  lazy val id = khipu.toHexString(bytes)

  def toArray = bytes.toArray
  def toUInt256 = if (bytes.length == 0) UInt256.Zero else UInt256(bytes)

  override def equals(that: Any): Boolean = that match {
    case x: Address => java.util.Arrays.equals(x.bytes.toArray, this.bytes.toArray)
    case other      => false
  }

  override def hashCode: Int = bytes.hashCode
  override def toString: String = s"0x$id"
}
