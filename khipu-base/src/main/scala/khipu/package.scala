import akka.util.ByteString
import org.spongycastle.util.encoders.Hex

package object khipu {

  type UInt256 = UInt256_biginteger.UInt256
  val UInt256 = UInt256_biginteger.UInt256

  def toHexString(bytes: Array[Byte]): String = Hex.toHexString(bytes)
  def toHexString(bytes: ByteString): String = Hex.toHexString(bytes.toArray)
  def hexDecode(hexString: String): Array[Byte] = Hex.decode(hexString)
}
