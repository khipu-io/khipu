import akka.util.ByteString
import org.spongycastle.util.encoders.Hex

package object khipu {

  def toHexString(bytes: Array[Byte]): String = Hex.toHexString(bytes)
  def toHexString(bytes: ByteString): String = Hex.toHexString(bytes.toArray)
  def hexDecode(hexString: String): Array[Byte] = Hex.decode(hexString)

  // -1 value of offset/timestamp means unset
  final case class TVal(value: Array[Byte], offset: Long)
  final case class TKeyVal(key: Array[Byte], value: Array[Byte], offset: Long)

  sealed trait Log[+T] { def value: T }
  sealed trait Changed[+T] extends Log[T]
  final case class Deleted[T](value: T) extends Changed[T]
  final case class Updated[T](value: T) extends Changed[T]
  final case class Original[T](value: T) extends Log[T]
}
