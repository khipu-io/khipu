package khipu.jsonrpc

import akka.util.ByteString
import java.math.BigInteger
import khipu.domain.Address
import org.json4s.JsonAST.{ JNull, JString }
import org.json4s.CustomSerializer

object JsonSerializers {

  object UnformattedDataJsonSerializer extends CustomSerializer[ByteString](_ =>
    (
      { PartialFunction.empty },
      { case bs: ByteString => JString(s"0x${khipu.toHexString(bs)}") }
    ))

  object QuantitiesSerializer extends CustomSerializer[BigInteger](_ =>
    (
      { PartialFunction.empty },
      {
        case n: BigInteger =>
          if (n.compareTo(BigInteger.ZERO) == 0)
            JString("0x0")
          else
            JString(s"0x${khipu.toHexString(n.toByteArray).dropWhile(_ == '0')}")
      }
    ))

  object OptionNoneToJNullSerializer extends CustomSerializer[Option[_]](formats =>
    (
      { PartialFunction.empty },
      { case None => JNull }
    ))

  object AddressJsonSerializer extends CustomSerializer[Address](_ =>
    (
      { PartialFunction.empty },
      { case addr: Address => JString(s"0x${khipu.toHexString(addr.bytes)}") }
    ))

}
