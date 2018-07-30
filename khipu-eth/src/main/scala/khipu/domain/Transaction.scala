package khipu.domain

import akka.util.ByteString
import java.math.BigInteger

object Transaction {

  val NonceLength = 32
  val GasLength = 32
  val ValueLength = 32

  def apply(nonce: BigInteger, gasPrice: BigInteger, gasLimit: Long, receivingAddress: Address, value: BigInteger, payload: ByteString): Transaction =
    Transaction(nonce, gasPrice, gasLimit, Some(receivingAddress), value, payload)

}

final case class Transaction(
    nonce:            BigInteger,
    gasPrice:         BigInteger,
    gasLimit:         Long,
    receivingAddress: Option[Address],
    value:            BigInteger,
    payload:          ByteString
) {

  def isContractCreation: Boolean = receivingAddress.isEmpty

  override def toString: String = {
    s"""Transaction {
         |nonce: $nonce
         |gasPrice: $gasPrice
         |gasLimit: $gasLimit
         |receivingAddress: ${if (receivingAddress.isDefined) khipu.toHexString(receivingAddress.get.bytes) else "[Contract creation]"}
         |value: $value wei
         |payload: ${if (isContractCreation) "isContractCreation: " else "TransactionData: "}${khipu.toHexString(payload)}
         |}""".stripMargin
  }
}
