package khipu.domain

import akka.util.ByteString
import khipu.EvmWord

object Transaction {

  val NonceLength = 32
  val GasLength = 32
  val ValueLength = 32

  def apply(nonce: EvmWord, gasPrice: EvmWord, gasLimit: Long, receivingAddress: Address, value: EvmWord, payload: ByteString): Transaction =
    Transaction(nonce, gasPrice, gasLimit, Some(receivingAddress), value, payload)
}

final case class Transaction(
    nonce:            EvmWord,
    gasPrice:         EvmWord,
    gasLimit:         Long,
    receivingAddress: Option[Address],
    value:            EvmWord,
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
         |payload: ${if (isContractCreation) "Program: " else "Input: "}${khipu.toHexString(payload)}
         |}""".stripMargin
  }
}
