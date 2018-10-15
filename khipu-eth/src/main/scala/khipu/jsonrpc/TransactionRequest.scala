package khipu.jsonrpc

import akka.util.ByteString
import khipu.UInt256
import khipu.domain.Address
import khipu.domain.Transaction

final case class TransactionRequest(
    from:     Address,
    to:       Option[Address]    = None,
    value:    Option[UInt256]    = None,
    gasLimit: Option[Long]       = None,
    gasPrice: Option[UInt256]    = None,
    nonce:    Option[UInt256]    = None,
    data:     Option[ByteString] = None
) {

  private def defaultGasPrice: UInt256 = UInt256.Two * (UInt256.Ten pow 10)
  private val defaultGasLimit: Long = 90000

  def toTransaction(defaultNonce: UInt256): Transaction =
    Transaction(
      nonce = nonce.getOrElse(defaultNonce),
      gasPrice = gasPrice.getOrElse(defaultGasPrice),
      gasLimit = gasLimit.getOrElse(defaultGasLimit),
      receivingAddress = to,
      value = value.getOrElse(UInt256.Zero),
      payload = data.getOrElse(ByteString.empty)
    )
}
