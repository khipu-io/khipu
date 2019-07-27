package khipu.jsonrpc

import akka.util.ByteString
import khipu.EvmWord
import khipu.domain.Address
import khipu.domain.Transaction

final case class TransactionRequest(
    from:     Address,
    to:       Option[Address]    = None,
    value:    Option[EvmWord]    = None,
    gasLimit: Option[Long]       = None,
    gasPrice: Option[EvmWord]    = None,
    nonce:    Option[EvmWord]    = None,
    data:     Option[ByteString] = None
) {

  private def defaultGasPrice: EvmWord = EvmWord.Two * (EvmWord.Ten pow 10)
  private val defaultGasLimit: Long = 90000

  def toTransaction(defaultNonce: EvmWord): Transaction =
    Transaction(
      nonce = nonce.getOrElse(defaultNonce),
      gasPrice = gasPrice.getOrElse(defaultGasPrice),
      gasLimit = gasLimit.getOrElse(defaultGasLimit),
      receivingAddress = to,
      value = value.getOrElse(EvmWord.Zero),
      payload = data.getOrElse(ByteString.empty)
    )
}
