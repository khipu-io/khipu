package khipu.jsonrpc

import akka.util.ByteString
import khipu.DataWord
import khipu.domain.Address
import khipu.domain.Transaction

final case class TransactionRequest(
    from:     Address,
    to:       Option[Address]    = None,
    value:    Option[DataWord]   = None,
    gasLimit: Option[Long]       = None,
    gasPrice: Option[DataWord]   = None,
    nonce:    Option[DataWord]   = None,
    data:     Option[ByteString] = None
) {

  private def defaultGasPrice: DataWord = DataWord.Two * (DataWord.Ten pow 10)
  private val defaultGasLimit: Long = 90000

  def toTransaction(defaultNonce: DataWord): Transaction =
    Transaction(
      nonce = nonce.getOrElse(defaultNonce),
      gasPrice = gasPrice.getOrElse(defaultGasPrice),
      gasLimit = gasLimit.getOrElse(defaultGasLimit),
      receivingAddress = to,
      value = value.getOrElse(DataWord.Zero),
      payload = data.getOrElse(ByteString.empty)
    )
}
