package khipu.jsonrpc

import akka.util.ByteString
import java.math.BigInteger
import khipu.domain.{ Address, Transaction }

final case class TransactionRequest(
    from:     Address,
    to:       Option[Address]    = None,
    value:    Option[BigInteger] = None,
    gasLimit: Option[Long]       = None,
    gasPrice: Option[BigInteger] = None,
    nonce:    Option[BigInteger] = None,
    data:     Option[ByteString] = None
) {

  private val defaultGasPrice: BigInteger = BigInteger.valueOf(2) multiply (BigInteger.valueOf(10) pow 10)
  private val defaultGasLimit: Long = 90000

  def toTransaction(defaultNonce: BigInteger): Transaction =
    Transaction(
      nonce = nonce.getOrElse(defaultNonce),
      gasPrice = gasPrice.getOrElse(defaultGasPrice),
      gasLimit = gasLimit.getOrElse(defaultGasLimit),
      receivingAddress = to,
      value = value.getOrElse(BigInteger.ZERO),
      payload = data.getOrElse(ByteString.empty)
    )
}
