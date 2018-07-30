package khipu.keystore

import akka.util.ByteString
import khipu.crypto
import khipu.domain.{ Address, SignedTransaction, Transaction }

final case class Wallet(address: Address, prvKey: ByteString) {
  lazy val keyPair = crypto.keyPairFromPrvKey(prvKey.toArray)

  def signTx(tx: Transaction, chainId: Option[Int]): SignedTransaction = SignedTransaction.sign(tx, keyPair, chainId)
}
