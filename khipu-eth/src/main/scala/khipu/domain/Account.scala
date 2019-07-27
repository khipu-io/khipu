package khipu.domain

import akka.util.ByteString
import khipu.Hash
import khipu.EvmWord
import khipu.crypto.kec256
import khipu.network.p2p.messages.PV63.AccountImplicits
import khipu.rlp
import khipu.rlp.RLPImplicits._
import khipu.trie.ByteArraySerializable

object Account {
  // 56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421
  val EMPTY_STATE_ROOT_HASH = Hash(kec256(rlp.encode(Array.emptyByteArray)))

  // c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470
  val EMPTY_CODE_HASH = Hash(kec256(ByteString()))

  def empty(startNonce: EvmWord) = Account(nonce = startNonce)

  val accountSerializer = new ByteArraySerializable[Account] {
    import AccountImplicits._

    override def fromBytes(bytes: Array[Byte]): Account = bytes.toAccount
    override def toBytes(input: Account): Array[Byte] = input.toBytes
  }
}

/**
 * nonce A value equal to the number of transactions sent
 *     from this address, or, in the case of contract accounts,
 *     the number of contract-creations made by this account
 *
 * balance A scalar value equal to the number of Wei owned by this address
 *
 * stateRoot A 256-bit hash of the root node of a trie structure
 *     that encodes the storage contents of the contract,
 *     itself a simple mapping between byte arrays of size 32.
 *     The hash is formally denoted σ[a] s .
 *
 *     Since I typically wish to refer not to the trie’s root hash
 *     but to the underlying set of key/value pairs stored within,
 *     I define a convenient equivalence TRIE (σ[a] s ) ≡ σ[a] s .
 *     It shall be understood that σ[a] s is not a ‘physical’ member
 *     of the account and does not contribute to its later serialisation
 *
 * codeHash The hash of the EVM code of this contract—this is the code
 *     that gets executed should this address receive a message call;
 *     it is immutable and thus, unlike all other fields, cannot be changed
 *     after construction. All such code fragments are contained in
 *     the state database under their corresponding hashes for later
 *     retrieval
 *
 */
final case class Account(
    nonce:     EvmWord,
    balance:   EvmWord = EvmWord.Zero,
    stateRoot: Hash    = Account.EMPTY_STATE_ROOT_HASH,
    codeHash:  Hash    = Account.EMPTY_CODE_HASH
) {

  def increaseNonce(value: EvmWord = EvmWord.One): Account =
    copy(nonce = nonce + value)

  def increaseBalance(value: EvmWord): Account =
    copy(balance = balance + value)

  def withCodeHash(codeHash: Hash): Account =
    copy(codeHash = codeHash)

  def withStateRoot(stateRoot: Hash): Account =
    copy(stateRoot = stateRoot)

  def isEmpty =
    codeHash == Account.EMPTY_CODE_HASH && nonce.isZero && balance.isZero

  override def toString: String = s"Account(nonce: $nonce, balance: $balance, stateRoot: ${stateRoot.hexString}, codeHash: ${codeHash.hexString})"

}
