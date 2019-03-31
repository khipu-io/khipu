package khipu.vm

import akka.util.ByteString
import khipu.UInt256
import khipu.crypto
import khipu.domain.Account
import khipu.domain.Address
import khipu.rlp
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPList

/**
 * This is a single entry point to all VM interactions with the persisted state. Implementations are meant to be
 * immutable so that rolling back a transaction is equivalent to discarding resulting changes. The changes to state
 * should be kept in memory and applied only after a transaction completes without errors. This does not forbid mutable
 * caches for DB retrieval operations.
 */
object WorldState {
  final case class StateException(message: String) extends RuntimeException(message)
}
trait WorldState[W <: WorldState[W, S], S <: Storage[S]] { self: W =>
  import WorldState._

  def getAccount(address: Address): Option[Account]
  def saveAccount(address: Address, account: Account): W
  def deleteAccount(address: Address): W
  def emptyAccount: Account

  /**
   * In certain situation an account is guaranteed to exist, e.g. the account that executes the code, the account that
   * transfer value to another. There could be no input to our application that would cause this fail, so we shouldn't
   * handle account existence in such cases. If it does fail, it means there's something terribly wrong with our code
   * and throwing an exception is an appropriate response.
   */
  def getGuaranteedAccount(address: Address): Account = {
    getAccount(address) getOrElse {
      throw StateException(s"Account not found ${address}, state is inconsistent")
    }
  }

  def getCode(address: Address): ByteString
  def getStorage(address: Address): S
  def getBlockHash(number: Long): Option[UInt256]
  def getCodeHash(address: Address): Option[UInt256]

  def saveCode(address: Address, code: ByteString): W
  def saveStorage(address: Address, storage: S): W

  def newEmptyAccount(address: Address): W =
    saveAccount(address, emptyAccount)

  def isAccountExist(address: Address): Boolean =
    getAccount(address).isDefined

  def isAccountDead(address: Address): Boolean =
    getAccount(address).map(_.isEmpty).getOrElse(true)

  def getBalance(address: Address): UInt256 =
    getAccount(address).map(a => a.balance).getOrElse(UInt256.Zero)

  def transfer(from: Address, to: Address, value: UInt256): W = {
    if (from == to) {
      this
    } else {
      val debited = getGuaranteedAccount(from).increaseBalance(-value)
      val credited = getAccount(to).getOrElse(emptyAccount).increaseBalance(value)
      saveAccount(from, debited).saveAccount(to, credited)
    }
  }

  def pay(address: Address, value: UInt256): W = {
    val account = getAccount(address).getOrElse(emptyAccount).increaseBalance(value)
    saveAccount(address, account)
  }

  def withdraw(address: Address, value: UInt256): W = {
    val account = getAccount(address).getOrElse(emptyAccount).increaseBalance(-value)
    saveAccount(address, account)
  }

  def increaseNonce(address: Address): W = {
    val account = getAccount(address).getOrElse(emptyAccount)
    saveAccount(address, account.increaseNonce())
  }

  /**
   * Creates a new address based on the address and nonce of the creator. YP equation 82
   *
   * @param creatorAddr, the address of the creator of the new address
   * @return the new address
   */
  def createAddress(creatorAddr: Address): Address = {
    val creatorAccount = getGuaranteedAccount(creatorAddr)
    val hash = crypto.kec256(rlp.encode(RLPList(creatorAddr.bytes, rlp.toRLPEncodable(creatorAccount.nonce - UInt256.One))))
    Address(hash)
  }

  /**
   * Increases the creator's nonce and creates a new address based on the address and the new nonce of the creator
   *
   * @param creatorAddr, the address of the creator of the new address
   * @return the new address and the state world after the creator's nonce was increased
   */
  def createAddressByOpCode(creatorAddr: Address): (Address, W) = {
    val creatorAccount = getGuaranteedAccount(creatorAddr)
    val world1 = saveAccount(creatorAddr, creatorAccount.increaseNonce())
    (world1.createAddress(creatorAddr), world1)
  }

  def mergeRaceConditions(that: W): W

  def copy: W

  def persist(): W
}
