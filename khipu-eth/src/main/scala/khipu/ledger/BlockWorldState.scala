package khipu.ledger

import akka.util.ByteString
import khipu.Deleted
import khipu.Hash
import khipu.Original
import khipu.Updated
import khipu.UInt256
import khipu.crypto
import khipu.domain.Account
import khipu.domain.Address
import khipu.domain.Blockchain
import khipu.domain.SignedTransaction
import khipu.store.trienode.NodeKeyValueStorage
import khipu.trie
import khipu.trie.MerklePatriciaTrie
import khipu.vm.WorldState

/**
 * == Tries in Ethereum ==
 * All of the merkle tries in Ethereum use a Merkle Patricia Trie.
 *
 * From a block header there are 3 roots from 3 of these tries.
 *
 * 1. stateRoot
 * 2. transactionsRoot
 * 3. receiptsRoot
 *
 * === State Trie ===
 * There is one global state trie, and it updates over time. In it, a `path` is
 * always: `sha3(ethereumAddress)` and a `value` is always: `rlp(ethereumAccount)`.
 * More specifically an ethereum `account` is a 4 item array of `[nonce,balance,storageRoot,codeHash]`.
 * At this point it's worth noting that this `storageRoot` is the root of another patricia trie:
 *
 * === Storage Trie ===
 * Storage trie is where '''all''' contract data lives. There is a separate
 * storage trie for each account. A `path` in this trie is somewhat complex but
 * they depend on [[https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getstorageat]].
 *
 * === Transactions Trie ===
 * There is a separate transactions trie for every block. A `path` here is: `rlp(transactionIndex)`.
 * `transactionIndex` is its index within the block it's mined. The ordering is
 * mostly decided by a miner so this data is unknown until mined. After a block
 * is mined, the transaction trie never updates.
 *
 * === Receipts Trie ===
 * Every block has its own Receipts trie. A `path` here is: `rlp(transactionIndex)`.
 * `transactionIndex` is its index within the block it's mined. Never updates.
 */
object BlockWorldState {

  sealed trait RaceCondition
  case object OnAddress extends RaceCondition
  case object OnAccount extends RaceCondition
  case object OnStorage extends RaceCondition
  case object OnCode extends RaceCondition

  object AccountDelta {
    def unapply(x: AccountDelta): Option[(UInt256, UInt256, Hash, Hash)] =
      Some(x.nonce, x.balance, x.stateRoot, x.codeHash)
  }
  final class AccountDelta {
    private var _nonce = UInt256.Zero
    private var _balance = UInt256.Zero
    private var _stateRoot = Account.EMPTY_STATE_ROOT_HASH
    private var _codeHash = Account.EMPTY_CODE_HASH

    def nonce = _nonce
    def balance = _balance
    def stateRoot = _stateRoot
    def codeHash = _codeHash

    def increaseNonce(nonce: UInt256) = {
      this._nonce += nonce
      this
    }

    def increaseBalance(balance: UInt256) = {
      this._balance += balance
      this
    }

    def withStateRoot(stateRoot: Hash) = {
      _stateRoot = stateRoot
      this
    }

    def withCodeHash(codeHash: Hash) = {
      _codeHash = codeHash
      this
    }

    override def toString = s"AccountDelta($nonce, $balance, $stateRoot, $codeHash)"
  }

  def apply(
    blockchain:         Blockchain,
    accountNodeStorage: NodeKeyValueStorage,
    storageNodeStorage: NodeKeyValueStorage,
    evmCodeStorage:     NodeKeyValueStorage,
    accountStartNonce:  UInt256,
    stateRootHash:      Option[Hash]        = None
  ): BlockWorldState = {

    /**
     * Returns an accounts state trie "The world state (state), is a mapping
     * between Keccak 256-bit hashes of the addresses (160-bit identifiers) and account states
     * (a data structure serialised as RLP [...]).
     * Though not stored on the blockchain, it is assumed that the implementation will maintain this mapping in a
     * modified Merkle Patricia tree [...])."
     *
     * See [[http://paper.gavwood.com YP 4.1]]
     */
    val underlyingAccountsTrie = MerklePatriciaTrie[Address, Account](
      stateRootHash.getOrElse(Hash(trie.EMPTY_TRIE_HASH)).bytes,
      accountNodeStorage
    )(Address.hashedAddressEncoder, Account.accountSerializer)

    new BlockWorldState(
      blockchain,
      accountNodeStorage,
      storageNodeStorage,
      evmCodeStorage,
      accountStartNonce,
      TrieAccounts(underlyingAccountsTrie),
      Map(),
      Map(),
      Map(),
      Map(),
      Set(),
      None
    )
  }
}

/**
 * stateStorage
 * trieAccounts
 *   State MPT nodes storage needed to construct the storage MPT when calling [[getStorage]].
 *   Accounts state and accounts storage states are saved within the same storage
 *
 * addressToTrieStorage
 *   Contract Storage by Address
 *
 * addressToCode
 *   It's easier to use the storage instead of the blockchain here . We might need to reconsider this
 *   Account's code by Address
 *
 * During vm(tx) excecution, only address' account/contractStorage/code may be changed.
 */
final class BlockWorldState private (
    blockchain:                   Blockchain,
    accountNodeStorage:           NodeKeyValueStorage,
    storageNodeStorage:           NodeKeyValueStorage,
    evmCodeStorage:               NodeKeyValueStorage,
    accountStartNonce:            UInt256,
    private var trieAccounts:     TrieAccounts,
    private var trieStorages:     Map[Address, TrieStorage],
    private var codes:            Map[Address, ByteString],
    private var accountDeltas:    Map[Address, Vector[BlockWorldState.AccountDelta]],
    private var raceConditions:   Map[BlockWorldState.RaceCondition, Set[Address]],
    private var touchedAddresses: Set[Address], // for debug
    private var stx:              Option[SignedTransaction] // for debug
) extends WorldState[BlockWorldState, TrieStorage] {
  import BlockWorldState._

  /**
   * Returns world state root hash. This value is only updated/accessing after committed.
   */
  def rootHash: Hash = Hash(trieAccounts.rootHash)

  def getBlockHash(number: Long): Option[UInt256] = blockchain.getHashByBlockNumber(number).map(UInt256(_))

  def emptyAccount: Account = Account.empty(accountStartNonce)

  def getAccount(address: Address): Option[Account] = trieAccounts.get(address)

  def saveAccount(address: Address, account: Account): BlockWorldState = {
    // accountDelta is not used by far, should we improve account increamental update?
    accountDeltas += (address -> (accountDeltas.getOrElse(address, Vector()) :+ toAccountDelta(address, account)))

    // should be added to trieAccounts after account delta calculated since toAccountDelta() is upon the original account
    trieAccounts += (address -> account)

    touchedAddresses += address

    // raceConditions on accounts have been considered during VM run, so do not need to addRaceCondition(OnAccount, address) here

    this
  }

  private def toAccountDelta(address: Address, updatedAccount: Account): AccountDelta = {
    val Account(nonce, balance, stateRoot, codeHash) = getAccount(address).getOrElse(emptyAccount)
    val nonceDelta = updatedAccount.nonce - nonce
    val balanceDelta = updatedAccount.balance - balance
    new AccountDelta().increaseNonce(nonceDelta).increaseBalance(balanceDelta).withStateRoot(stateRoot).withCodeHash(codeHash)
  }

  def deleteAccount(address: Address): BlockWorldState = {
    trieAccounts -= address
    trieStorages -= address
    codes -= address

    touchedAddresses += address

    addRaceCondition(OnAddress, address)
    this
  }

  /**
   * Returns a trie based contract storage defined as "trie as a map-ping from the Keccak
   * 256-bit hash of the 256-bit integer keys to the RLP-encoded256-bit integer values."
   * See [[http://paper.gavwood.com YP 4.1]]
   *
   * @param address     we'll get the stateRoot via account
   * @param contractStorage Storage where trie nodes are saved
   * @return Contract Storage Trie
   */
  def getStorage(address: Address): TrieStorage = {
    trieStorages.get(address) match {
      case Some(x) => x
      case None =>
        val trieStorage = newTrieStorage(address)
        trieStorages += (address -> trieStorage)
        trieStorage
    }
  }

  private def newTrieStorage(address: Address) = {
    val underlyingTrie = MerklePatriciaTrie(
      getStateRoot(address).bytes,
      storageNodeStorage
    )(trie.hashUInt256Serializable, trie.rlpUInt256Serializer)

    TrieStorage(underlyingTrie)
  }

  private def getStateRoot(address: Address): Hash = {
    getAccount(address).map(_.stateRoot).getOrElse(Account.EMPTY_STATE_ROOT_HASH)
  }

  def saveStorage(address: Address, storage: TrieStorage): BlockWorldState = {
    val flushedStorage = storage.flush()
    trieStorages += (address -> flushedStorage)
    addRaceCondition(OnStorage, address)

    trieAccounts += (address -> getGuaranteedAccount(address).withStateRoot(stateRoot = Hash(flushedStorage.underlying.rootHash)))
    addRaceCondition(OnAccount, address)

    this
  }

  def getCode(address: Address): ByteString = {
    codes.get(address) match {
      case Some(x) => x
      case None =>
        val code = getCodeFromEvmCodeStorage(address)
        codes += (address -> code)
        code
    }
  }

  private def getCodeFromEvmCodeStorage(address: Address): ByteString = {
    getAccount(address).map(_.codeHash).flatMap(x => evmCodeStorage.get(x).map(ByteString(_))).getOrElse(ByteString())
  }

  def saveCode(address: Address, code: ByteString): BlockWorldState = {
    codes += (address -> code)
    addRaceCondition(OnCode, address)

    trieAccounts += (address -> getGuaranteedAccount(address).withCodeHash(codeHash = Hash(crypto.kec256(code))))
    addRaceCondition(OnAccount, address)

    this
  }

  def getCodeHash(address: Address): Option[UInt256] = {
    getAccount(address).map(_.codeHash).map(UInt256(_))
  }

  private def addRaceCondition(modified: RaceCondition, address: Address) {
    raceConditions += (modified -> (raceConditions.getOrElse(modified, Set()) + address))
  }

  /**
   * Used for debug
   */
  def withTx(stx: Option[SignedTransaction]) = {
    this.stx = stx
    this
  }

  /**
   * Updates state trie with current changes but does not persist them into the storages. To do so it:
   *   - Flush code (to get account's code hashes)
   *   - Flush constract storages (to get account's contract storage root)
   *   - Updates state tree
   *
   * @param worldState to flush
   * @return Updated world
   */
  private[ledger] def flush(): BlockWorldState = {
    trieAccounts = trieAccounts.flush()
    this
  }

  /**
   * Should be called adter committed
   */
  def persist(): BlockWorldState = {
    // deduplicate codes first
    this.codes.foldLeft(Map[Hash, ByteString]()) {
      case (acc, (address, code)) => acc + (Hash(crypto.kec256(code)) -> code)
    } foreach {
      case (hash, code) => evmCodeStorage.put(hash, code.toArray)
    }

    this.trieStorages.foreach {
      case (address, storageTrie) => storageTrie.underlying.persist()
    }

    this.trieAccounts.underlying.persist()

    this
  }

  // --- merge ---

  def mergeRaceConditions(later: BlockWorldState): BlockWorldState = {
    later.raceConditions foreach {
      case (k, vs) => this.raceConditions += (k -> (this.raceConditions.getOrElse(k, Set()) ++ vs))
    }
    this
  }

  private[ledger] def merge(later: BlockWorldState): Either[Map[RaceCondition, Set[Address]], BlockWorldState] = {
    val raceCondiftions = this.raceConditions.foldLeft(Map[RaceCondition, Set[Address]]()) {
      case (acc, (OnAccount, addresses)) => acc + (OnAccount -> addresses.filter(later.trieAccounts.logs.contains))
      case (acc, (OnStorage, addresses)) => acc + (OnStorage -> addresses.filter(later.trieStorages.contains))
      case (acc, (OnCode, addresses))    => acc + (OnCode -> addresses.filter(later.codes.contains))
      case (acc, (OnAddress, addresses)) => acc + (OnAddress -> addresses.filter(x => later.codes.contains(x) || later.trieStorages.contains(x) || later.trieAccounts.logs.contains(x)))
    } filter (_._2.nonEmpty)

    if (raceCondiftions.isEmpty) {
      val toMerge = this.copy
      toMerge.touchedAddresses ++= later.touchedAddresses
      //mergeAccountTrieAccount_simple(toMerge, that)
      toMerge.mergeAccountTrieAccount(later).mergeTrieStorage(later).mergeCode(later).mergeRaceConditions(later)
      Right(toMerge)
    } else {
      Left(raceCondiftions)
    }
  }

  /** mergeAccountTrieAccount should work now, mergeAccountTrieAccount_simple is left here for reference only */
  private def mergeAccountTrieAccount_simple(later: BlockWorldState): BlockWorldState = {
    this.trieAccounts.logs ++= later.trieAccounts.logs
    this
  }

  private def mergeAccountTrieAccount(later: BlockWorldState): BlockWorldState = {
    val alreadyMergedAddresses = later.accountDeltas map {
      case (address, deltas) =>
        val valueMerged = deltas.foldLeft(this.getAccount(address).getOrElse(this.emptyAccount)) {
          case (acc, AccountDelta(nonce, balance, _, _)) => acc.increaseNonce(nonce).increaseBalance(balance)
        }

        // just put the lasted stateRoot and codeHash of y and merge delete
        later.trieAccounts.logs.get(address).map {
          case Updated(Account(_, _, stateRoot, codeHash)) => this.trieAccounts += (address -> valueMerged.withStateRoot(stateRoot).withCodeHash(codeHash))
          case Original(_)                                 => this.trieAccounts += (address -> valueMerged)
          case Deleted(_)                                  => this.trieAccounts -= address
        }

        address
    } toSet

    this.trieAccounts.logs ++= (later.trieAccounts.logs -- alreadyMergedAddresses)
    this
  }

  private def mergeTrieStorage(later: BlockWorldState): BlockWorldState = {
    this.trieStorages ++= later.trieStorages
    this
  }

  private def mergeCode(later: BlockWorldState): BlockWorldState = {
    this.codes ++= later.codes
    this
  }

  private[ledger] def touchedAccounts = touchedAddresses.map(x => x -> getAccount(x).getOrElse(null)).toMap

  def copy = new BlockWorldState(
    blockchain,
    accountNodeStorage,
    storageNodeStorage,
    evmCodeStorage,
    accountStartNonce,
    trieAccounts,
    trieStorages,
    codes,
    accountDeltas,
    raceConditions,
    touchedAddresses,
    stx
  )
}

