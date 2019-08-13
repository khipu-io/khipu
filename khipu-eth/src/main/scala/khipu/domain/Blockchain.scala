package khipu.domain

import akka.util.ByteString
import khipu.Hash
import khipu.DataWord
import khipu.crypto
import khipu.ledger.TrieStorage
import khipu.ledger.BlockWorldState
import khipu.network.p2p.messages.PV62.BlockBody
import khipu.network.p2p.messages.PV63.MptNode
import khipu.network.p2p.messages.PV63.MptNode._
import khipu.store.Storages
import khipu.store.TransactionStorage
import khipu.store.TransactionStorage.TxLocation
import khipu.store.trienode.ReadOnlyNodeStorage
import khipu.trie
import khipu.trie.MerklePatriciaTrie
import khipu.vm.Storage
import khipu.vm.WorldState

object Blockchain {
  /**
   * Entity to be used to persist and query  Blockchain related objects (blocks, transactions, ommers)
   */
  trait I[S <: Storage[S], W <: WorldState[W, S]] {
    lazy val genesisHeader: BlockHeader = getBlockHeaderByNumber(0).get
    lazy val genesisBlock: Block = getBlockByNumber(0).get

    /**
     * Allows to query a blockHeader by block hash
     *
     * @param hash of the block that's being searched
     * @return [[BlockHeader]] if found
     */
    def getBlockHeaderByHash(hash: Hash): Option[BlockHeader]

    def getBlockHeaderByNumber(number: Long): Option[BlockHeader] = {
      for {
        hash <- getHashByBlockNumber(number)
        header <- getBlockHeaderByHash(hash)
      } yield header
    }

    /**
     * Allows to query a blockBody by block hash
     *
     * @param hash of the block that's being searched
     * @return [[khipu.network.p2p.messages.PV62.BlockBody]] if found
     */
    def getBlockBodyByHash(hash: Hash): Option[BlockBody]

    /**
     * Allows to query for a block based on it's hash
     *
     * @param hash of the block that's being searched
     * @return Block if found
     */
    def getBlockByHash(hash: Hash): Option[Block] =
      for {
        header <- getBlockHeaderByHash(hash)
        body <- getBlockBodyByHash(hash)
      } yield Block(header, body)

    /**
     * Allows to query for a block based on it's number
     *
     * @param number Block number
     * @return Block if it exists
     */
    def getBlockByNumber(number: Long): Option[Block] =
      for {
        hash <- getHashByBlockNumber(number)
        block <- getBlockByHash(hash)
      } yield block

    /**
     * Get an account for an address and a block number
     *
     * @param address address of the account
     * @param blockNumber the block that determines the state of the account
     */
    def getAccount(address: Address, blockNumber: Long): Option[Account]

    /**
     * Get account storage at given position
     *
     * @param rootHash storage root hash
     * @param position storage position
     */
    def getAccountStorageAt(rootHash: Hash, position: DataWord): ByteString

    /**
     * Returns the receipts based on a block hash
     * @param blockhash
     * @return Receipts if found
     */
    def getReceiptsByHash(blockhash: Hash): Option[Seq[Receipt]]
    def getReceiptsByNumber(blockNumber: Long): Option[Seq[Receipt]]

    /**
     * Returns EVM code searched by it's hash
     * @param hash Code Hash
     * @return EVM code if found
     */
    def getEvmcodeByHash(hash: Hash): Option[ByteString]

    /**
     * Returns MPT node searched by it's hash
     * @param hash Node Hash
     * @return MPT node
     */
    def getMptNodeByHash(hash: Hash): Option[MptNode]

    /**
     * Returns the total difficulty based on a block hash
     * @param blockhash
     * @return total difficulty if found
     */
    def getTotalDifficultyByHash(blockhash: Hash): Option[DataWord]

    def getTransactionLocation(txHash: Hash): Option[TxLocation]

    /**
     * Persists a block in the underlying Blockchain Database
     *
     * @param block Block to be saved
     */
    def saveBlock(block: Block): Unit = {
      saveBlockHeader(block.header)
      saveBlockBody(block.header.hash, block.body)
    }

    def removeBlock(hash: Hash): Unit

    def saveNewBlock(world: WorldState[_, _], block: Block, receipts: Seq[Receipt], totalDifficulty: DataWord): Unit

    /**
     * Persists a block header in the underlying Blockchain Database
     */
    def saveBlockHeader(blockHeader: BlockHeader): Unit
    def saveBlockBody(blockHash: Hash, blockBody: BlockBody): Unit
    def saveReceipts(blockHash: Hash, receipts: Seq[Receipt]): Unit
    def saveEvmcode(hash: Hash, evmCode: ByteString): Unit
    def saveTotalDifficulty(blockhash: Hash, totalDifficulty: DataWord): Unit

    /**
     * To get unconfirmed queue working, batched saving methods can only be used in fast sync
     */
    def saveBlockHeader_batched(blockHeaders: Iterable[BlockHeader]): Unit
    def saveBlockBody_batched(kvs: Iterable[(Hash, BlockBody)]): Unit
    def saveReceipts_batched(kvs: Iterable[(Hash, Seq[Receipt])]): Unit
    def saveEvmcode_batched(kvs: Iterable[(Hash, ByteString)]): Unit
    def saveTotalDifficulty_batched(kvs: Iterable[(Hash, DataWord)]): Unit

    /**
     * Returns a block hash given a block number
     *
     * @param number Number of the searchead block
     * @return Block hash if found
     */
    def getHashByBlockNumber(number: Long): Option[Hash]
    def getNumberByBlockHash(hash: Hash): Option[Long]

    def getWorldState(blockNumber: Long, accountStartNonce: DataWord, stateRootHash: Option[Hash] = None): W
    def getReadOnlyWorldState(blockNumber: Option[Long], accountStartNonce: DataWord, stateRootHash: Option[Hash] = None): W
  }

  def apply(storages: Storages): Blockchain = new Blockchain(storages)
}
final class Blockchain(val storages: Storages) extends Blockchain.I[TrieStorage, BlockWorldState] {

  private val accountNodeStorage = storages.accountNodeStorage
  private val storageNodeStorage = storages.storageNodeStorage
  private val evmcodeStorage = storages.evmcodeStorage

  private val blockHeaderStorage = storages.blockHeaderStorage
  private val blockBodyStorage = storages.blockBodyStorage
  private val receiptsStorage = storages.receiptsStorage
  private val blockNumberStorage = storages.blockNumberStorage

  private val totalDifficultyStorage = storages.totalDifficultyStorage
  private val transactionStorage = storages.transactionStorage

  private val appStateStorage = storages.appStateStorage

  private val blockNumbers = storages.blockNumbers

  def getHashByBlockNumber(number: Long): Option[Hash] =
    blockNumbers.getHashByBlockNumber(number)

  def getNumberByBlockHash(blockHash: Hash): Option[Long] =
    blockNumbers.get(blockHash)

  def getBlockHeaderByHash(blockHash: Hash): Option[BlockHeader] =
    blockNumbers.get(blockHash) flatMap blockHeaderStorage.get

  def getBlockBodyByHash(blockHash: Hash): Option[BlockBody] =
    blockNumbers.get(blockHash) flatMap blockBodyStorage.get

  def getReceiptsByHash(blockHash: Hash): Option[Seq[Receipt]] =
    blockNumbers.get(blockHash) flatMap receiptsStorage.get

  def getReceiptsByNumber(blockNumber: Long): Option[Seq[Receipt]] =
    receiptsStorage.get(blockNumber)

  def getEvmcodeByHash(hash: Hash): Option[ByteString] =
    evmcodeStorage.get(hash).map(ByteString(_))

  def getTotalDifficultyByHash(hash: Hash): Option[DataWord] =
    blockNumbers.get(hash) flatMap totalDifficultyStorage.get

  def saveBlockHeader(blockHeader: BlockHeader) {
    blockHeaderStorage.put(blockHeader.number, blockHeader)
    blockNumberStorage.put(blockHeader.hash, blockHeader.number)
    blockNumbers.put(blockHeader.hash, blockHeader.number)
  }

  def saveBlockHeader_batched(blockHeaders: Iterable[BlockHeader]) {
    val kvs = blockHeaders.map(x => x.number -> x)
    blockHeaderStorage.update(Nil, kvs)
    val nums = blockHeaders.map(x => x.hash -> x.number)
    blockNumberStorage.update(Nil, nums)
    nums foreach { case (hash, number) => blockNumbers.put(hash, number) }
  }

  def saveBlockBody(blockHash: Hash, blockBody: BlockBody) = {
    blockNumbers.get(blockHash) foreach { blockNumber =>
      blockBodyStorage.put(blockNumber, blockBody)
      saveTxsLocations(blockNumber, blockBody)
    }
  }

  def saveBlockBody_batched(kvs: Iterable[(Hash, BlockBody)]) = {
    val toSave = kvs flatMap {
      case (hash, body) =>
        blockNumbers.get(hash) map {
          blockNumber => (blockNumber -> body)
        }
    }
    blockBodyStorage.update(Nil, toSave)
    toSave foreach {
      case (blockNumber, blockBody) => saveTxsLocations(blockNumber, blockBody)
    }
  }

  def saveReceipts(blockHash: Hash, receipts: Seq[Receipt]) =
    blockNumbers.get(blockHash) foreach { blockNumber =>
      receiptsStorage.put(blockNumber, receipts)
    }

  def saveReceipts_batched(kvs: Iterable[(Hash, Seq[Receipt])]) {
    val toSave = kvs flatMap {
      case (hash, receipts) =>
        blockNumbers.get(hash) map {
          blockNumber => (blockNumber -> receipts)
        }
    }
    receiptsStorage.update(Nil, toSave)
  }

  def saveEvmcode(hash: Hash, evmCode: ByteString) =
    evmcodeStorage.put(hash, evmCode.toArray)

  def saveEvmcode_batched(kvs: Iterable[(Hash, ByteString)]) =
    evmcodeStorage.update(Nil, kvs.map(x => x._1 -> x._2.toArray))

  def saveTotalDifficulty(blockHash: Hash, td: DataWord) =
    blockNumbers.get(blockHash) foreach { blockNumber =>
      totalDifficultyStorage.put(blockNumber, td)
    }

  def saveTotalDifficulty_batched(kvs: Iterable[(Hash, DataWord)]) {
    val toSave = kvs flatMap {
      case (hash, td) =>
        blockNumbers.get(hash) map {
          blockNumber => (blockNumber -> td)
        }
    }
    totalDifficultyStorage.update(Nil, toSave)
  }

  private def saveWorld(world: WorldState[_, _]) {
    world.persist()
  }

  private def saveBestBlockNumber(n: Long) {
    appStateStorage.putBestBlockNumber(n)
  }

  private def saveTxsLocations(blockNumber: Long, blockBody: BlockBody) {
    val kvs = blockBody.transactionList.zipWithIndex map {
      case (tx, index) => (tx.hash, TxLocation(blockNumber, index))
    }
    transactionStorage.update(Nil, kvs)
  }

  private def removeTxsLocations(stxs: Seq[SignedTransaction]) {
    stxs.map(_.hash).foreach(transactionStorage.remove)
  }

  def getWorldState(blockNumber: Long, accountStartNonce: DataWord, stateRootHash: Option[Hash]): BlockWorldState =
    BlockWorldState(
      this,
      accountNodeStorage,
      storageNodeStorage,
      evmcodeStorage,
      accountStartNonce,
      stateRootHash
    )

  //FIXME Maybe we can use this one in regular execution too and persist underlying storage when block execution is successful
  def getReadOnlyWorldState(blockNumber: Option[Long], accountStartNonce: DataWord, stateRootHash: Option[Hash]): BlockWorldState =
    BlockWorldState(
      this,
      ReadOnlyNodeStorage(accountNodeStorage),
      ReadOnlyNodeStorage(storageNodeStorage),
      evmcodeStorage,
      accountStartNonce,
      stateRootHash
    )

  def removeBlock(blockHash: Hash) {
    blockNumbers.get(blockHash) foreach { blockNumber =>
      blockHeaderStorage.remove(blockNumber)
      blockBodyStorage.remove(blockNumber)
      totalDifficultyStorage.remove(blockNumber)
      receiptsStorage.remove(blockNumber)
      blockNumbers.remove(blockHash)
      getBlockBodyByHash(blockHash).map(_.transactionList).foreach(removeTxsLocations)
    }
  }

  /**
   * API for outside query. Account can only be quered via MPT trie
   */
  def getAccount(address: Address, blockNumber: Long): Option[Account] =
    getBlockHeaderByNumber(blockNumber).flatMap { blockheader =>
      MerklePatriciaTrie[Address, Account](
        blockheader.stateRoot.bytes,
        accountNodeStorage
      )(Address.hashedAddressEncoder, Account.accountSerializer).get(address)
    }

  /**
   * API for outside query. Account storage can only be quered via MPT trie
   */
  def getAccountStorageAt(accountStateRootHash: Hash, position: DataWord): ByteString = {
    val storage = MerklePatriciaTrie(
      accountStateRootHash.bytes,
      storageNodeStorage
    )(trie.hashDataWordSerializable, trie.rlpDataWordSerializer).get(position).getOrElse(DataWord.Zero).bytes

    ByteString(storage)
  }

  def getMptNodeByHash(hash: Hash): Option[MptNode] =
    (accountNodeStorage.get(hash) orElse storageNodeStorage.get(hash)).map(_.toMptNode)

  def getTransactionLocation(txHash: Hash): Option[TransactionStorage.TxLocation] =
    transactionStorage.get(txHash)

  def saveNewBlock(world: WorldState[_, _], block: Block, receipts: Seq[Receipt], totalDifficulty: DataWord) {
    // TODO save in one transaction
    saveWorld(world)
    saveBlock(block)
    saveReceipts(block.header.hash, receipts)
    saveTotalDifficulty(block.header.hash, totalDifficulty)
    saveBestBlockNumber(block.header.number)
  }

  def swithToWithUnconfirmed() {
    storages.swithToWithUnconfirmed()
  }

  def clearUnconfirmed() {
    storages.clearUnconfirmed()
  }
}

