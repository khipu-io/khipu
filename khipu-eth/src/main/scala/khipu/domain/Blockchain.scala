package khipu.domain

import akka.util.ByteString
import java.math.BigInteger
import khipu.Hash
import khipu.crypto
import khipu.ledger.TrieStorage
import khipu.ledger.BlockWorldState
import khipu.network.p2p.messages.PV62.BlockBody
import khipu.network.p2p.messages.PV63.MptNode
import khipu.network.p2p.messages.PV63.MptNode._
import khipu.store.BlockchainStorages
import khipu.store.EvmCodeStorage
import khipu.store.TransactionMappingStorage
import khipu.store.TransactionMappingStorage.TransactionLocation
import khipu.store.trienode.ReadOnlyNodeStorage
import khipu.trie
import khipu.trie.MerklePatriciaTrie
import khipu.vm.{ Storage, UInt256, WorldState }

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
    def getAccountStorageAt(rootHash: Hash, position: BigInteger): ByteString

    /**
     * Returns the receipts based on a block hash
     * @param blockhash
     * @return Receipts if found
     */
    def getReceiptsByHash(blockhash: Hash): Option[Seq[Receipt]]

    /**
     * Returns EVM code searched by it's hash
     * @param hash Code Hash
     * @return EVM code if found
     */
    def getEvmCodeByHash(hash: Hash): Option[ByteString]

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
    def getTotalDifficultyByHash(blockhash: Hash): Option[BigInteger]

    def getTransactionLocation(txHash: Hash): Option[TransactionLocation]

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

    /**
     * Persists a block header in the underlying Blockchain Database
     *
     * @param blockHeader Block to be saved
     */
    def saveBlockHeader(blockHeader: BlockHeader): Unit
    def saveBlockBody(blockHash: Hash, blockBody: BlockBody): Unit
    def saveReceipts(blockHash: Hash, receipts: Seq[Receipt]): Unit
    def saveEvmcode(hash: Hash, evmCode: ByteString): Unit
    def saveTotalDifficulty(blockhash: Hash, totalDifficulty: BigInteger): Unit

    /**
     * Returns a block hash given a block number
     *
     * @param number Number of the searchead block
     * @return Block hash if found
     */
    def getHashByBlockNumber(number: Long): Option[Hash]

    def getWorldState(blockNumber: Long, accountStartNonce: UInt256, stateRootHash: Option[Hash] = None): W
    def getReadOnlyWorldState(blockNumber: Option[Long], accountStartNonce: UInt256, stateRootHash: Option[Hash] = None): W

    def evmCodeStorage: EvmCodeStorage
  }

  def apply(storages: BlockchainStorages): Blockchain =
    new Blockchain(storages)
}
final class Blockchain(val storages: BlockchainStorages) extends Blockchain.I[TrieStorage, BlockWorldState] {

  private val blockHeaderStorage = storages.blockHeaderStorage
  private val blockBodyStorage = storages.blockBodyStorage
  private val receiptsStorage = storages.receiptsStorage
  private val accountNodeStorageFor = storages.accountNodeStorageFor
  private val storageNodeStorageFor = storages.storageNodeStorageFor
  private val totalDifficultyStorage = storages.totalDifficultyStorage
  private val transactionMappingStorage = storages.transactionMappingStorage
  //private val blockNumberMappingStorage = storages.blockNumberMappingStorage

  val evmCodeStorage = storages.evmCodeStorage

  def getHashByBlockNumber(number: Long): Option[Hash] =
    blockHeaderStorage.getBlockHash(number)

  def getBlockHeaderByHash(hash: Hash): Option[BlockHeader] =
    blockHeaderStorage.get(hash)

  def getBlockBodyByHash(hash: Hash): Option[BlockBody] =
    blockBodyStorage.get(hash)

  def getReceiptsByHash(blockhash: Hash): Option[Seq[Receipt]] =
    receiptsStorage.get(blockhash)

  def getEvmCodeByHash(hash: Hash): Option[ByteString] =
    evmCodeStorage.get(hash)

  def getTotalDifficultyByHash(blockhash: Hash): Option[BigInteger] =
    totalDifficultyStorage.get(blockhash)

  def saveBlockHeader(blockHeader: BlockHeader) {
    val hash = blockHeader.hash
    blockHeaderStorage.setWritingBlockNumber(blockHeader.number)
    blockHeaderStorage.put(hash, blockHeader)
    saveBlockNumberMapping(blockHeader.number, hash)
  }

  def saveBlockBody(blockHash: Hash, blockBody: BlockBody) = {
    blockBodyStorage.put(blockHash, blockBody)
    saveTxsLocations(blockHash, blockBody)
  }

  def saveReceipts(blockHash: Hash, receipts: Seq[Receipt]) =
    receiptsStorage.put(blockHash, receipts)

  def saveEvmcode(hash: Hash, evmCode: ByteString) =
    evmCodeStorage.put(hash, evmCode)

  def saveTotalDifficulty(blockhash: Hash, td: BigInteger): Unit =
    totalDifficultyStorage.put(blockhash, td)

  def getWorldState(blockNumber: Long, accountStartNonce: UInt256, stateRootHash: Option[Hash]): BlockWorldState =
    BlockWorldState(
      this,
      accountNodeStorageFor(Some(blockNumber)),
      storageNodeStorageFor(Some(blockNumber)),
      accountStartNonce,
      stateRootHash
    )

  //FIXME Maybe we can use this one in regular execution too and persist underlying storage when block execution is successful
  def getReadOnlyWorldState(blockNumber: Option[Long], accountStartNonce: UInt256, stateRootHash: Option[Hash]): BlockWorldState =
    BlockWorldState(
      this,
      ReadOnlyNodeStorage(accountNodeStorageFor(blockNumber)),
      ReadOnlyNodeStorage(storageNodeStorageFor(blockNumber)),
      accountStartNonce,
      stateRootHash
    )

  def removeBlock(blockHash: Hash) {
    val maybeTxList = getBlockBodyByHash(blockHash).map(_.transactionList)
    blockHeaderStorage.remove(blockHash)
    blockBodyStorage.remove(blockHash)
    totalDifficultyStorage.remove(blockHash)
    receiptsStorage.remove(blockHash)
    maybeTxList.foreach(removeTxsLocations)
  }

  /**
   * API for outside query. Account can only be quered via MPT trie
   */
  def getAccount(address: Address, blockNumber: Long): Option[Account] =
    getBlockHeaderByNumber(blockNumber).flatMap { blockheader =>
      MerklePatriciaTrie[Address, Account](
        blockheader.stateRoot.bytes,
        accountNodeStorageFor(Some(blockNumber))
      )(Address.hashedAddressEncoder, Account.accountSerializer).get(address)
    }

  /**
   * API for outside query. Account storage can only be quered via MPT trie
   */
  def getAccountStorageAt(accountStateRootHash: Hash, position: BigInteger): ByteString = {
    val storage = MerklePatriciaTrie(
      accountStateRootHash.bytes,
      storageNodeStorageFor(None)
    )(trie.hashUInt256Serializable, trie.rlpUInt256Serializer).get(UInt256(position)).getOrElse(UInt256.Zero).bytes

    ByteString(storage)
  }

  def getMptNodeByHash(hash: Hash): Option[MptNode] =
    (accountNodeStorageFor(None).get(hash) orElse storageNodeStorageFor(None).get(hash)).map(_.toMptNode)

  def getTransactionLocation(txHash: Hash): Option[TransactionLocation] =
    transactionMappingStorage.get(txHash)

  private def saveBlockNumberMapping(number: Long, hash: Hash): Unit =
    blockHeaderStorage.putBlockHash(number, hash)

  private def saveTxsLocations(blockHash: Hash, blockBody: BlockBody): Unit =
    blockBody.transactionList.zipWithIndex.foreach {
      case (tx, index) => transactionMappingStorage.put(tx.hash, TransactionLocation(blockHash, index))
    }

  private def removeTxsLocations(stxs: Seq[SignedTransaction]) {
    stxs.map(_.hash).foreach(transactionMappingStorage.remove)
  }
}

