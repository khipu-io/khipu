package khipu.store

import khipu.Hash
import khipu.store.datasource.BlockDataSource
import khipu.store.datasource.DataSource
import khipu.store.datasource.NodeDataSource
import khipu.store.trienode.NodeKeyValueStorage

trait BlockchainStorages {
  // -- data source
  def accountNodeDataSource: NodeDataSource
  def storageNodeDataSource: NodeDataSource
  def evmcodeDataSource: NodeDataSource

  def blockHashDataSource: DataSource

  def blockHeaderDataSource: BlockDataSource
  def blockBodyDataSource: BlockDataSource
  def receiptsDataSource: BlockDataSource
  def totalDifficultyDataSource: BlockDataSource

  // -- storage
  def transactionStorage: TransactionStorage
  def totalDifficultyStorage: TotalDifficultyStorage

  def accountNodeStorageFor: (Option[Long]) => NodeKeyValueStorage
  def storageNodeStorageFor: (Option[Long]) => NodeKeyValueStorage
  def evmcodeStorage: NodeKeyValueStorage

  def blockHashStorage: BlockHashStorage
  def blockHeaderStorage: BlockHeaderStorage
  def blockBodyStorage: BlockBodyStorage
  def receiptsStorage: ReceiptsStorage

  // ---
  def blockHashes: BlockHashes

  def getBlockNumberByHash(hash: Hash) = blockHashes.getBlockNumberByHash(hash)
  def getHashByBlockNumber(blockNumber: Long) = blockHashes.getHashByBlockNumber(blockNumber)
  def getHashsByBlockNumberRange(from: Long, to: Long) = blockHashes.getHashsByBlockNumberRange(from, to)
  def putBlockNumber(blockNumber: Long, hash: Hash) = blockHashes.putBlockNumber(blockNumber, hash)
  def removeBlockNumber(key: Hash) = blockHashes.removeBlockNumber(key)
}
