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

  def blockNumberDataSource: DataSource

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

  def blockNumberStorage: BlockNumberStorage
  def blockHeaderStorage: BlockHeaderStorage
  def blockBodyStorage: BlockBodyStorage
  def receiptsStorage: ReceiptsStorage

  // ---
  def blockNumbers: BlockNumbers

  def getBlockNumberByHash(hash: Hash) = blockNumbers.getBlockNumberByHash(hash)
  def getHashByBlockNumber(blockNumber: Long) = blockNumbers.getHashByBlockNumber(blockNumber)
  def getHashsByBlockNumberRange(from: Long, to: Long) = blockNumbers.getHashsByBlockNumberRange(from, to)
  def putBlockNumber(blockNumber: Long, hash: Hash) = blockNumbers.putBlockNumber(blockNumber, hash)
  def removeBlockNumber(key: Hash) = blockNumbers.removeBlockNumber(key)
}
