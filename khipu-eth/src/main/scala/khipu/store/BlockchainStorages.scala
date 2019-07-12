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

  def blockNumberMappingDataSource: DataSource

  def blockHeaderDataSource: BlockDataSource
  def blockBodyDataSource: BlockDataSource
  def receiptsDataSource: BlockDataSource
  def totalDifficultyDataSource: BlockDataSource

  // -- storage
  def transactionMappingStorage: TransactionMappingStorage
  def totalDifficultyStorage: TotalDifficultyStorage

  def accountNodeStorageFor: (Option[Long]) => NodeKeyValueStorage
  def storageNodeStorageFor: (Option[Long]) => NodeKeyValueStorage
  def evmcodeStorage: NodeKeyValueStorage

  def blockNumberMappingStorage: BlockNumberMappingStorage
  def blockHeaderStorage: BlockHeaderStorage
  def blockBodyStorage: BlockBodyStorage
  def receiptsStorage: ReceiptsStorage

  // ---
  def blockNumberMapping: BlockNumberMapping

  def getBlockNumberByHash(hash: Hash) = blockNumberMapping.getBlockNumberByHash(hash)
  def getHashByBlockNumber(blockNumber: Long) = blockNumberMapping.getHashByBlockNumber(blockNumber)
  def getHashsByBlockNumberRange(from: Long, to: Long) = blockNumberMapping.getHashsByBlockNumberRange(from, to)
  def putBlockNumber(blockNumber: Long, hash: Hash) = blockNumberMapping.putBlockNumber(blockNumber, hash)
  def removeBlockNumber(key: Hash) = blockNumberMapping.removeBlockNumber(key)
}
