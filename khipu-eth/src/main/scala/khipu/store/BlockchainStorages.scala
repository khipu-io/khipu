package khipu.store

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
}
