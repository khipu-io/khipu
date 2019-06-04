package khipu.store

import khipu.store.datasource.BlockDataSource
import khipu.store.datasource.DataSource
import khipu.store.datasource.NodeDataSource
import khipu.store.trienode.NodeKeyValueStorage

trait BlockchainStorages {
  def accountNodeDataSource: NodeDataSource
  def storageNodeDataSource: NodeDataSource
  def evmCodeDataSource: DataSource

  def blockHeaderDataSource: BlockDataSource
  def blockBodyDataSource: BlockDataSource
  def receiptsDataSource: BlockDataSource
  def totalDifficultyDataSource: BlockDataSource

  def transactionMappingStorage: TransactionMappingStorage
  def totalDifficultyStorage: TotalDifficultyStorage

  def accountNodeStorageFor: (Option[Long]) => NodeKeyValueStorage
  def storageNodeStorageFor: (Option[Long]) => NodeKeyValueStorage
  def evmCodeStorage: EvmCodeStorage
  def blockHeaderStorage: BlockHeaderStorage
  def blockBodyStorage: BlockBodyStorage
  def receiptsStorage: ReceiptsStorage
  //def blockNumberMappingStorage: BlockNumberMappingStorage
}
