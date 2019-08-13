package khipu.store

import khipu.store.datasource.BlockDataSource
import khipu.store.datasource.DataSource
import khipu.store.datasource.NodeDataSource

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
  def transactionDataSource: DataSource

  // -- storage
  def accountNodeStorage: NodeStorage
  def storageNodeStorage: NodeStorage
  def evmcodeStorage: NodeStorage

  def blockNumberStorage: BlockNumberStorage

  def blockHeaderStorage: BlockHeaderStorage
  def blockBodyStorage: BlockBodyStorage
  def receiptsStorage: ReceiptsStorage

  def totalDifficultyStorage: TotalDifficultyStorage
  def transactionStorage: TransactionStorage

  // -- others
  def blockNumbers: BlockNumbers

  def unconfirmedDepth: Int
}
