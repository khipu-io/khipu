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

  // -- storage
  def transactionStorage: TransactionStorage
  def totalDifficultyStorage: TotalDifficultyStorage

  def accountNodeStorage: NodeStorage
  def storageNodeStorage: NodeStorage
  def evmcodeStorage: NodeStorage

  def blockNumberStorage: BlockNumberStorage
  def blockHeaderStorage: BlockHeaderStorage
  def blockBodyStorage: BlockBodyStorage
  def receiptsStorage: ReceiptsStorage

  def unconfirmedDepth: Int

  // ---
  def blockNumbers: BlockNumbers
}
