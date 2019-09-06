package khipu.storage

import khipu.storage.datasource.BlockDataSource
import khipu.storage.datasource.KeyValueDataSource
import khipu.storage.datasource.NodeDataSource

trait BlockchainStorages {
  // -- data source
  def accountNodeDataSource: NodeDataSource
  def storageNodeDataSource: NodeDataSource
  def evmcodeDataSource: NodeDataSource

  def blockNumberDataSource: KeyValueDataSource

  def blockHeaderDataSource: BlockDataSource
  def blockBodyDataSource: BlockDataSource
  def receiptsDataSource: BlockDataSource

  def totalDifficultyDataSource: BlockDataSource
  def transactionDataSource: KeyValueDataSource

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
