package khipu.storage

import khipu.Hash
import khipu.storage.datasource.BlockDataSource
import khipu.storage.datasource.DataSource

trait BlockchainStorages {
  // -- data source
  def accountNodeDataSource: BlockDataSource[Hash, Array[Byte]]
  def storageNodeDataSource: BlockDataSource[Hash, Array[Byte]]
  def evmcodeDataSource: BlockDataSource[Hash, Array[Byte]]

  def blockNumberDataSource: DataSource

  def blockHeaderDataSource: BlockDataSource[Long, Array[Byte]]
  def blockBodyDataSource: BlockDataSource[Long, Array[Byte]]
  def receiptsDataSource: BlockDataSource[Long, Array[Byte]]

  def totalDifficultyDataSource: BlockDataSource[Long, Array[Byte]]
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
