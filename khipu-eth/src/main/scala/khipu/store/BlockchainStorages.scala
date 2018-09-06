package khipu.store

import khipu.Hash
import khipu.domain.BlockHeader
import khipu.network.p2p.messages.PV62.BlockBody
import khipu.store.datasource.KesqueDataSource
import khipu.store.trienode.NodeKeyValueStorage
import khipu.util.cache.sync.Cache

trait BlockchainStorages {
  // share nodeKeyValueCache instance for all CachedNodeStorage
  def accountNodeDataSource: KesqueDataSource
  def storageNodeDataSource: KesqueDataSource
  def evmCodeDataSource: KesqueDataSource

  def blockHeaderDataSource: KesqueDataSource
  def blockBodyDataSource: KesqueDataSource
  def receiptsDataSource: KesqueDataSource
  def totalDifficultyDataSource: KesqueDataSource

  protected def nodeKeyValueCache: Cache[Hash, Array[Byte]]
  protected def blockHeaderCache: Cache[Hash, BlockHeader]
  protected def blockBodyCache: Cache[Hash, BlockBody]
  protected def blockNumberCache: Cache[Long, Hash]

  def cacheSize = nodeKeyValueCache.size

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
