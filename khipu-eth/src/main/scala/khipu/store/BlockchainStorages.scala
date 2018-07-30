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

  def blockHeadersDataSource: KesqueDataSource
  def blockBodiesDataSource: KesqueDataSource
  def receiptsDataSource: KesqueDataSource

  protected def nodeKeyValueCache: Cache[Hash, Array[Byte]]
  protected def blockHeadersCache: Cache[Hash, BlockHeader]
  protected def blockBodiesCache: Cache[Hash, BlockBody]
  protected def blockNumberCache: Cache[Long, Hash]

  def cacheSize = nodeKeyValueCache.size

  def totalDifficultyStorage: TotalDifficultyStorage
  def transactionMappingStorage: TransactionMappingStorage

  def accountNodeStorageFor: (Option[Long]) => NodeKeyValueStorage
  def storageNodeStorageFor: (Option[Long]) => NodeKeyValueStorage
  def evmCodeStorage: EvmCodeStorage
  def blockHeadersStorage: BlockHeadersStorage
  def blockBodiesStorage: BlockBodiesStorage
  def blockNumberMappingStorage: BlockNumberMappingStorage
  def receiptsStorage: ReceiptsStorage
}
