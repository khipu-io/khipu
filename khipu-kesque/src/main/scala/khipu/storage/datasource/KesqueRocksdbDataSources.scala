package khipu.storage.datasource

import khipu.config.DbConfig
import khipu.config.RocksdbConfig

trait KesqueRocksdbDataSources extends KesqueDataSources with SharedRocksdbDataSources {
  lazy val rocksdbConfig = new RocksdbConfig(datadir, config.getConfig("db").getConfig("rocksdb"))

  // block size evalution: https://etherscan.io/chart/blocksize, https://ethereum.stackexchange.com/questions/1106/is-there-a-limit-for-transaction-size/1110#1110
  // trie node size evalution:
  //   LeafNode - 256bytes(key) + value ~ 256 + value
  //   ExtensionNode - 256bytes(key) + 256bytes(hash) ~ 512
  //   BranchNode - 32bytes (children) + (256bytes(key) + value) (terminator with k-v) ~ 288 + value
  // account trie node size evalution: account value - 4x256bytes ~ 288 + 1024
  // storage trie node size evalution: storage valye - 256bytes ~ 288 + 256 

  lazy val accountNodeDataSource = new KesqueNodeDataSource(DbConfig.account, kesque, Right(rocksdbConfig), cacheSize = cacheCfg.cacheSize)
  lazy val storageNodeDataSource = new KesqueNodeDataSource(DbConfig.storage, kesque, Right(rocksdbConfig), cacheSize = cacheCfg.cacheSize)
  lazy val evmcodeDataSource = new KesqueNodeDataSource(DbConfig.evmcode, kesque, Right(rocksdbConfig), cacheSize = 10000)

  lazy val blockNumberDataSource = new RocksdbDataSource(DbConfig.blocknum, rocksdbConfig, cacheSize = 1000)

  lazy val blockHeaderDataSource = new KesqueBlockDataSource(DbConfig.header, kesque, cacheSize = 1000)
  lazy val blockBodyDataSource = new KesqueBlockDataSource(DbConfig.body, kesque, cacheSize = 1000)
  lazy val receiptsDataSource = new KesqueBlockDataSource(DbConfig.receipts, kesque, cacheSize = 1000)
  lazy val totalDifficultyDataSource = new KesqueBlockDataSource(DbConfig.td, kesque, cacheSize = 1000)

  //  private val futureTables = Future.sequence(List(
  //    Future(kesque.getTable(Array(DbConfig.account), 4096, CompressionType.NONE, cacheCfg.cacheSize)),
  //    Future(kesque.getTable(Array(DbConfig.storage), 4096, CompressionType.NONE, cacheCfg.cacheSize)),
  //    Future(kesque.getTable(Array(DbConfig.evmcode), 24576)),
  //    Future(kesque.getTimedTable(Array(
  //      DbConfig.header,
  //      DbConfig.body,
  //      DbConfig.receipts,
  //      DbConfig.td
  //    ), 102400))
  //  ))
  //  private val List(accountTable, storageTable, evmcodeTable, blockTable) = Await.result(futureTables, Duration.Inf)
  //  //private val headerTable = kesque.getTimedTable(Array(KesqueDataSource.header), 1024000)
  //  //private val bodyTable = kesque.getTable(Array(KesqueDataSource.body), 1024000)
  //  //private val tdTable = kesque.getTable(Array(KesqueDataSource.td), 1024000)
  //  //private val receiptTable = kesque.getTable(Array(KesqueDataSource.receipts), 1024000)
  //
  //  lazy val accountNodeDataSource = new KesqueDataSource(accountTable, DbConfig.account)
  //  lazy val storageNodeDataSource = new KesqueDataSource(storageTable, DbConfig.storage)
  //  lazy val evmCodeDataSource = new KesqueDataSource(evmcodeTable, DbConfig.evmcode)
  //
  //  lazy val blockHeaderDataSource = new KesqueDataSource(blockTable, DbConfig.header)
  //  lazy val blockBodyDataSource = new KesqueDataSource(blockTable, DbConfig.body)
  //  lazy val receiptsDataSource = new KesqueDataSource(blockTable, DbConfig.receipts)
  //  lazy val totalDifficultyDataSource = new KesqueDataSource(blockTable, DbConfig.td)

  def terminate() {
    log.info("db syncing...")

    accountNodeDataSource.terminate()
    storageNodeDataSource.terminate()
    evmcodeDataSource.terminate()
    blockNumberDataSource.terminate()
    blockHeaderDataSource.terminate()
    blockBodyDataSource.terminate()
    receiptsDataSource.terminate()
    totalDifficultyDataSource.terminate()

    dataSource.terminate()

    //kesque.shutdown()

    log.info("db synced")
  }
}