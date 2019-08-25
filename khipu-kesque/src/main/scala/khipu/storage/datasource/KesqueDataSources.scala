package khipu.storage.datasource

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import com.typesafe.config.Config
import java.io.File
import kesque.Kesque
import khipu.config.CacheConfig
import khipu.config.DbConfig
import khipu.util.cache.CachingSettings

trait KesqueDataSources extends SharedRocksdbDataSources {
  implicit protected val system: ActorSystem
  import system.dispatcher

  protected val config: Config
  protected val log: LoggingAdapter
  protected val khipuPath: File
  protected val datadir: String

  private lazy val defaultCachingSettings = CachingSettings(system)
  private lazy val cacheCfg = CacheConfig(config)

  protected lazy val configDir = new File(khipuPath, "conf")
  protected lazy val kafkaConfigFile = new File(configDir, "kafka.server.properties")
  private lazy val kafkaProps = {
    val props = org.apache.kafka.common.utils.Utils.loadProps(kafkaConfigFile.getAbsolutePath)
    props.put("log.dirs", datadir + "/" + config.getString("kesque-dir"))
    props
  }
  lazy val kesque = new Kesque(kafkaProps)

  lazy val rocksdbHome = {
    val h = new File(datadir + "/" + "rocksdb")
    if (h.exists) {
      h.mkdir()
    }
    h
  }
  // block size evalution: https://etherscan.io/chart/blocksize, https://ethereum.stackexchange.com/questions/1106/is-there-a-limit-for-transaction-size/1110#1110
  // trie node size evalution:
  //   LeafNode - 256bytes(key) + value ~ 256 + value
  //   ExtensionNode - 256bytes(key) + 256bytes(hash) ~ 512
  //   BranchNode - 32bytes (children) + (256bytes(key) + value) (terminator with k-v) ~ 288 + value
  // account trie node size evalution: account value - 4x256bytes ~ 288 + 1024
  // storage trie node size evalution: storage valye - 256bytes ~ 288 + 256 

  lazy val accountNodeDataSource = new KesqueNodeDataSource(DbConfig.account, kesque, Right(rocksdbHome), cacheSize = cacheCfg.cacheSize)
  lazy val storageNodeDataSource = new KesqueNodeDataSource(DbConfig.storage, kesque, Right(rocksdbHome), cacheSize = cacheCfg.cacheSize)
  lazy val evmcodeDataSource = new KesqueNodeDataSource(DbConfig.evmcode, kesque, Right(rocksdbHome))

  lazy val blockNumberDataSource = new RocksdbDataSource(DbConfig.blocknum, rocksdbHome)

  lazy val blockHeaderDataSource = new KesqueBlockDataSource(DbConfig.header, kesque)
  lazy val blockBodyDataSource = new KesqueBlockDataSource(DbConfig.body, kesque)
  lazy val receiptsDataSource = new KesqueBlockDataSource(DbConfig.receipts, kesque)
  lazy val totalDifficultyDataSource = new KesqueBlockDataSource(DbConfig.td, kesque)

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

  def closeAll() {
    accountNodeDataSource.close()
    storageNodeDataSource.close()
    evmcodeDataSource.close()
    blockNumberDataSource.close()
    blockHeaderDataSource.close()
    blockBodyDataSource.close()
    receiptsDataSource.close()
    totalDifficultyDataSource.close()

    dataSource.close()

    kesque.shutdown()
  }
}