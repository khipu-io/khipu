package khipu.config

import com.typesafe.config.Config

object DbConfig {
  trait DBEngine
  case object LMDB extends DBEngine
  case object KESQUE extends DBEngine

  val account = "account"
  val storage = "storage"
  val evmcode = "evmcode"
  val blocknum = "blocknum"
  val header = "header"
  val body = "body"
  val td = "td" // total difficulty
  val receipts = "receipts"

}
class DbConfig(dbConfig: Config) {
  import DbConfig._

  val dbEngine: DBEngine = dbConfig.getString("engine") match {
    case "lmdb"   => LMDB
    case "kesque" => KESQUE
  }

  val batchSize = dbConfig.getInt("batch-size")
}

class LeveldbConfig(datadir: String, leveldbConfig: Config) {
  val path = datadir + "/" + leveldbConfig.getString("path")
  val createIfMissing = leveldbConfig.getBoolean("create-if-missing")
  val paranoidChecks = leveldbConfig.getBoolean("paranoid-checks")
  val verifyChecksums = leveldbConfig.getBoolean("verify-checksums")
  val cacheSize = leveldbConfig.getLong("cache-size")
}
