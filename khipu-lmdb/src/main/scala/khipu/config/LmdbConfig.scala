package khipu.config

import com.typesafe.config.Config

class LmdbConfig(datadir: String, lmdbConfig: Config) {
  val path = datadir + "/" + lmdbConfig.getString("path")
  val mapSize = lmdbConfig.getLong("map_size")
  val maxDbs = lmdbConfig.getInt("max_dbs")
  val maxReaders = lmdbConfig.getInt("max_readers")
}
