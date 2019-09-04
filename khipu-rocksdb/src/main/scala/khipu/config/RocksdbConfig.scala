package khipu.config

import com.typesafe.config.Config

class RocksdbConfig(datadir: String, rocksdbConfig: Config) {
  val path = datadir + "/" + rocksdbConfig.getString("path")
  val writeBufferSize = rocksdbConfig.getInt("write_buffer_size")
  val maxWriteBufferNumber = rocksdbConfig.getInt("max_write_buffer_number")
  val minWriteBufferNumberToMerge = rocksdbConfig.getInt("min_write_buffer_number_to_merge")
}
