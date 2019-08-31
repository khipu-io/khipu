package khipu.config

import com.typesafe.config.Config

class RocksdbConfig(datadir: String, rocksdbConfig: Config) {
  val path = datadir + "/" + rocksdbConfig.getString("path")
  val writeBufferSize = rocksdbConfig.getInt("write_buffer_size")
  val maxWriteBufferNumber = rocksdbConfig.getInt("max_Write_Buffer_Number")
  val minWriteBufferNumberToMerge = rocksdbConfig.getInt("min_Write_Buffer_Number_To_Merge")
}
