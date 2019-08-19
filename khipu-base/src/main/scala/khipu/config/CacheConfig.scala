package khipu.config

import com.typesafe.config.{ Config => TypesafeConfig }

object CacheConfig {
  def apply(clientConfig: TypesafeConfig): CacheConfig = {
    val cacheConfig = clientConfig.getConfig("cache")

    new CacheConfig {
      val cacheSize = cacheConfig.getInt("cache-size")
    }
  }
}

trait CacheConfig {
  val cacheSize: Int
}
