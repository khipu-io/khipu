package khipu.storage.datasource

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import com.typesafe.config.Config
import java.io.File
import kesque.Kesque
import khipu.config.CacheConfig
import khipu.util.cache.CachingSettings

trait KesqueDataSources {
  implicit protected val system: ActorSystem
  import system.dispatcher

  protected val config: Config
  protected val log: LoggingAdapter
  protected val datadir: String
  protected val khipuPath: File

  protected lazy val defaultCachingSettings = CachingSettings(system)
  protected lazy val cacheCfg = CacheConfig(config)

  protected lazy val configDir = new File(khipuPath, "conf")
  protected lazy val kafkaConfigFile = new File(configDir, "kafka.server.properties")
  private lazy val kafkaProps = {
    val props = org.apache.kafka.common.utils.Utils.loadProps(kafkaConfigFile.getAbsolutePath)
    props.put("log.dirs", datadir + "/" + config.getString("kesque-dir"))
    props
  }
  lazy val kesque = new Kesque(kafkaProps)
}
