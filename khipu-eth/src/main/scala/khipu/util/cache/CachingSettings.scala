package khipu.util.cache

import com.typesafe.config.Config
import khipu.util.EnhancedConfig
import khipu.util.SettingsCompanion
import scala.concurrent.duration.Duration

object CachingSettings extends SettingsCompanion[CachingSettings]("khipu.caching") {
  implicit def enhanceConfig(config: Config): EnhancedConfig = new EnhancedConfig(config)

  def fromSubConfig(root: Config, c: Config) = {
    val lfuConfig = c.getConfig("lfu-cache")
    CachingSettingsImpl(
      LfuCacheSettingsImpl(
        lfuConfig getInt "max-capacity",
        lfuConfig getInt "initial-capacity",
        lfuConfig getPotentiallyInfiniteDuration "time-to-live",
        lfuConfig getPotentiallyInfiniteDuration "time-to-idle"
      )
    )
  }
}

/**
 * Public API but not intended for subclassing
 */
abstract class CachingSettings private[util] () { self: CachingSettingsImpl =>
  override def lfuCacheSettings: LfuCacheSettings

  // overloads for idiomatic Scala use
  def withLfuCacheSettings(newSettings: LfuCacheSettings): CachingSettings =
    self.copy(lfuCacheSettings = newSettings)
}

/** INTERNAL API */
private[util] final case class CachingSettingsImpl(lfuCacheSettings: LfuCacheSettings) extends CachingSettings {
  override def productPrefix = "CachingSettings"
}

/**
 * Public API but not intended for subclassing
 */
abstract class LfuCacheSettings private[util] () { self: LfuCacheSettingsImpl =>
  def maxCapacity: Int
  def initialCapacity: Int
  def timeToLive: Duration
  def timeToIdle: Duration

  def withMaxCapacity(newMaxCapacity: Int): LfuCacheSettings = self.copy(maxCapacity = newMaxCapacity)
  def withInitialCapacity(newInitialCapacity: Int): LfuCacheSettings = self.copy(initialCapacity = newInitialCapacity)
  def withTimeToLive(newTimeToLive: Duration): LfuCacheSettings = self.copy(timeToLive = newTimeToLive)
  def withTimeToIdle(newTimeToIdle: Duration): LfuCacheSettings = self.copy(timeToIdle = newTimeToIdle)
}

/** INTERNAL API */
private[util] final case class LfuCacheSettingsImpl(
    maxCapacity:     Int,
    initialCapacity: Int,
    timeToLive:      Duration,
    timeToIdle:      Duration
) extends LfuCacheSettings {
  override def productPrefix = "LfuCacheSettings"
}
