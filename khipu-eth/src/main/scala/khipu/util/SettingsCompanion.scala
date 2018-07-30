package khipu.util

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigFactory._
import java.net.{ InetAddress, InetSocketAddress }
import scala.util.control.NonFatal
import scala.collection.immutable.ListMap
import scala.collection.JavaConverters._
import akka.actor.{ ActorRefFactory, ActorSystem, ExtendedActorSystem, ActorContext }

/**
 * INTERNAL API
 */
private[util] abstract class SettingsCompanion[T](protected val prefix: String) {
  private final val MaxCached = 8
  private[this] var cache = ListMap.empty[ActorSystem, T]

  private[util] def actorSystem(implicit refFactory: ActorRefFactory): ExtendedActorSystem =
    refFactory match {
      case x: ActorContext        => actorSystem(x.system)
      case x: ExtendedActorSystem => x
      case _                      => throw new IllegalStateException
    }

  implicit def default(implicit refFactory: ActorRefFactory): T =
    apply(actorSystem)

  def apply(system: ActorSystem): T =
    // we use and update the cache without any synchronization,
    // there are two possible "problems" resulting from this:
    // - cache misses of things another thread has already put into the cache,
    //   in these cases we do double work, but simply accept it
    // - cache hits of things another thread has already dropped from the cache,
    //   in these cases we avoid double work, which is nice
    cache.getOrElse(system, {
      val settings = apply(system.settings.config)
      val c =
        if (cache.size < MaxCached) cache
        else cache.tail // drop the first (and oldest) cache entry
      cache = c.updated(system, settings)
      settings
    })

  def apply(configOverrides: String): T =
    apply(parseString(configOverrides)
      .withFallback(SettingsCompanion.configAdditions)
      .withFallback(defaultReference(getClass.getClassLoader)))

  def apply(config: com.typesafe.config.Config): T =
    fromSubConfig(config, config getConfig prefix)

  def fromSubConfig(root: com.typesafe.config.Config, c: com.typesafe.config.Config): T
}

private[util] object SettingsCompanion {
  lazy val configAdditions: com.typesafe.config.Config = {
    val localHostName =
      try new InetSocketAddress(InetAddress.getLocalHost, 80).getHostString
      catch { case NonFatal(_) ⇒ "" }
    ConfigFactory.parseMap(Map("akka.http.hostname" → localHostName).asJava)
  }
}
