package khipu.util.cache.sync

import akka.actor.ActorSystem
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.CacheLoader
import java.util.concurrent.{ Executor, TimeUnit }
import khipu.util.cache.CachingSettings
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.Future
import scala.compat.java8.FutureConverters._
import scala.compat.java8.FunctionConverters._

object LfuCache {

  def apply[K, V](implicit system: ActorSystem): Cache[K, V] =
    apply(CachingSettings(system))

  /**
   * Creates a new [[khipu.util.LfuCache]], with optional expiration depending
   * on whether a non-zero and finite timeToLive and/or timeToIdle is set or not.
   */
  def apply[K, V](cachingSettings: CachingSettings): Cache[K, V] = {
    val settings = cachingSettings.lfuCacheSettings

    require(settings.maxCapacity >= 0, "maxCapacity must not be negative")
    require(settings.initialCapacity <= settings.maxCapacity, "initialCapacity must be <= maxCapacity")

    if (settings.timeToLive.isFinite() || settings.timeToIdle.isFinite()) expiringLfuCache(settings.maxCapacity, settings.initialCapacity, settings.timeToLive, settings.timeToIdle)
    else simpleLfuCache(settings.maxCapacity, settings.initialCapacity)
  }

  private def simpleLfuCache[K, V](maxCapacity: Int, initialCapacity: Int): LfuCache[K, V] = {
    val store = Caffeine.newBuilder().asInstanceOf[Caffeine[K, V]]
      .initialCapacity(initialCapacity)
      .maximumSize(maxCapacity)
      .build[K, V](dummyLoader[K, V])
    new LfuCache[K, V](store)
  }

  private def expiringLfuCache[K, V](
    maxCapacity:     Long,
    initialCapacity: Int,
    timeToLive:      Duration,
    timeToIdle:      Duration
  ): LfuCache[K, V] = {
    require(
      !timeToLive.isFinite || !timeToIdle.isFinite || timeToLive > timeToIdle,
      s"timeToLive($timeToLive) must be greater than timeToIdle($timeToIdle)"
    )

    def ttl: Caffeine[K, V] => Caffeine[K, V] = { builder =>
      if (timeToLive.isFinite) builder.expireAfterWrite(timeToLive.toMillis, TimeUnit.MILLISECONDS)
      else builder
    }

    def tti: Caffeine[K, V] => Caffeine[K, V] = { builder =>
      if (timeToIdle.isFinite) builder.expireAfterAccess(timeToIdle.toMillis, TimeUnit.MILLISECONDS)
      else builder
    }

    val builder = Caffeine.newBuilder().asInstanceOf[Caffeine[K, V]]
      .initialCapacity(initialCapacity)
      .maximumSize(maxCapacity)

    val store = (ttl andThen tti)(builder).build[K, V](dummyLoader[K, V])
    new LfuCache[K, V](store)
  }

  // LfuCache requires a loader function on creation - this will not be used.
  private def dummyLoader[K, V] = new CacheLoader[K, V] {
    def load(key: K): V = throw new RuntimeException("Dummy loader should not be used by LfuCache")

    override def asyncLoad(k: K, e: Executor) = Future.failed[V](new RuntimeException("Dummy loader should not be used by LfuCache")).toJava.toCompletableFuture
  }

  final case class ToJavaFunction[K, V](genValue: () => V) extends java.util.function.Function[K, V] {
    def apply(k: K) = genValue()
  }
}

private[util] final class LfuCache[K, V](val store: com.github.benmanes.caffeine.cache.Cache[K, V]) extends Cache[K, V] {
  import LfuCache.ToJavaFunction

  def get(key: K): Option[V] = Option(store.getIfPresent(key))

  def apply(key: K, genValue: () => V): V = store.get(key, ToJavaFunction(genValue))

  def getOrLoad(key: K, loadValue: K => V) = store.get(key, loadValue.asJava)

  def put(key: K, value: V) = store.put(key, value)

  def remove(key: K): Unit = store.invalidate(key)

  def clear(): Unit = store.invalidateAll()

  def keys: Set[K] = store.asMap().keySet().asScala.toSet

  def size: Int = store.asMap().size()
}

