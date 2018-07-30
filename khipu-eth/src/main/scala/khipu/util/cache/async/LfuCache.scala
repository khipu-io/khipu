package khipu.util.cache.async

import akka.actor.ActorSystem
import com.github.benmanes.caffeine.cache.{ AsyncCacheLoader, AsyncLoadingCache, Caffeine }
import java.util.concurrent.{ CompletableFuture, Executor, TimeUnit }
import java.util.function.BiFunction
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
      .buildAsync[K, V](dummyLoader[K, V])
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

    val store = (ttl andThen tti)(builder).buildAsync[K, V](dummyLoader[K, V])
    new LfuCache[K, V](store)
  }

  // LfuCache requires a loader function on creation - this will not be used.
  private def dummyLoader[K, V] = new AsyncCacheLoader[K, V] {
    def asyncLoad(k: K, e: Executor) =
      Future.failed[V](new RuntimeException("Dummy loader should not be used by LfuCache")).toJava.toCompletableFuture
  }

  def toJavaMappingFunction[K, V](genValue: () => Future[V]): BiFunction[K, Executor, CompletableFuture[V]] =
    asJavaBiFunction[K, Executor, CompletableFuture[V]]((k, e) => genValue().toJava.toCompletableFuture)

  def toJavaMappingFunction[K, V](loadValue: K => Future[V]): BiFunction[K, Executor, CompletableFuture[V]] =
    asJavaBiFunction[K, Executor, CompletableFuture[V]]((k, e) => loadValue(k).toJava.toCompletableFuture)
}

private[util] final class LfuCache[K, V](val store: AsyncLoadingCache[K, V]) extends Cache[K, V] {
  import LfuCache.toJavaMappingFunction

  def get(key: K): Option[Future[V]] = Option(store.getIfPresent(key)).map(_.toScala)

  def apply(key: K, genValue: () => Future[V]): Future[V] = store.get(key, toJavaMappingFunction[K, V](genValue)).toScala

  def getOrLoad(key: K, loadValue: K => Future[V]) = store.get(key, toJavaMappingFunction[K, V](loadValue)).toScala

  def put(key: K, genValue: () => Future[V]) = store.put(key, genValue().toJava.toCompletableFuture)

  def remove(key: K): Unit = store.synchronous().invalidate(key)

  def clear(): Unit = store.synchronous().invalidateAll()

  def keys: Set[K] = store.synchronous().asMap().keySet().asScala.toSet

  def size: Int = store.synchronous().asMap().size()
}

