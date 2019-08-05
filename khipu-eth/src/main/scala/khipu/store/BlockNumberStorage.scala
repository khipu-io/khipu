package khipu.store

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import khipu.Hash
import khipu.store.datasource.DataSource
import khipu.store.datasource.LmdbDataSource
import khipu.util.SimpleMap
import org.lmdbjava.Dbi
import org.lmdbjava.Env
import scala.collection.mutable

object BlockNumberStorage {
  val namespace: Array[Byte] = Array[Byte]()
}
/**
 * This class is used to store the blockhash -> blocknumber
 */
final class BlockNumberStorage(storags: Storages, val source: DataSource)(implicit system: ActorSystem) extends SimpleMap[Hash, Long] {
  type This = BlockNumberStorage

  import BlockNumberStorage._

  private val log = Logging(system, this.getClass)
  private val lmdbSource = source.asInstanceOf[LmdbDataSource]
  private val env = lmdbSource.env
  private val table = lmdbSource.table

  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: Long => Array[Byte] = ByteBuffer.allocate(8).putLong(_).array
  def valueDeserializer: Array[Byte] => Long = ByteBuffer.wrap(_).getLong

  // batch load clock number - woo, this takes lots time
  //loadBlockNumberIndex(env, table)
  private def loadBlockNumberIndex(env: Env[ByteBuffer], table: Dbi[ByteBuffer]) {
    log.info(s"Loading block number index ...")

    val start = System.nanoTime
    val rtx = env.txnRead()
    val itr = table.iterate(rtx)
    while (itr.hasNext) {
      val entry = itr.next()

      val hash = new Array[Byte](entry.key.remaining)
      entry.key.get(hash)

      val blockNumber = entry.`val`.getLong()
      log.debug(s"blockNumber: $blockNumber, hash: ${Hash(hash)}")

      storags.putBlockNumber(blockNumber, Hash(hash))
    }
    itr.close()
    rtx.commit()
    rtx.close()

    log.info(s"Loaded block number index in ${(System.nanoTime - start) / 1000000000}s.")
  }

  override def get(key: Hash): Option[Long] = {
    source.get(namespace, key.bytes).map(x => ByteBuffer.wrap(x).getLong)
  }

  override def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Long)]): BlockNumberStorage = {
    val remove = toRemove map { key => key.bytes }
    val upsert = toUpsert map { case (key, value) => (key.bytes -> ByteBuffer.allocate(8).putLong(value).array) }
    source.update(namespace, remove, upsert)
    this
  }

  def count = source.asInstanceOf[LmdbDataSource].count

  protected def apply(storags: Storages, source: DataSource): BlockNumberStorage = new BlockNumberStorage(storags, source)
}

