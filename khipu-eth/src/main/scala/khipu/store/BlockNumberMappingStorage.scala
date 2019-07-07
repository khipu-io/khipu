package khipu.store

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import khipu.Hash
import khipu.store.datasource.DataSource
import khipu.store.datasource.LmdbBlockDataSource
import khipu.store.datasource.LmdbDataSource
import khipu.util.SimpleMap
import org.lmdbjava.Dbi
import org.lmdbjava.Env

/**
 * This class is used to store the blockhash -> blocknumber
 */
final class BlockNumberMappingStorage(val source: DataSource)(implicit system: ActorSystem) extends SimpleMap[Hash, Long] {
  type This = BlockNumberMappingStorage

  private val log = Logging(system, this.getClass)
  private val lmdbSource = source.asInstanceOf[LmdbDataSource]
  private val env = lmdbSource.env
  private val table = lmdbSource.table

  val namespace: Array[Byte] = Array[Byte]()
  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: Long => Array[Byte] = ByteBuffer.allocate(8).putLong(_).array
  def valueDeserializer: Array[Byte] => Long = ByteBuffer.wrap(_).getLong

  {
    log.info(s"Loading block number index ...")
    val start = System.nanoTime

    loadBlockNumberIndex(env, table)

    log.info(s"Loaded block number index in ${(System.nanoTime - start) / 1000000000}s.")
  }

  private def loadBlockNumberIndex(env: Env[ByteBuffer], table: Dbi[ByteBuffer]) {
    val start = System.nanoTime
    val rtx = env.txnRead()
    val itr = table.iterate(rtx)
    while (itr.hasNext) {
      val entry = itr.next()

      val hash = new Array[Byte](entry.key.remaining)
      entry.key.get(hash)

      val blockNumber = entry.`val`.getLong()
      log.debug(s"blockNumber: $blockNumber, hash: ${Hash(hash)}")

      LmdbBlockDataSource.putTimestampToKey(blockNumber, Hash(hash))
    }
    itr.close()
    rtx.commit()
    rtx.close()
  }

  override def get(key: Hash): Option[Long] = {
    source.get(namespace, key.bytes).map(x => ByteBuffer.wrap(x).getLong)
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, Long]): BlockNumberMappingStorage = {
    val remove = toRemove map { key => key.bytes }
    val upsert = toUpsert map { case (key, value) => (key.bytes -> ByteBuffer.allocate(8).putLong(value).array) }
    source.update(namespace, remove, upsert)
    this
  }

  def count = source.asInstanceOf[LmdbDataSource].count

  protected def apply(source: DataSource): BlockNumberMappingStorage = new BlockNumberMappingStorage(source)
}

