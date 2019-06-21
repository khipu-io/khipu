package khipu.store

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kesque.TVal
import khipu.Hash
import khipu.domain.BlockHeader
import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
import khipu.store.datasource.BlockDataSource
import khipu.store.datasource.LmdbBlockDataSource
import khipu.util.SimpleMap
import org.lmdbjava.Dbi
import org.lmdbjava.Env
import scala.collection.mutable

/**
 * This class is used to store the BlockHeader, by using:
 *   Key: hash of the block to which the BlockHeader belong
 *   Value: the block header
 */
final class BlockHeaderStorage(val source: BlockDataSource)(implicit system: ActorSystem) extends SimpleMap[Hash, BlockHeader] {
  type This = BlockHeaderStorage

  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: BlockHeader => Array[Byte] = _.toBytes
  def valueDeserializer: Array[Byte] => BlockHeader = b => b.toBlockHeader

  private val log = Logging(system, this.getClass)

  {
    val start = System.nanoTime

    val blockHeaderSource = source.asInstanceOf[LmdbBlockDataSource]
    loadBlockNumberIndex(blockHeaderSource.env, blockHeaderSource.table)

    log.info(s"loaded blocknumber index in ${(System.nanoTime - start) / 1000000000}s ")
  }

  private def loadBlockNumberIndex(env: Env[ByteBuffer], table: Dbi[ByteBuffer]) {
    val start = System.nanoTime
    val txn = env.txnRead()
    val itr = table.iterate(txn)
    while (itr.hasNext) {
      val entry = itr.next()

      val blockNumber = entry.key.order(ByteOrder.nativeOrder).getLong()

      val data = new Array[Byte](entry.`val`.remaining)
      entry.`val`.get(data)

      LmdbBlockDataSource.putTimestampToKey(blockNumber, data.toBlockHeader.hash)
    }
    itr.close()
    txn.commit()
    txn.close()
  }

  override def get(key: Hash): Option[BlockHeader] = {
    LmdbBlockDataSource.getTimestampByKey(key) flatMap {
      blockNum => source.get(blockNum).map(_.value.toBlockHeader)
    }
  }

  override def update(toRemove: Set[Hash], toUpsert: Map[Hash, BlockHeader]): BlockHeaderStorage = {
    val upsert = toUpsert map {
      case (key, value) =>
        LmdbBlockDataSource.putTimestampToKey(value.number, key)
        (value.number -> TVal(value.toBytes, -1, value.number))
    }
    val remove = toRemove flatMap {
      key =>
        val blockNum = LmdbBlockDataSource.getTimestampByKey(key)
        LmdbBlockDataSource.removeTimestamp(key)
        blockNum
    }
    source.update(remove, upsert)
    this
  }

  def getBlockHash(blockNumber: Long) = LmdbBlockDataSource.getKeyByTimestamp(blockNumber)
  def getBlockNumber(hash: Hash) = LmdbBlockDataSource.getTimestampByKey(hash)

  protected def apply(source: BlockDataSource): BlockHeaderStorage = new BlockHeaderStorage(source)
}

