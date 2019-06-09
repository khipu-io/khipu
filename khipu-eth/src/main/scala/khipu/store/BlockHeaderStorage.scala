package khipu.store

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
final class BlockHeaderStorage(val source: BlockDataSource) extends SimpleMap[Hash, BlockHeader] {
  type This = BlockHeaderStorage

  def keySerializer: Hash => Array[Byte] = _.bytes
  def valueSerializer: BlockHeader => Array[Byte] = _.toBytes
  def valueDeserializer: Array[Byte] => BlockHeader = b => b.toBlockHeader

  {
    val blockHeaderSource = source.asInstanceOf[LmdbBlockDataSource]
    loadTimeIndex(blockHeaderSource.env, blockHeaderSource.table)
  }

  private def loadTimeIndex(env: Env[ByteBuffer], table: Dbi[ByteBuffer]) {
    val start = System.nanoTime
    val txn = env.txnRead()
    val itr = table.iterate(txn)
    while (itr.hasNext) {
      val entry = itr.next()

      val timestamp = entry.key.order(ByteOrder.nativeOrder).getLong()

      val data = new Array[Byte](entry.`val`.remaining)
      entry.`val`.get(data)

      LmdbBlockDataSource.putTimestampToKey(timestamp, data.toBlockHeader.hash)
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

