package khipu.store

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import khipu.Hash
import khipu.HashWithBlockNumber
import khipu.store.datasource.BlockDataSource
import khipu.store.datasource.DataSource
import scala.collection.mutable

final class BlockNumbers(blockNumberSource: DataSource, blockHeaderSource: BlockDataSource) {
  private val blockNumberToHash = new mutable.HashMap[Long, Hash]()
  private val hashToBlockNumber = new mutable.HashMap[Hash, Long]()

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  def getBlockNumberByHash(hash: Hash): Option[Long] = {
    try {
      readLock.lock()

      hashToBlockNumber.get(hash) match {
        case None =>
          blockNumberSource.get(BlockNumberStorage.namespace, hash.bytes).map(x => ByteBuffer.wrap(x).getLong) map { blockNumber =>
            blockNumberToHash += (blockNumber -> hash)
            hashToBlockNumber += (hash -> blockNumber)
            blockNumber
          }
        case some => some
      }
    } finally {
      readLock.unlock()
    }
  }

  def getHashByBlockNumber(blockNumber: Long): Option[Hash] = {
    import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
    try {
      readLock.lock()

      blockNumberToHash.get(blockNumber) match {
        case None =>
          blockHeaderSource.get(blockNumber).map(_.value.toBlockHeader.hash) map { hash =>
            blockNumberToHash += (blockNumber -> hash)
            hashToBlockNumber += (hash -> blockNumber)
            hash
          }
        case some => some
      }
    } finally {
      readLock.unlock()
    }
  }

  def getHashesByBlockNumberRange(from: Long, to: Long): Seq[HashWithBlockNumber] = {
    try {
      readLock.lock()

      val ret = new mutable.ListBuffer[HashWithBlockNumber]()
      var break = false
      var i = from
      while (i <= to && !break) {
        getHashByBlockNumber(i) match {
          case Some(hash) =>
            ret += HashWithBlockNumber(i, hash)
          case None =>
            break = true
        }
        i += 1
      }

      ret
    } finally {
      readLock.unlock()
    }
  }

  def putBlockNumber(blockNumber: Long, hash: Hash) {
    try {
      writeLock.lock()

      blockNumberToHash += (blockNumber -> hash)
      hashToBlockNumber += (hash -> blockNumber)
    } finally {
      writeLock.unlock()
    }
  }

  def removeBlockNumber(key: Hash) {
    try {
      writeLock.lock()

      hashToBlockNumber.get(key) foreach blockNumberToHash.remove
      hashToBlockNumber -= key
    } finally {
      writeLock.unlock()
    }
  }

}
