package khipu.storage

import java.util.concurrent.locks.ReentrantReadWriteLock
import khipu.Hash
import khipu.HashWithBlockNumber
import khipu.util.CircularArrayQueue
import scala.collection.mutable

final class BlockNumbers(blockNumberStorage: BlockNumberStorage, blockHeaderStorage: BlockHeaderStorage, unconfirmedDepth: Int) {
  private val blockNumberToHash = new mutable.HashMap[Long, Hash]()
  private val hashToBlockNumber = new mutable.HashMap[Hash, Long]()

  private val unconfirmed = new CircularArrayQueue[(Hash, Long)](unconfirmedDepth)

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  def get(hash: Hash): Option[Long] = {
    try {
      readLock.lock()

      hashToBlockNumber.get(hash) match {
        case None =>
          blockNumberStorage.get(hash).map { blockNumber =>
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

  def put(blockhash: Hash, blockNumber: Long) {
    try {
      writeLock.lock()

      hashToBlockNumber += (blockhash -> blockNumber)
      blockNumberToHash += (blockNumber -> blockhash)
      unconfirmed.enqueue((blockhash, blockNumber))
    } finally {
      writeLock.unlock()
    }
  }

  def remove(blockhash: Hash) {
    try {
      writeLock.lock()

      hashToBlockNumber.get(blockhash) foreach blockNumberToHash.remove
      hashToBlockNumber -= blockhash
    } finally {
      writeLock.unlock()
    }
  }

  def getHashByBlockNumber(blockNumber: Long): Option[Hash] = {
    import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
    try {
      readLock.lock()

      blockNumberToHash.get(blockNumber) match {
        case None =>
          blockHeaderStorage.get(blockNumber).map(_.hash) map { hash =>
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

  def clearUnconfirmed() {
    try {
      writeLock.lock()

      while (!unconfirmed.isEmpty) {
        val (hash, number) = unconfirmed.dequeue()
        hashToBlockNumber -= hash
        blockNumberToHash -= number
      }
    } finally {
      writeLock.unlock()
    }
  }
}
