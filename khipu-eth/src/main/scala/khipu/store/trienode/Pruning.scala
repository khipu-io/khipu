package khipu.store.trienode

import akka.util.ByteString
import khipu.Hash
import scala.collection.mutable

// --- helping classes
/**
 * Model to be used to store, by block number, which block keys are no longer needed (and can potentially be deleted)
 */
final case class PruneCandidates(var nodeKeys: Set[Hash])

/**
 * Wrapper of MptNode in order to store number of references it has.
 *
 * @param nodeEncoded Encoded Mpt Node to be used in MerklePatriciaTrie
 * @param references  Number of references the node has. Each time it's updated references are increased and everytime it's deleted, decreased
 */
final case class StoredNode(nodeEncoded: ByteString, var references: Int) {
  def incrementReferences(amount: Int): StoredNode = {
    references += amount
    this
  }
  def decrementReferences(amount: Int): StoredNode = {
    references -= amount
    this
  }
}

sealed trait PruningMode
case object ArchivePruning extends PruningMode
final case class HistoryPruning(history: Int) extends PruningMode

object PruningMode {
  type PruneFn = (=> Long, => Long) => PruneResult

  //  def nodeTableStorage(pruningMode: PruningMode, nodeStorage: NodeStorage, source: KesqueDataSource)(blockNumber: Option[Long])(implicit system: ActorSystem): NodeKeyValueStorage =
  //    pruningMode match {
  //      case ArchivePruning          => new NodeTableStorage(source) // new ArchiveNodeStorage(nodeStorage) // new DistributedNodeStorage(nodeStorage) 
  //      case HistoryPruning(history) => new ReferenceCountNodeStorage(nodeStorage, history, blockNumber)
  //    }
}

final case class PruneResult(lastPrunedBlockNumber: Long, pruned: Int) {
  override def toString: String = s"Number of mpt nodes deleted: $pruned. Last Pruned Block: $lastPrunedBlockNumber"
}

trait RangePrune {
  /**
   * Prunes data between [start, end)
   * @param start block to prone
   * @param end block where to stop. This one will not be pruned
   * @param pruneFn function that given a certain block number prunes the data and returns how many nodes were deleted
   * @return resulting PruneResult
   */
  def pruneBetween(start: Long, end: Long, pruneFn: Long => Int): PruneResult = {
    //log.debug(s"Pruning start for range $start - $end")
    val prunedCount = (start until end).foldLeft(0) { (acc, bn) =>
      acc + pruneFn(bn)
    }
    val result = PruneResult(end - 1, prunedCount)
    //log.debug(s"Pruning finished for range $start - $end. $result.")
    result
  }
}
