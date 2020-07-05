package khipu.ledger

import khipu.Log
import khipu.Original
import khipu.Updated
import khipu.Removed
import khipu.DataWord
import khipu.vm.Storage
import khipu.trie.MerklePatriciaTrie

/**
 * '''Immutable''' tried based vm storage: address -> value
 */
object TrieStorage {
  val REMOVED_VALUE = Removed(null)

  def apply(underlyingTrie: MerklePatriciaTrie[DataWord, DataWord]) =
    new TrieStorage(underlyingTrie, Map())
}
final class TrieStorage private (
    underlyingTrie:           MerklePatriciaTrie[DataWord, DataWord],
    private[ledger] var logs: Map[DataWord, Log[DataWord]]
) extends Storage[TrieStorage] {
  import TrieStorage._

  def underlying = underlyingTrie

  def load(offset: DataWord): DataWord = {
    logs.get(offset) match {
      case None =>
        underlyingTrie.get(offset) match {
          case Some(value) =>
            logs += (offset -> Original(value)) // for cache
            value
          case None => DataWord.Zero
        }
      case Some(Original(value)) => value
      case Some(Updated(value))  => value
      case Some(Removed(_))      => DataWord.Zero
    }
  }

  def store(offset: DataWord, value: DataWord): TrieStorage = {
    val updatedLogs = if (value.isZero) {
      logs + (offset -> REMOVED_VALUE)
    } else {
      logs + (offset -> Updated(value))
    }
    new TrieStorage(underlyingTrie, updatedLogs)
  }

  def flush(): TrieStorage = {
    // we'll keep cache logs (as Original logs)
    val (flushedTrie, cacheLogs) = logs.foldLeft(this.underlyingTrie, Map[DataWord, Log[DataWord]]()) {
      case ((acc, cache), (k, Removed(_)))  => (acc - k, cache + (k -> Original(DataWord.Zero)))
      case ((acc, cache), (k, Updated(v)))  => (acc + (k -> v), cache + (k -> Original(v)))
      case ((acc, cache), (k, Original(v))) => (acc, cache + (k -> Original(v)))
    }
    new TrieStorage(flushedTrie, cacheLogs)
  }

  override def toString() = {
    s"TrieStorage logs ${logs.mkString("(", ",", ")")}"
  }
}