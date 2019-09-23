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

  private var originalValues = Map[DataWord, DataWord]()

  def getOriginalValue(address: DataWord): Option[DataWord] =
    originalValues.get(address) orElse underlyingTrie.get(address)

  def load(address: DataWord): DataWord = {
    logs.get(address) match {
      case None =>
        underlyingTrie.get(address) match {
          case Some(value) =>
            if (!originalValues.contains(address)) {
              originalValues += (address -> value)
            }
            logs += (address -> Original(value))
            value
          case None => DataWord.Zero
        }
      case Some(Original(value)) => value
      case Some(Updated(value))  => value
      case Some(Removed(_))      => DataWord.Zero
    }
  }

  def store(address: DataWord, value: DataWord): TrieStorage = {
    val updatedLogs = if (value.isZero) {
      logs + (address -> REMOVED_VALUE)
    } else {
      logs + (address -> Updated(value))
    }
    new TrieStorage(underlyingTrie, updatedLogs)
  }

  def flush(): TrieStorage = {
    val flushed = this.logs.foldLeft(this.underlyingTrie) {
      case (acc, (k, Removed(_))) => acc - k
      case (acc, (k, Updated(v))) => acc + (k -> v)
      case (acc, _)               => acc
    }
    new TrieStorage(flushed, Map())
  }
}