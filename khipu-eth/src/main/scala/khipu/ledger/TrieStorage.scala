package khipu.ledger

import khipu.Deleted
import khipu.Log
import khipu.Original
import khipu.Updated
import khipu.UInt256
import khipu.vm.Storage
import khipu.trie.MerklePatriciaTrie

/**
 * '''Immutable''' tried based vm storage: address -> value
 */
object TrieStorage {
  val DeletedValue = Deleted(null)

  def apply(underlyingTrie: MerklePatriciaTrie[UInt256, UInt256]) =
    new TrieStorage(underlyingTrie, Map())
}
final class TrieStorage private (
    underlyingTrie:           MerklePatriciaTrie[UInt256, UInt256],
    private[ledger] var logs: Map[UInt256, Log[UInt256]]
) extends Storage[TrieStorage] {
  import TrieStorage._

  def underlying = underlyingTrie

  private var originalValues = Map[UInt256, UInt256]()

  def getOriginalValue(address: UInt256): Option[UInt256] =
    originalValues.get(address) orElse underlyingTrie.get(address)

  def load(address: UInt256): UInt256 = {
    logs.get(address) match {
      case None =>
        underlyingTrie.get(address) match {
          case Some(value) =>
            if (!originalValues.contains(address)) {
              originalValues += (address -> value)
            }
            logs += (address -> Original(value))
            value
          case None => UInt256.Zero
        }
      case Some(Original(value)) => value
      case Some(Updated(value))  => value
      case Some(Deleted(_))      => UInt256.Zero
    }
  }

  def store(address: UInt256, value: UInt256): TrieStorage = {
    val updatedLogs = if (value.isZero) {
      logs + (address -> DeletedValue)
    } else {
      logs + (address -> Updated(value))
    }
    new TrieStorage(underlyingTrie, updatedLogs)
  }

  def flush(): TrieStorage = {
    val flushed = this.logs.foldLeft(this.underlyingTrie) {
      case (acc, (k, Deleted(_))) => acc - k
      case (acc, (k, Updated(v))) => acc + (k -> v)
      case (acc, _)               => acc
    }
    new TrieStorage(flushed, Map())
  }
}