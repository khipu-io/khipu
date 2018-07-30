package khipu.ledger

import khipu.Deleted
import khipu.Log
import khipu.Original
import khipu.Updated
import khipu.vm.Storage
import khipu.vm.UInt256
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

  def load(address: UInt256): UInt256 = {
    logs.get(address) match {
      case None =>
        underlyingTrie.get(address) match {
          case Some(value) =>
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

  def commit(): TrieStorage = {
    val committed = this.logs.foldLeft(this.underlyingTrie) {
      case (acc, (k, Deleted(_))) => acc - k
      case (acc, (k, Updated(v))) => acc + (k -> v)
      case (acc, _)               => acc
    }
    new TrieStorage(committed, Map())
  }
}