package khipu.ledger

import khipu.Log
import khipu.Original
import khipu.Removed
import khipu.Updated
import khipu.domain.Account
import khipu.domain.Address
import khipu.trie.MerklePatriciaTrie

/**
 * '''Immutable''' tried based accounts: address -> account
 *
 * Before flush(), all kv are kept in logs. flush() will write kv actually to
 * trie's node tree.  And underlyingTire.persist() will write trie nodes to
 * persistence.
 *
 */
object TrieAccounts {
  val REMOVED_VALUE = Removed(null)

  private def flush(trie: MerklePatriciaTrie[Address, Account], logs: Map[Address, Log[Account]]): MerklePatriciaTrie[Address, Account] = {
    logs.foldLeft(trie) {
      case (accTrie, (k, Removed(_)))  => accTrie - k
      case (accTrie, (k, Updated(v)))  => accTrie + (k -> v)
      case (accTrie, (k, Original(v))) => accTrie
    }
  }

  def apply(underlyingTrie: MerklePatriciaTrie[Address, Account]) =
    new TrieAccounts(underlyingTrie, Map())
}
final class TrieAccounts private (
    underlyingTrie:           MerklePatriciaTrie[Address, Account],
    private[ledger] var logs: Map[Address, Log[Account]]
) {

  def underlying = underlyingTrie

  def +(kv: (Address, Account)) = put(kv._1, kv._2)
  def -(address: Address) = remove(address)

  def get(address: Address): Option[Account] = {
    logs.get(address) match {
      case None => underlyingTrie.get(address) map { account =>
        logs += (address -> Original(account))
        account
      }
      case Some(Original(account)) => Some(account)
      case Some(Updated(account))  => Some(account)
      case Some(Removed(account))  => None
    }
  }

  def put(address: Address, account: Account): TrieAccounts = {
    val updatedLogs = logs + (address -> Updated(account))
    new TrieAccounts(underlyingTrie, updatedLogs)
  }

  def remove(address: Address): TrieAccounts = {
    val updatedLogs = logs + (address -> TrieAccounts.REMOVED_VALUE)
    new TrieAccounts(underlyingTrie, updatedLogs)
  }

  def flush(): TrieAccounts = {
    val flushedTrie = TrieAccounts.flush(this.underlyingTrie, this.logs)
    new TrieAccounts(flushedTrie, Map())
  }

  /**
   * Get root hash without flush original underlyingTrie by cloning it
   */
  def rootHash = {
    if (this.logs.isEmpty) {
      this.underlyingTrie.rootHash
    } else {
      val flushedTrie = TrieAccounts.flush(this.underlyingTrie.copy, this.logs)
      flushedTrie.rootHash
    }
  }

}
