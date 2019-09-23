package khipu.trie

import java.util.Arrays
import khipu.Changed
import khipu.Hash
import khipu.Log
import khipu.Original
import khipu.Removed
import khipu.Updated
import khipu.crypto
import khipu.rlp
import khipu.util.BytesUtil
import khipu.util.SimpleMap
import scala.annotation.tailrec

/**
 * The [[https://github.com/ethereum/wiki/wiki/Patricia-Tree Merkle Patricia Trie]]
 * is an '''immutable''' data structure. Whenever you make any changes to any
 * part of the trie it will result in a new trie with a different root with a
 * different hash. However, the new trie might have pointers to subtrees from
 * the previous state trie.
 *
 * Let's say you had a trie with 3 levels. If you want to change something on
 * the lowest level you need to create a new node on the 2nd level, and a new
 * node on the 1st level (the root) which will be recorded to the blockchain.
 *
 * Before persist(), all nodes of trie is kept in nodeLogs.
 *
 * All node are persisted as (key -> value) pair, where key is always the node.hash
 * which is the hash of value, and value is rlp encoded format of node (if it's leaf
 * or branch node, the original key/value is packed together as node's value).
 * Also see updateNodesToLogs and node.hash node.encoded
 */
object MerklePatriciaTrie {

  private case class NodeInsertResult(
    newNode: Node,
    changes: Vector[Changed[Node]] = Vector()
  )

  private case class NodeRemoveResult(
    hasChanged: Boolean,
    newNode:    Option[Node],
    changes:    Vector[Changed[Node]] = Vector()
  )

  final case class MPTException(message: String) extends RuntimeException(message)
  final case class MPTNodeMissingException(message: String, hash: Hash, storage: SimpleMap[Hash, Array[Byte]]) extends RuntimeException(message)

  private def matchingLength(a: Array[Byte], b: Array[Byte]): Int = {
    var i = 0
    while (i < a.length && i < b.length && a(i) == b(i)) {
      i += 1
    }
    i
  }

  def apply[K, V](source: SimpleMap[Hash, Array[Byte]])(implicit kSerializer: ByteArrayEncoder[K], vSerializer: ByteArraySerializable[V]): MerklePatriciaTrie[K, V] =
    new MerklePatriciaTrie[K, V](None, source, Map())(kSerializer, vSerializer)

  def apply[K, V](rootHash: Array[Byte], storage: SimpleMap[Hash, Array[Byte]])(implicit kSerializer: ByteArrayEncoder[K], vSerializer: ByteArraySerializable[V]): MerklePatriciaTrie[K, V] = {
    if (Arrays.equals(EMPTY_TRIE_HASH, rootHash)) {
      new MerklePatriciaTrie[K, V](None, storage, Map())(kSerializer, vSerializer)
    } else {
      new MerklePatriciaTrie[K, V](Some(rootHash), storage, Map())(kSerializer, vSerializer)
    }
  }
}
final class MerklePatriciaTrie[K, V] private (
    rootHashOpt:          Option[Array[Byte]],
    nodeStorage:          SimpleMap[Hash, Array[Byte]],
    private var nodeLogs: Map[Hash, Log[Array[Byte]]]
)(implicit kSerializer: ByteArrayEncoder[K], vSerializer: ByteArraySerializable[V]) extends SimpleMap[K, V] {
  type This = MerklePatriciaTrie[K, V]

  import MerklePatriciaTrie._

  // The root hash will be already here via a series of put/remove operations
  def rootHash: Array[Byte] = rootHashOpt.getOrElse(EMPTY_TRIE_HASH)

  def topic = nodeStorage.topic

  /**
   * Obtains the value asociated with the key passed, if there exists one.
   * The trie should keep in same state without side effect
   *
   * @param key
   * @return Option object with value if there exists one.
   * @throws khipu.trie.MerklePatriciaTrie.MPTException if there is any inconsistency in how the trie is build.
   */
  def get(key: K): Option[V] = {
    rootHashOpt flatMap { rootId =>
      val rootNode = getNode(rootId)
      val keyNibbles = HexPrefix.bytesToNibbles(kSerializer.toBytes(key))
      get(rootNode, keyNibbles).map(vSerializer.fromBytes)
    }
  }

  @tailrec
  private def get(node: Node, searchKey: Array[Byte]): Option[Array[Byte]] = node match {
    case LeafNode(key, value) =>
      if (Arrays.equals(key, searchKey)) {
        Some(value)
      } else {
        None
      }

    case extNode @ ExtensionNode(sharedKey, _) =>
      if (searchKey.length >= sharedKey.length) {
        val (commonKey, remainingKey) = BytesUtil.split(searchKey, sharedKey.length)
        if (Arrays.equals(sharedKey, commonKey)) {
          val nextNode = getNextNode(extNode)
          get(nextNode, remainingKey)
        } else {
          None
        }
      } else {
        None
      }

    case branch @ BranchNode(_, terminator) =>
      if (searchKey.length == 0) {
        terminator
      } else {
        getChild(branch, searchKey(0)) match {
          case Some(child) =>
            get(child, BytesUtil.tail(searchKey))
          case None =>
            None
        }
      }
  }

  private def getNextNode(extensionNode: ExtensionNode): Node =
    extensionNode.next match {
      case Right(node) => node
      case Left(hash)  => getNode(hash)
    }

  private def getChild(branchNode: BranchNode, pos: Int): Option[Node] =
    branchNode.children(pos) map {
      case Right(node) => node
      case Left(hash)  => getNode(hash)
    }

  /**
   * This function inserts a (key-value) pair into the trie. If the key is already asociated with another value it is updated.
   *
   * @param key
   * @param value
   * @return New trie with the (key-value) pair inserted.
   * @throws khipu.trie.MerklePatriciaTrie.MPTException if there is any inconsistency in how the trie is build.
   */
  override def put(key: K, value: V): This = {
    val keyNibbles = HexPrefix.bytesToNibbles(kSerializer.toBytes(key))

    rootHashOpt map { rootId =>
      val root = getNode(rootId)
      val NodeInsertResult(newRoot, changes) = put(root, keyNibbles, vSerializer.toBytes(value))
      new MerklePatriciaTrie(
        Some(newRoot.hash),
        nodeStorage,
        updateNodesToLogs(previousRootHash = Hash(rootHash), newRoot = Some(newRoot), changes = changes)
      )(kSerializer, vSerializer)

    } getOrElse {
      val newRoot = LeafNode(keyNibbles, vSerializer.toBytes(value))
      new MerklePatriciaTrie(
        Some(newRoot.hash),
        nodeStorage,
        updateNodesToLogs(Hash(rootHash), Some(newRoot), Vector(Updated(newRoot)))
      )(kSerializer, vSerializer)
    }
  }

  /**
   * This function deletes a (key-value) pair from the trie. If no (key-value) pair exists with the passed trie then there's no effect on it.
   *
   * @param key
   * @return New trie with the (key-value) pair associated with the key passed deleted from the trie.
   * @throws khipu.trie.MerklePatriciaTrie.MPTException if there is any inconsistency in how the trie is build.
   */
  override def remove(key: K): This = {
    rootHashOpt map { rootId =>
      val keyNibbles = HexPrefix.bytesToNibbles(bytes = kSerializer.toBytes(key))
      val root = getNode(rootId)
      remove(root, keyNibbles) match {
        case NodeRemoveResult(true, Some(newRoot), changes) =>
          new MerklePatriciaTrie(
            Some(newRoot.hash),
            nodeStorage,
            updateNodesToLogs(previousRootHash = Hash(rootHash), newRoot = Some(newRoot), changes = changes)
          )(kSerializer, vSerializer)

        case NodeRemoveResult(true, None, changes) =>
          new MerklePatriciaTrie(
            None,
            nodeStorage,
            updateNodesToLogs(previousRootHash = Hash(rootHash), newRoot = None, changes = changes)
          )(kSerializer, vSerializer)

        case NodeRemoveResult(false, _, _) => this
      }
    } getOrElse {
      this
    }
  }

  private def put(node: Node, searchKey: Array[Byte], value: Array[Byte]): NodeInsertResult = node match {
    case leafNode: LeafNode           => putInLeafNode(leafNode, searchKey, value)
    case extensionNode: ExtensionNode => putInExtensionNode(extensionNode, searchKey, value)
    case branchNode: BranchNode       => putInBranchNode(branchNode, searchKey, value)
  }

  private def putInLeafNode(node: LeafNode, searchKey: Array[Byte], value: Array[Byte]): NodeInsertResult = {
    val LeafNode(existingKey, storedValue) = node
    matchingLength(existingKey, searchKey) match {
      case ml if ml == existingKey.length && ml == searchKey.length =>
        // We are trying to insert a leaf node that has the same key as this one but different value so we need to
        // replace it
        val newLeafNode = LeafNode(existingKey, value)
        NodeInsertResult(
          newNode = newLeafNode,
          changes = Vector(Removed(node), Updated(newLeafNode))
        )
      case 0 =>
        // There is no common prefix between the node which means that we need to replace this leaf node
        val (temporalBranchNode, maybeNewLeaf) = if (existingKey.length == 0) {
          // This node has no key so it should be stored as branch's value
          (BranchNode.withValueOnly(storedValue), None)
        } else {
          // The leaf should be put inside one of new branch nibbles
          val newLeafNode = LeafNode(BytesUtil.tail(existingKey), storedValue)
          (BranchNode.withSingleChild(existingKey(0), newLeafNode, None), Some(newLeafNode))
        }
        val NodeInsertResult(newBranchNode: BranchNode, changes) = put(temporalBranchNode, searchKey, value)
        NodeInsertResult(
          newNode = newBranchNode,
          changes = (changes :+ Removed(node)) ++ maybeNewLeaf.map(Updated(_))
        )
      case ml =>
        // Partially shared prefix, we replace the leaf with an extension and a branch node
        val (searchKeyPrefix, searchKeySuffix) = BytesUtil.split(searchKey, ml)
        val temporalNode = if (ml == existingKey.length) {
          BranchNode.withValueOnly(storedValue)
        } else {
          LeafNode(BytesUtil.drop(existingKey, ml), storedValue)
        }
        val NodeInsertResult(newBranchNode: BranchNode, changes) = put(temporalNode, searchKeySuffix, value)
        val newExtNode = ExtensionNode(searchKeyPrefix, newBranchNode)
        NodeInsertResult(
          newNode = newExtNode,
          changes = (changes :+ Removed(node)) :+ Updated(newExtNode)
        )
    }
  }

  private def putInExtensionNode(extensionNode: ExtensionNode, searchKey: Array[Byte], value: Array[Byte]): NodeInsertResult = {
    val ExtensionNode(sharedKey, next) = extensionNode
    matchingLength(sharedKey, searchKey) match {
      case 0 =>
        // There is no common prefix with the node which means we have to replace it for a branch node
        val sharedKeyHead = sharedKey(0)
        val (temporalBranchNode, maybeNewLeaf) = if (sharedKey.length == 1) {
          // Direct extension, we just replace the extension with a branch
          (BranchNode.withSingleChild(sharedKeyHead, next, None), None)
        } else {
          // The new branch node will have an extension that replaces current one
          val newExtNode = ExtensionNode(BytesUtil.tail(sharedKey), next)
          (BranchNode.withSingleChild(sharedKeyHead, newExtNode, None), Some(newExtNode))
        }
        val NodeInsertResult(newBranchNode: BranchNode, changes) = put(temporalBranchNode, searchKey, value)
        NodeInsertResult(
          newNode = newBranchNode,
          changes = (changes :+ Removed(extensionNode)) ++ maybeNewLeaf.map(Updated(_))
        )
      case ml if ml == sharedKey.length =>
        // Current extension node's key is a prefix of the one being inserted, so we insert recursively on the extension's child
        val NodeInsertResult(newChild: BranchNode, changes) = put(getNextNode(extensionNode), BytesUtil.drop(searchKey, ml), value)
        val newExtNode = ExtensionNode(sharedKey, newChild)
        NodeInsertResult(
          newNode = newExtNode,
          changes = (changes :+ Removed(extensionNode)) :+ Updated(newExtNode)
        )
      case ml =>
        // Partially shared prefix, we have to replace the node with an extension with the shared prefix
        val (sharedKeyPrefix, sharedKeySuffix) = BytesUtil.split(sharedKey, ml)
        val temporalExtensionNode = ExtensionNode(sharedKeySuffix, next)
        val NodeInsertResult(newBranchNode: BranchNode, changes) = put(temporalExtensionNode, BytesUtil.drop(searchKey, ml), value)
        val newExtNode = ExtensionNode(sharedKeyPrefix, newBranchNode)
        NodeInsertResult(
          newNode = newExtNode,
          changes = (changes :+ Removed(extensionNode)) :+ Updated(newExtNode)
        )
    }
  }

  private def putInBranchNode(branchNode: BranchNode, searchKey: Array[Byte], value: Array[Byte]): NodeInsertResult = {
    val BranchNode(children, _) = branchNode
    if (searchKey.isEmpty) {
      // The key is empty, the branch node should now be a terminator node with the new value asociated with it
      val newBranchNode = BranchNode(children, Some(value))
      NodeInsertResult(
        newNode = newBranchNode,
        changes = Vector(Removed(branchNode), Updated(newBranchNode))
      )
    } else {
      // Non empty key, we need to insert the value in the correct branch node's child
      val searchKeyHead = searchKey(0).toInt
      val searchKeyTail = BytesUtil.tail(searchKey)
      if (children(searchKeyHead).isDefined) {
        // The associated child is not empty, we recursively insert in that child
        val NodeInsertResult(changedChild, changes) = put(getChild(branchNode, searchKeyHead).get, searchKeyTail, value)
        val newBranchNode = branchNode.updateChild(searchKeyHead, changedChild)
        NodeInsertResult(
          newNode = newBranchNode,
          changes = (changes :+ Removed(branchNode)) :+ Updated(newBranchNode)
        )
      } else {
        // The associated child is empty, we just replace it with a leaf
        val newLeafNode = LeafNode(searchKeyTail, value)
        val newBranchNode = branchNode.updateChild(searchKeyHead, newLeafNode)
        NodeInsertResult(
          newNode = newBranchNode,
          changes = Vector(Removed(branchNode), Updated(newLeafNode), Updated(newBranchNode))
        )
      }
    }
  }

  private def remove(node: Node, searchKey: Array[Byte]): NodeRemoveResult = node match {
    case leafNode: LeafNode           => removeFromLeafNode(leafNode, searchKey)
    case extensionNode: ExtensionNode => removeFromExtensionNode(extensionNode, searchKey)
    case branchNode: BranchNode       => removeFromBranchNode(branchNode, searchKey)
  }

  private def removeFromBranchNode(node: BranchNode, searchKey: Array[Byte]): NodeRemoveResult = (node, searchKey.isEmpty) match {
    // They key matches a branch node but it's value doesn't match the key
    case (BranchNode(_, None), true) => NodeRemoveResult(hasChanged = false, newNode = None)
    // We want to delete Branch node value

    case (BranchNode(children, _), true) =>
      // We need to remove old node and fix it because we removed the value
      val fixedNode = fix(BranchNode(children, None), Vector())
      NodeRemoveResult(hasChanged = true, newNode = Some(fixedNode), changes = Vector(Removed(node), Updated(fixedNode)))

    case (branchNode @ BranchNode(children, optStoredValue), false) =>
      // We might be trying to remove a node that's inside one of the 16 mapped nibbles
      val searchKeyHead = searchKey(0)
      getChild(branchNode, searchKeyHead) map { child =>
        // Child has been found so we try to remove it
        remove(child, BytesUtil.tail(searchKey)) match {
          case NodeRemoveResult(true, maybeNewChild, changes) =>
            // Something changed in a child so we need to fix
            val nodeToFix = maybeNewChild map { newChild =>
              branchNode.updateChild(searchKeyHead, newChild)
            } getOrElse {
              children(searchKeyHead) = None
              BranchNode(children, optStoredValue)
            }
            val nodesToUpdate = changes.collect { case Updated(node) => node }
            val fixedNode = fix(nodeToFix, nodesToUpdate)
            NodeRemoveResult(
              hasChanged = true,
              newNode = Some(fixedNode),
              changes = (changes :+ Removed(node)) :+ Updated(fixedNode)
            )

          // No removal made on children, so we return without any change
          case NodeRemoveResult(false, _, changes) =>
            NodeRemoveResult(
              hasChanged = false,
              newNode = None,
              changes = changes
            )
        }
      } getOrElse {
        // Child not found in this branch node, so key is not present
        NodeRemoveResult(hasChanged = false, newNode = None)
      }
  }

  private def removeFromLeafNode(leafNode: LeafNode, searchKey: Array[Byte]): NodeRemoveResult = {
    val LeafNode(existingKey, _) = leafNode
    if (Arrays.equals(existingKey, searchKey)) {
      // We found the node to delete
      NodeRemoveResult(hasChanged = true, newNode = None, changes = Vector(Removed(leafNode)))
    } else {
      NodeRemoveResult(hasChanged = false, newNode = None)
    }
  }

  private def removeFromExtensionNode(extensionNode: ExtensionNode, searchKey: Array[Byte]): NodeRemoveResult = {
    val ExtensionNode(sharedKey, _) = extensionNode
    val cp = matchingLength(sharedKey, searchKey)
    if (cp == sharedKey.length) {
      // A child node of this extension is removed, so move forward
      remove(getNextNode(extensionNode), BytesUtil.drop(searchKey, cp)) match {
        case NodeRemoveResult(true, maybeNewChild, changes) =>
          // If we changed the child, we need to fix this extension node
          maybeNewChild map { newChild =>
            val toFix = ExtensionNode(sharedKey, newChild)
            val nodesToUpdate = changes.collect { case Updated(node) => node }
            val fixedNode = fix(toFix, nodesToUpdate)
            NodeRemoveResult(
              hasChanged = true,
              newNode = Some(fixedNode),
              changes = (changes :+ Removed(extensionNode)) :+ Updated(fixedNode)
            )
          } getOrElse {
            throw MPTException("A trie with newRoot extension should have at least 2 values stored")
          }

        case NodeRemoveResult(false, _, changes) =>
          NodeRemoveResult(
            hasChanged = false,
            newNode = None,
            changes = changes
          )
      }
    } else NodeRemoveResult(hasChanged = false, newNode = Some(extensionNode))
  }

  /**
   * Given a node which may be in an invalid state, fix it such that it is then in a valid state. Invalid state means:
   *   - Branch node where there is only a single entry;
   *   - Extension node followed by anything other than a Branch node.
   *
   * @param node         that may be in an invalid state.
   * @param nodeStorage  to obtain the nodes referenced in the node that may be in an invalid state.
   * @param notStoredYet to obtain the nodes referenced in the node that may be in an invalid state,
   *                     if they were not yet inserted into the nodeStorage.
   * @return fixed node.
   * @throws khipu.trie.MerklePatriciaTrie.MPTException if there is any inconsistency in how the trie is build.
   */
  @tailrec
  private def fix(node: Node, notStoredYet: Vector[Node]): Node = node match {
    case BranchNode(children, optStoredValue) =>
      var usedIdxes = List[Int]()
      var i = 0
      while (i < children.length) {
        children(i) match {
          case Some(_) => usedIdxes = i :: usedIdxes
          case None    =>
        }
        i += 1
      }

      (usedIdxes, optStoredValue) match {
        case (Nil, None) =>
          throw MPTException("Branch with no subvalues")
        case (index :: Nil, None) =>
          val temporalExtNode = ExtensionNode(Array[Byte](index.toByte), children(index).get)
          fix(temporalExtNode, notStoredYet)
        case (Nil, Some(value)) =>
          LeafNode(Array.emptyByteArray, value)
        case _ =>
          node
      }

    case extensionNode @ ExtensionNode(sharedKey, _) =>
      val nextNode = extensionNode.next match {
        case Left(nextHash) =>
          // If the node is not in the extension node then it might be a node to be inserted at the end of this remove
          // so we search in this list too
          notStoredYet.find(n => Arrays.equals(n.hash, nextHash)).getOrElse(
            getNextNode(extensionNode) // We search for the node in the db
          )
        case Right(nextNodeOnExt) => nextNodeOnExt
      }
      val newNode = nextNode match {
        // Compact Two extensions into one
        case ExtensionNode(subSharedKey, subNext) => ExtensionNode(BytesUtil.concat(sharedKey, subSharedKey), subNext)
        // Compact the extension and the leaf into the same leaf node
        case LeafNode(subRemainingKey, subValue)  => LeafNode(BytesUtil.concat(sharedKey, subRemainingKey), subValue)
        // It's ok
        case _: BranchNode                        => node
      }
      newNode

    case _ => node
  }

  /**
   * Compose trie by toRemove and toUpsert
   *
   * @param toRemove which includes all the keys to be removed from the KeyValueStore.
   * @param toUpsert which includes all the (key-value) pairs to be inserted into the KeyValueStore.
   *                 If a key is already in the DataSource its value will be updated.
   * @return the new trie after the removals and insertions were done.
   */
  override def update(toRemove: Iterable[K], toUpsert: Iterable[(K, V)]): This = {
    throw new UnsupportedOperationException("Use put/remove")
  }

  private def updateNodesToLogs(
    previousRootHash: Hash,
    newRoot:          Option[Node],
    changes:          Vector[Changed[Node]]
  ): Map[Hash, Log[Array[Byte]]] = {
    val rootCapped = newRoot.map(_.capped).getOrElse(Array.emptyByteArray)

    val orderlyDedeplucated = changes.foldLeft(Map[Hash, Changed[Node]]()) {
      case (acc, nodeToRemove @ Removed(node)) => acc + (Hash(node.hash) -> nodeToRemove)
      case (acc, nodeToUpdate @ Updated(node)) => acc + (Hash(node.hash) -> nodeToUpdate)
    }

    val (toRemove, toUpdate) = orderlyDedeplucated.foldLeft(Map[Hash, Log[Array[Byte]]](), Map[Hash, Log[Array[Byte]]]()) {
      case ((toRemove, toUpdate), (hash, Removed(node))) =>
        val nCapped = node.capped
        if (nCapped.length == 32 || hash == previousRootHash) {
          (toRemove + (hash -> Removed(Array.emptyByteArray)), toUpdate)
        } else {
          (toRemove, toUpdate)
        }
      case ((toRemove, toUpdate), (hash, Updated(node))) =>
        val nCapped = node.capped
        if (nCapped.length == 32 || Arrays.equals(nCapped, rootCapped)) {
          (toRemove, toUpdate + (hash -> Updated(node.encoded)))
        } else {
          (toRemove, toUpdate)
        }
    }

    nodeLogs ++ toRemove ++ toUpdate
  }

  // --- node storage related
  private def getNode(nodeId: Array[Byte]): Node = {
    val encoded = if (nodeId.length < 32) {
      nodeId
    } else {
      val id = Hash(nodeId)
      nodeLogs.get(id) match {
        case Some(Original(x)) => x
        case Some(Updated(x))  => x
        case None =>
          nodeStorage.get(id) match {
            case Some(x) =>
              nodeLogs += (id -> Original(x))
              x
            case None =>
              throw MPTNodeMissingException(s"Node not found ${khipu.toHexString(nodeId)}, trie is inconsistent", id, nodeStorage)
          }
        case Some(Removed(_)) =>
          throw MPTNodeMissingException(s"Node has been deleted ${khipu.toHexString(nodeId)}, trie is inconsistent", id, nodeStorage)
      }
    }

    rlp.decode[Node](encoded)(Node.nodeDec)
  }

  def persist(): MerklePatriciaTrie[K, V] = {
    nodeStorage.update(changes)
    this
  }

  def changes: Map[Hash, Option[Array[Byte]]] = {
    nodeLogs.collect {
      case (k, Removed(_)) => k -> None
      case (k, Updated(v)) => k -> Some(v)
    }
  }

  def copy: MerklePatriciaTrie[K, V] = {
    new MerklePatriciaTrie[K, V](rootHashOpt, nodeStorage, nodeLogs)(kSerializer, vSerializer)
  }

}
