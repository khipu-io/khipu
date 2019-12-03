package khipu.network.rlpx.discovery

import java.util._

/**
 * Kademlia Table
 * Nodes in the Discovery Protocol keep information about other nodes in their
 * neighborhood. Neighbor nodes are stored in a routing table consisting of
 * 'k-buckets'. For each 0 ≤ i < 256, every node keeps a k-bucket for nodes of
 * distance between 2^i and 2^(i+1) from itself.
 *
 * The Node Discovery Protocol uses k = 16, i.e. every k-bucket contains up to
 * 16 node entries. The entries are sorted by time last seen — least-recently
 * seen node at the head, most-recently seen at the tail.
 *
 * Whenever a new node N1 is encountered, it can be inserted into the
 * corresponding bucket. If the bucket contains less than k entries N1 can simply
 * be added as the first entry. If the bucket already contains k entries, the
 * least recently seen node in the bucket, N2, needs to be revalidated by sending
 *  a Ping packet. If no reply is received from N2 it is considered dead, removed
 *  and N2 added to the front of the bucket.
 */
class KRoutingTable(val homeNodeId: Array[Byte], includeHomeNode: Option[Node]) {
  private val nodes = new ArrayList[NodeEntry]()
  private val buckets = Array.fill[KBucket](KademliaOptions.ID_LENGTH_IN_BITS)(new KBucket())

  includeHomeNode match {
    case Some(node) => tryAddNode(node)
    case None       =>
  }

  def tryAddNode(n: Node): Option[Node] = synchronized {
    val e = NodeEntry(homeNodeId, n)
    buckets(e.bucketId).tryAddNode(e) match {
      case Some(leastRecentlySeen) => Some(leastRecentlySeen.node)
      case None =>
        if (!nodes.contains(e)) {
          nodes.add(e)
        }
        None
    }
  }

  def dropNode(n: Node): Unit = synchronized {
    val e = NodeEntry(homeNodeId, n)
    buckets(e.bucketId).dropNode(e)
    nodes.remove(e)
  }

  def contains(n: Node): Boolean = synchronized {
    val e = NodeEntry(homeNodeId, n)
    val itr = buckets.iterator
    while (itr.hasNext) {
      val b = itr.next
      if (b.nodes.contains(e)) {
        return true
      }
    }
    false
  }

  def touchNode(n: Node): Unit = synchronized {
    val e = NodeEntry(homeNodeId, n)
    val itr = buckets.iterator
    var break = false
    while (itr.hasNext && !break) {
      val b = itr.next
      val idx = b.nodes.indexOf(e)
      if (idx >= 0) {
        b.nodes.get(idx).touch()
        break = true
      }
    }
  }

  def getBucketsCount: Int = synchronized {
    var i = 0
    val itr = buckets.iterator
    while (itr.hasNext) {
      val b = itr.next
      if (b.getNodesCount > 0) {
        i += 1
      }
    }
    i
  }

  def getBuckets: Array[KBucket] = synchronized {
    buckets
  }

  def getNodeCount: Int = synchronized {
    nodes.size
  }

  def getAllNodes: List[NodeEntry] = synchronized {
    val nodes = new ArrayList[NodeEntry]()

    val itr = buckets.iterator
    while (itr.hasNext) {
      val b = itr.next
      val nodesItr = b.nodes.iterator
      while (nodesItr.hasNext) {
        val e = nodesItr.next
        if (!java.util.Arrays.equals(e.node.id.toArray, homeNodeId)) {
          nodes.add(e)
        }
      }
    }

    nodes
  }

  def getClosestNodes(targetId: Array[Byte]): List[Node] = synchronized {
    var closestEntries = getAllNodes
    Collections.sort(closestEntries, new DistanceComparator(targetId))
    if (closestEntries.size > KademliaOptions.BUCKET_SIZE) {
      closestEntries = closestEntries.subList(0, KademliaOptions.BUCKET_SIZE)
    }

    val closestNodes = new ArrayList[Node]()
    val itr = closestEntries.iterator
    while (itr.hasNext) {
      val e = itr.next
      //if (!e.node.isDiscoveryNode()) {
      closestNodes.add(e.node)
      //}
    }

    closestNodes
  }

  override def toString() = {
    val sb = new StringBuilder().append("KRoutingTable(")
    buckets.foreach { bucket =>
      sb.append(bucket.getNodesCount).append(",")
    }
    sb.append(")")
    sb.toString
  }
}

object NodeEntry {
  /**
   * Logarithmic distance between a and b // log2(a ^ b) ??
   * Compute the xor of this and to
   * Get the index i of the first set bit of the xor returned NodeId
   * The distance between them is ID_LENGTH - i
   *
   * Each bucket represents a distance from our node, calculated by XORing the
   * hash of the peer's ID with ours (key from now on) and counting leading 0,
   * i.e. boils down to how many prefix bits our keys share. This is called
   * distance (rather, closeness). The higher the value, the closer they are to us
   *
   * See discuss at https://github.com/libp2p/go-libp2p-kad-dht/issues/194
   *
   * ---------------------------------------------------------------------------
   * Actuall, this distance algorithm is a directly bucket-id calculating. For
   * example:  an xor distance of 0000 ... 0000 0101, the bucket distance is 3,
   * thus will be put into bucket 3 (2 if count from 0).
   *
   * Why? Considerating a binary network partition:
   *
   * For each node, the entire binary tree can be split according to its own
   * perspective. The rule of splitting is: starting from the root node,
   * splitting out the subtree that does not contain itself; then splitting the
   * next subtree that does not contain itself in the remaining subtrees; and
   * so on, until the end Only myself.
   *
   * The last bucket that is split (the bucket closest to itself) differs only
   * in the last bit, which XOR value is 0000 ... 0000 0001, the highest bit is
   * 1, put into the bucket #1; The next to last bucket, which XOR value is
   * 0000 ... 0000 001X, the highest set bit is 2, put into the bucket #2; And
   * so on ...
   * ---------------------------------------------------------------------------
   * Kademlia table after a while:
   * 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
   * 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
   * 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
   * 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,1,3,10,16,16,16,16
   */
  def distance_ori(ownerId: Array[Byte], targetId: Array[Byte]): Int = {
    val length = math.min(ownerId.length, targetId.length)
    var pos = 0
    var i = 0
    var break = false
    while (i < length && !break) {
      val xor = ((ownerId(i) ^ targetId(i)) & 0xFF).toByte
      if (xor == 0) {
        pos += 8
        i += 1
      } else {
        var p = 7
        while (p >= 0 && ((xor >> p) & 0x01) == 0) {
          pos += 1
          p -= 1
        }

        break = true
      }
    }

    // id_length_in_bits (length * 8) - pos
    KademliaOptions.ID_LENGTH_IN_BITS - pos
  }

  /**
   * Distance is the count of unset bits of XOR
   * ---------------------------------------------------------------------------
   * Kademlia table after a while:
   * 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,1,2,1,3,1,2,4,6,10,4,4,8,13,15,11,14,16,16,16,16,16,16,16,16,16,16,16,16,
   * 16,16,15,16,16,16,16,16,14,16,10,11,12,12,6,7,8,4,3,7,2,2,2,0,5,0,2,1,1,1,0,1,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
   * 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
   * 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
   */
  def distance(ownerId: Array[Byte], targetId: Array[Byte]): Int = {
    val length = math.min(ownerId.length, targetId.length)
    var count = 0 // count of 0 bits
    var i = 0
    var break = false
    while (i < length && !break) {
      val xor = ((ownerId(i) ^ targetId(i)) & 0xFF).toByte
      if (xor == 0) {
        count += 8
      } else {
        var p = 7
        while (p >= 0 && ((xor >> p) & 0x01) == 0) {
          if (((xor >> p) & 0x01) == 0) {
            count += 1
          }
          p -= 1
        }
      }

      i += 1
    }

    // id_length_in_bits (length * 8) - pos
    count
  }

  private def bucketId(bucketDistance: Int): Int = {
    val id = bucketDistance - 1
    // if we are puting a node into it's own routing table, the bucket ID will 
    // be -1, so let's just keep it in bucket 0
    if (id < 0) 0 else id
  }

  def apply(node: Node): NodeEntry = {
    val ownerId = node.id.toArray
    val bId = bucketId(distance(ownerId, node.id.toArray))
    NodeEntry(ownerId, node, node.toString, bId, System.currentTimeMillis)
  }

  def apply(ownerId: Array[Byte], node: Node): NodeEntry = {
    val bId = bucketId(distance(ownerId, node.id.toArray))
    NodeEntry(ownerId, node, node.toString, bId, System.currentTimeMillis)
  }
}
final case class NodeEntry(
    ownerId:      Array[Byte],
    node:         Node,
    id:           String,
    bucketId:     Int,
    var modified: Long
) {

  def touch() {
    modified = System.currentTimeMillis
  }

  override def equals(o: Any) = {
    o match {
      case e: NodeEntry => this.id == e.id
      case _            => false
    }
  }

  override def hashCode = node.hashCode
}

final class KBucket() {
  val nodes = new ArrayList[NodeEntry]()

  /**
   * @return evictCandidate
   */
  def tryAddNode(e: NodeEntry): Option[NodeEntry] = synchronized {
    if (!nodes.contains(e)) {
      if (nodes.size >= KademliaOptions.BUCKET_SIZE) {
        // return the least recently seen node
        Collections.sort(nodes, TimeComparator)
        Some(nodes.get(0))
      } else {
        nodes.add(e)
        None
      }
    } else {
      None
    }
  }

  def dropNode(entry: NodeEntry): Unit = synchronized {
    val itr = nodes.iterator
    var break = false
    while (itr.hasNext && !break) {
      val e = itr.next
      if (e.id == entry.id) {
        nodes.remove(e)
        break = true
      }
    }
  }

  def getNodesCount: Int = {
    nodes.size
  }
}

object TimeComparator extends Ordering[NodeEntry] {
  override def compare(e1: NodeEntry, e2: NodeEntry): Int = {
    val t1 = e1.modified
    val t2 = e2.modified

    if (t1 > t2) {
      -1
    } else if (t1 > t2) {
      1
    } else {
      0
    }
  }
}

final class DistanceComparator(targetId: Array[Byte]) extends Ordering[NodeEntry] {
  override def compare(e1: NodeEntry, e2: NodeEntry): Int = {
    val d1 = NodeEntry.distance(targetId, e1.node.id.toArray)
    val d2 = NodeEntry.distance(targetId, e2.node.id.toArray)

    if (d1 > d2) {
      1
    } else if (d1 < d2) {
      -1
    } else {
      0
    }
  }
}