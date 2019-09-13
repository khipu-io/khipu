package khipu.storage

import java.net.URI
import khipu.storage.datasource.KeyValueDataSource
import khipu.util.BytesUtil

/**
 * This class is used to store discovered nodes
 *   Value: stored nodes list
 */
final class KnownNodesStorage(val source: KeyValueDataSource) extends KeyValueStorage[Array[Byte], Set[String]] {
  type This = KnownNodesStorage

  private val KEY = BytesUtil.concat(Namespaces.KnownNodes, "KnownNodes".getBytes)

  def topic = source.topic

  def keyToBytes(k: Array[Byte]): Array[Byte] = k
  def valueToBytes(v: Set[String]): Array[Byte] = v.mkString(" ").getBytes
  def valueFromBytes(bytes: Array[Byte]): Set[String] = new String(bytes).split(' ').toSet

  protected def apply(dataSource: KeyValueDataSource): KnownNodesStorage = new KnownNodesStorage(dataSource)

  def getKnownNodes: Set[URI] = {
    get(KEY).getOrElse(Set()).filter(_.nonEmpty).map(new URI(_))
  }

  def updateKnownNodes(toAdd: Set[URI] = Set(), toRemove: Set[URI] = Set()): This = {
    val updated = (getKnownNodes ++ toAdd) -- toRemove
    put(KEY, updated.map(_.toString))
  }

}
