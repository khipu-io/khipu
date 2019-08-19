package khipu.storage

import java.net.URI
import khipu.storage.datasource.DataSource

/**
 * This class is used to store discovered nodes
 *   Value: stored nodes list
 */
final class KnownNodesStorage(val source: DataSource) extends KeyValueStorage[String, Set[String]] {
  type This = KnownNodesStorage

  val key = "KnownNodes"

  def topic = source.topic

  val namespace: Array[Byte] = Namespaces.KnownNodes
  def keyToBytes(k: String): Array[Byte] = k.getBytes
  def valueToBytes(v: Set[String]): Array[Byte] = v.mkString(" ").getBytes
  def valueFromBytes(bytes: Array[Byte]): Set[String] = new String(bytes).split(' ').toSet

  protected def apply(dataSource: DataSource): KnownNodesStorage = new KnownNodesStorage(dataSource)

  def getKnownNodes: Set[URI] = {
    get(key).getOrElse(Set()).filter(_.nonEmpty).map(new URI(_))
  }

  def updateKnownNodes(toAdd: Set[URI] = Set(), toRemove: Set[URI] = Set()): This = {
    val updated = (getKnownNodes ++ toAdd) -- toRemove
    put(key, updated.map(_.toString))
  }

}
