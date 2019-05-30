package khipu.store

import java.net.URI
import khipu.store.datasource.DataSource

/**
 * This class is used to store discovered nodes
 *   Value: stored nodes list
 */
final class KnownNodesStorage(val source: DataSource) extends KeyValueStorage[String, Set[String]] {
  type This = KnownNodesStorage

  val key = "KnownNodes"

  val namespace: Array[Byte] = Namespaces.KnownNodesNamespace
  def keySerializer: String => Array[Byte] = _.getBytes
  def valueSerializer: Set[String] => Array[Byte] = _.mkString(" ").getBytes
  def valueDeserializer: Array[Byte] => Set[String] = (valueBytes: Array[Byte]) => new String(valueBytes).split(' ').toSet

  protected def apply(dataSource: DataSource): KnownNodesStorage = new KnownNodesStorage(dataSource)

  def getKnownNodes: Set[URI] = {
    get(key).getOrElse(Set()).filter(_.nonEmpty).map(new URI(_))
  }

  def updateKnownNodes(toAdd: Set[URI] = Set(), toRemove: Set[URI] = Set()): KnownNodesStorage = {
    val updated = (getKnownNodes ++ toAdd) -- toRemove
    put(key, updated.map(_.toString))
  }

}
