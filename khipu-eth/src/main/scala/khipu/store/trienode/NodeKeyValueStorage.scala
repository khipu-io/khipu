package khipu.store.trienode

import khipu.Hash
import khipu.util.SimpleMap

trait NodeKeyValueStorage extends SimpleMap[Hash, Array[Byte], NodeKeyValueStorage] {
  def tableName = ""
}
