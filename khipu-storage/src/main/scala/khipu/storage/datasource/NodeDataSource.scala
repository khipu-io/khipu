package khipu.storage.datasource

import khipu.Hash

trait NodeDataSource extends DataSource[Hash, Array[Byte]] {
  type This <: NodeDataSource

  final protected def isValueChanged(v1: Array[Byte], v2: Array[Byte]) = {
    if ((v1 eq null) && (v2 eq null)) {
      false
    } else if ((v1 eq null) || (v2 eq null)) {
      true
    } else {
      !java.util.Arrays.equals(v1, v2)
    }
  }
}
