package khipu.storage.datasource

import khipu.Hash

trait NodeDataSource extends DataSource[Hash, Array[Byte]] {
  type This <: NodeDataSource
}
