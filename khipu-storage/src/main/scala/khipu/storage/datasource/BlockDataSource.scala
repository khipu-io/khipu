package khipu.storage.datasource

trait BlockDataSource extends DataSource[Long, Array[Byte]] {
  type This <: BlockDataSource

  def bestBlockNumber: Long
}
