package khipu.storage.datasource

trait KeyValueDataSource extends DataSource[Array[Byte], Array[Byte]] {
  type This <: KeyValueDataSource
}