package khipu

import akka.util.ByteString

package object trie {

  trait ByteArrayEncoder[T] {
    def toBytes(input: T): Array[Byte]
  }

  trait ByteArrayDecoder[T] {
    def fromBytes(bytes: Array[Byte]): T
  }

  trait ByteArraySerializable[T] extends ByteArrayEncoder[T] with ByteArrayDecoder[T]

  // TODO not used anywhere ?
  private object byteStringSerializer extends ByteArraySerializable[ByteString] {
    override def toBytes(input: ByteString): Array[Byte] = input.toArray
    override def fromBytes(bytes: Array[Byte]): ByteString = ByteString(bytes)
  }

  object byteArraySerializable extends ByteArraySerializable[Array[Byte]] {
    override def toBytes(input: Array[Byte]): Array[Byte] = input
    override def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
  }

  object rlpDataWordSerializer extends ByteArraySerializable[DataWord] {
    // NOTE should rlp decode first before deser to DataWord, see rlp.toDataWord
    override def fromBytes(bytes: Array[Byte]): DataWord = rlp.toDataWord(bytes)
    override def toBytes(input: DataWord): Array[Byte] = rlp.encode(rlp.toRLPEncodable(input))
  }

  object hashDataWordSerializable extends ByteArrayEncoder[DataWord] {
    override def toBytes(input: DataWord): Array[Byte] = crypto.kec256(input.bytes)
  }

  def toHash(bytes: Array[Byte]): Array[Byte] = crypto.kec256(bytes)

  import khipu.rlp.RLPImplicits._
  val EMPTY_TRIE_HASH = crypto.kec256(rlp.encode(Array.emptyByteArray))
}
