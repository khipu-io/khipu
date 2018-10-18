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

  object rlpUInt256Serializer extends ByteArraySerializable[UInt256] {
    // NOTE should rlp decode first before deser to UInt256, see rlp.toUInt256
    override def fromBytes(bytes: Array[Byte]): UInt256 = rlp.toUInt256(bytes)
    override def toBytes(input: UInt256): Array[Byte] = rlp.encode(rlp.toRLPEncodable(input))
  }

  object hashUInt256Serializable extends ByteArrayEncoder[UInt256] {
    override def toBytes(input: UInt256): Array[Byte] = crypto.kec256(input.bytes)
  }

  def toHash(bytes: Array[Byte]): Array[Byte] = crypto.kec256(bytes)

  import khipu.rlp.RLPImplicits._
  val EmptyTrieHash = toHash(rlp.encode(Array.ofDim[Byte](0)))
}
