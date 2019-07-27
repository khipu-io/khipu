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

  object rlpEvmWordSerializer extends ByteArraySerializable[EvmWord] {
    // NOTE should rlp decode first before deser to EvmWord, see rlp.toEvmWord
    override def fromBytes(bytes: Array[Byte]): EvmWord = rlp.toEvmWord(bytes)
    override def toBytes(input: EvmWord): Array[Byte] = rlp.encode(rlp.toRLPEncodable(input))
  }

  object hashEvmWordSerializable extends ByteArrayEncoder[EvmWord] {
    override def toBytes(input: EvmWord): Array[Byte] = crypto.kec256(input.bytes)
  }

  def toHash(bytes: Array[Byte]): Array[Byte] = crypto.kec256(bytes)

  import khipu.rlp.RLPImplicits._
  val EMPTY_TRIE_HASH = crypto.kec256(rlp.encode(Array.emptyByteArray))
}
