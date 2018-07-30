package khipu

package object rlp {

  val EmptyRLPData = encode(RLPValue(Array[Byte]()))
  val EmptyRLPList = encode(RLPList())

  final case class RLPException(message: String) extends RuntimeException(message)

  sealed trait RLPEncodeable
  final case class RLPList(items: RLPEncodeable*) extends RLPEncodeable
  final case class RLPValue(bytes: Array[Byte]) extends RLPEncodeable {
    override def toString: String = s"RLPValue(${khipu.toHexString(bytes)})"
  }

  trait RLPEncoder[T] {
    def encode(obj: T): RLPEncodeable
  }

  trait RLPDecoder[T] {
    def decode(rlp: RLPEncodeable): T
  }

  def encode[T](input: T)(implicit enc: RLPEncoder[T]): Array[Byte] = RLP.encode(enc.encode(input))
  def encode(input: RLPEncodeable): Array[Byte] = RLP.encode(input)

  def decode[T](data: Array[Byte])(implicit dec: RLPDecoder[T]): T = dec.decode(RLP.rawDecode(data))
  def decode[T](data: RLPEncodeable)(implicit dec: RLPDecoder[T]): T = dec.decode(data)

  def rawDecode(input: Array[Byte]): RLPEncodeable = RLP.rawDecode(input)

  /**
   * This function calculates the next element item based on a previous element starting position. It's meant to be
   * used while decoding a stream of RLPEncoded Items.
   *
   * @param data Data with encoded items
   * @param pos  Where to start. This value should be a valid start element position in order to be able to calculate
   *             next one
   * @return Next item position
   * @throws RLPException if there is any error
   */
  def nextElementIndex(data: Array[Byte], pos: Int): Int = RLP.getItemBounds(data, pos).end + 1

  trait RLPSerializable {
    def toRLPEncodable: RLPEncodeable
    def toBytes: Array[Byte] = encode(this.toRLPEncodable)
  }

}
