package khipu.network.p2p

object Message {
  type Version = Int
}

trait Message {
  def code: Int
}

trait MessageSerializable extends Message {
  def toBytes: Array[Byte]
  def underlying: Message
}

