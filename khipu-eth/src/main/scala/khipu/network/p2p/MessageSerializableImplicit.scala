package khipu.network.p2p

/**
 * Helper class
 */
abstract class MessageSerializableImplicit[T <: Message](val msg: T) extends MessageSerializable {

  override def equals(that: Any): Boolean = that match {
    case that: MessageSerializableImplicit[T] => that.msg equals msg
    case _                                    => false
  }

  override def hashCode(): Int = msg.hashCode()
}
