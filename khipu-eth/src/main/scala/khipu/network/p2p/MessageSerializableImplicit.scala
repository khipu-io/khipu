package khipu.network.p2p

/**
 * Helper class
 */
abstract class MessageSerializableImplicit[T <: Message](val msg: T) extends MessageSerializable {

  override def equals(any: Any): Boolean = {
    any match {
      case that: MessageSerializableImplicit[T] => (this eq that) || (that.msg == msg)
      case _                                    => false
    }
  }

  override def hashCode(): Int = msg.hashCode()
}
