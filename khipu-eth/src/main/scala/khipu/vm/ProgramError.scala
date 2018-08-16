package khipu.vm

/**
 * Marker trait for errors that may occur during program execution
 */
sealed trait ProgramError
case object OutOfGas extends ProgramError
case object PrecompiledContractFailed extends ProgramError
final case class InvalidOpCode(code: Byte) extends ProgramError {
  override def toString: String =
    f"InvalidOpCode(0x${code.toInt & 0xff}%02x)"
}
final case class InvalidJump(dest: UInt256) extends ProgramError {
  override def toString: String =
    f"InvalidJump(${dest.toHexString})"
}
case object StaticCallModification extends ProgramError {
  override def toString = "StaticCallModification Attempt to call a state modifying opcode inside STATICCALL"
}
case object ArithmeticException extends ProgramError {
  override def toString = "ArithmeticException"
}

sealed trait StackError extends ProgramError
case object StackOverflow extends StackError
case object StackUnderflow extends StackError
