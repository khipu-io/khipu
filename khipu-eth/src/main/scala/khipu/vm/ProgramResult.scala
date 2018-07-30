package khipu.vm

import akka.util.ByteString
import khipu.domain.{ Address, TxLogEntry }

/**
 * Represenation of the result of execution of a contract
 *
 * @param returnData bytes returned by the executed contract (set by [[RETURN]] opcode)
 * @param gasRemaining amount of gas remaining after execution
 * @param world represents changes to the world state
 * @param addressesToDelete list of addresses of accounts scheduled to be deleted
 * @param error defined when the program terminated abnormally
 */
final case class ProgramResult[W <: WorldState[W, S], S <: Storage[S]](
  returnData:             ByteString,
  gasRemaining:           Long,
  world:                  W,
  txLogs:                 Vector[TxLogEntry],
  gasRefund:              Long,
  addressesToDelete:      Set[Address],
  addressesTouched:       Set[Address],
  error:                  Option[ProgramError],
  isRevert:               Boolean,
  parallelRaceConditions: Set[ProgramState.ParallelRace],
  trace:                  Vector[String]
)
