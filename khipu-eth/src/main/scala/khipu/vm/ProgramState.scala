package khipu.vm

import akka.util.ByteString
import khipu.UInt256
import khipu.domain.Address
import khipu.domain.TxLogEntry

object ProgramState {
  trait ParallelRace
  case object OnAccount extends ParallelRace
  case object OnError extends ParallelRace
}
/**
 * Intermediate state updated with execution of each opcode in the program
 *
 * @param context the context which initiates the program
 * @param gas current gas for the execution
 * @param stack current stack
 * @param memory current memory
 * @param pc program counter - an index of the opcode in the program to be executed
 * @param returnData data to be returned from the program execution
 * @param gasRefund the amount of gas to be refunded after execution (not sure if a separate field is required)
 * @param addressesToDelete list of addresses of accounts scheduled to be deleted
 * @param halted a flag to indicate program termination
 * @param error indicates whether the program terminated abnormally
 */
final class ProgramState[W <: WorldState[W, S], S <: Storage[S]](val context: ProgramContext[W, S], val isDebugTraceEnabled: Boolean) {
  import ProgramState._

  var gas: Long = context.startGas
  var world: W = context.world
  var addressesToDelete: Set[Address] = context.initialAddressesToDelete
  var addressesTouched: Set[Address] = context.initialAddressesTouched

  var pc: Int = 0
  var returnData: ByteString = ByteString()
  var gasRefund: Long = 0
  var txLogs: Vector[TxLogEntry] = Vector()
  private var _halted: Boolean = false
  var error: Option[ProgramError] = None
  private var _isRevert: Boolean = false
  var info: Option[String] = None

  var returnDataBuffer: ByteString = ByteString()

  private var _parallelRaceConditions = Set[ParallelRace]()

  val stack: Stack = Stack.empty()
  val memory: Memory = Memory.empty()

  def config: EvmConfig = context.config
  def env: ExecEnv = context.env
  def program: Program = env.program
  def inputData: ByteString = env.inputData
  def ownAddress: Address = env.ownerAddr
  def ownBalance: UInt256 = world.getBalance(ownAddress)
  def storage: S = world.getStorage(ownAddress)
  def gasUsed = context.startGas - gas

  def withGas(gas: Long): ProgramState[W, S] = {
    this.gas = gas
    this
  }

  def withWorld(updated: W): ProgramState[W, S] = {
    this.world = updated
    this
  }

  def spendGas(amount: Long): ProgramState[W, S] = {
    this.gas -= amount
    this
  }

  def refundGas(amount: Long): ProgramState[W, S] = {
    this.gasRefund += amount
    this
  }

  def step(i: Int = 1): ProgramState[W, S] = {
    this.pc += i
    this
  }

  def goto(i: Int): ProgramState[W, S] = {
    this.pc = i
    this
  }

  def withError(error: ProgramError): ProgramState[W, S] = {
    this.error = Some(error)
    this._halted = true
    this
  }

  def withInfo(info: String): ProgramState[W, S] = {
    this.info = Some(info)
    this
  }

  def resetInfo(): ProgramState[W, S] = {
    this.info = None
    this
  }

  def withReturnData(data: ByteString): ProgramState[W, S] = {
    this.returnData = data
    this
  }

  def resetReturnDataBuffer(): ProgramState[W, S] = {
    this.returnDataBuffer = ByteString()
    this
  }

  def withReturnDataBuffer(data: ByteString): ProgramState[W, S] = {
    this.returnDataBuffer = data
    this
  }

  def withAddAddressToDelete(address: Address): ProgramState[W, S] = {
    this.addressesToDelete += address
    this
  }

  def withAddAddressesToDelete(addresses: Set[Address]): ProgramState[W, S] = {
    this.addressesToDelete ++= addresses
    this
  }

  def withAddAddressTouched(address: Address): ProgramState[W, S] = {
    this.addressesTouched += address
    this
  }

  def withAddAddressesTouched(addresses: Set[Address]): ProgramState[W, S] = {
    this.addressesTouched ++= addresses
    this
  }

  def withTxLog(log: TxLogEntry): ProgramState[W, S] = {
    this.txLogs :+= log
    this
  }

  def withTxLogs(logs: Seq[TxLogEntry]): ProgramState[W, S] = {
    this.txLogs ++= logs
    this
  }

  def parallelRaceConditions = _parallelRaceConditions
  def withParallelRaceCondition(race: ParallelRace) = {
    this._parallelRaceConditions += race
    this
  }
  def mergeParallelRaceConditions(races: Set[ParallelRace]) = {
    this._parallelRaceConditions ++= races
    this
  }

  def isHalted = _halted
  def halt(): ProgramState[W, S] = {
    this._halted = true
    this
  }

  def isRevert = _isRevert
  def revert(): ProgramState[W, S] = {
    this._isRevert = true
    this
  }
}
