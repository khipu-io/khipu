package khipu.vm

import scala.annotation.tailrec

/**
 * Entry point to executing a program.
 */
object VM {
  /**
   * Executes a program
   * @param context context to be executed
   * @return result of the execution
   */
  def run[W <: WorldState[W, S], S <: Storage[S]](context: ProgramContext[W, S], isDebugTraceEnabled: Boolean): ProgramResult[W, S] = {
    // new init state is created for each run(context)
    val initState = new ProgramState[W, S](context, isDebugTraceEnabled)
    val postState = run(initState)

    ProgramResult[W, S](
      postState.returnData,
      postState.gas,
      postState.world,
      postState.txLogs,
      postState.gasRefund,
      postState.addressesToDelete,
      postState.addressesTouched,
      postState.error,
      postState.isRevert,
      postState.parallelRaceConditions
    )
  }

  // TODO write debug trace to a file
  @tailrec
  private def run[W <: WorldState[W, S], S <: Storage[S]](state: ProgramState[W, S]): ProgramState[W, S] = {
    val byte = state.program.getByte(state.pc)
    state.resetInfo()
    state.config.getOpCode(byte) match {
      case Some(opcode) =>
        if (state.isDebugTraceEnabled) {
          println(s"[trace] $opcode | pc: ${state.pc} | depth: ${state.env.callDepth} | gas: ${state.gas} | ${state.stack} | ${state.memory} | error: ${state.error} | info: ${state.info}")
        }
        val newState = opcode.execute(state) // may reentry VM.run(context) by CREATE/CALL op

        if (newState.isHalted) {
          if (state.isDebugTraceEnabled) {
            println(s"[trace] halt | pc: ${newState.pc} | depth: ${newState.env.callDepth} | gas: ${newState.gas} | ${newState.stack} | ${newState.memory} | error: ${newState.error} | info: ${newState.info}")
          }
          newState
        } else {
          run[W, S](newState)
        }

      case None =>
        if (state.isDebugTraceEnabled) {
          println(s"[trace] ${InvalidOpCode(byte)} | pc: ${state.pc} | depth: ${state.env.callDepth} | gas: ${state.gas} | ${state.stack} | error: ${state.error} | info: ${state.info}")
        }
        state.withError(InvalidOpCode(byte)).halt()
    }
  }
}

