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
  def run[W <: WorldState[W, S], S <: Storage[S]](context: ProgramContext[W, S]): ProgramResult[W, S] = {
    // new init state is created for each run(context)
    val initState = new ProgramState[W, S](context)
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
      postState.parallelRaceConditions,
      postState.trace
    )
  }

  @tailrec
  private def run[W <: WorldState[W, S], S <: Storage[S]](state: ProgramState[W, S]): ProgramState[W, S] = {
    val byte = state.program.getByte(state.pc)
    state.config.byteToOpCode.get(byte) match {
      case Some(opcode) =>
        if (state.isTraceEnabled) {
          state.addTrace(s"[trace] $opcode | pc: ${state.pc} | depth: ${state.env.callDepth} | gas: ${state.gas} | ${state.stack} | ${state.memory} | error: ${state.error}")
        }
        val newState = opcode.execute(state) // may reentry VM.run(context) by CREATE/CALL op

        if (newState.isHalted) {
          if (state.isTraceEnabled) {
            state.addTrace(s"[trace] halt | pc: ${newState.pc} | depth: ${newState.env.callDepth} | gas: ${newState.gas} | ${newState.stack} | ${newState.memory} | error: ${newState.error}")
          }
          newState
        } else {
          run[W, S](newState)
        }

      case None =>
        if (state.isTraceEnabled) {
          state.addTrace(s"[trace] ${InvalidOpCode(byte)} | pc: ${state.pc} | depth: ${state.env.callDepth} | gas: ${state.gas} | ${state.stack} | error: ${state.error}")
        }
        state.withError(InvalidOpCode(byte)).halt()
    }
  }
}

