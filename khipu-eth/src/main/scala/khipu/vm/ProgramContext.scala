package khipu.vm

import akka.util.ByteString
import khipu.domain.Address
import khipu.domain.BlockHeader
import khipu.domain.SignedTransaction

object ProgramContext {
  def apply[W <: WorldState[W, S], S <: Storage[S]](
    stx:                      SignedTransaction,
    recipientAddress:         Address,
    program:                  Program,
    input:                    ByteString,
    blockHeader:              BlockHeader,
    world:                    W,
    config:                   EvmConfig,
    initialAddressesToDelete: Set[Address],
    initialAddressesTouched:  Set[Address],
    isStaticCall:             Boolean
  ): ProgramContext[W, S] = {

    val env = ExecEnv(
      recipientAddress,
      stx.sender,
      stx.sender,
      stx.tx.gasPrice,
      stx.tx.value,
      program,
      input,
      blockHeader,
      callDepth = 0
    )

    val startGas = stx.tx.gasLimit - config.calcTransactionIntrinsicGas(stx.tx.payload, stx.tx.isContractCreation)

    ProgramContext(env, recipientAddress, startGas, world, config, initialAddressesToDelete, initialAddressesTouched, isStaticCall)
  }
}

/**
 * Input parameters to a program executed on the EVM. Apart from the code itself
 * it should have all (interfaces to) the data accessible from the EVM.
 *
 * @param env set of constants for the execution
 * @param targetAddress used for determining whether a precompiled contract is being called (potentially
 *                      different from the addresses defined in env)
 * @param startGas initial gas for the execution
 * @param world provides interactions with world state
 * @param config evm config
 * @param initialAddressesToDelete contains initial set of addresses to delete (from lower depth calls)
 */
final case class ProgramContext[W <: WorldState[W, S], S <: Storage[S]](
  env:                      ExecEnv,
  targetAddress:            Address,
  startGas:                 Long,
  world:                    W,
  config:                   EvmConfig,
  initialAddressesToDelete: Set[Address],
  initialAddressesTouched:  Set[Address],
  isStaticCall:             Boolean
)
