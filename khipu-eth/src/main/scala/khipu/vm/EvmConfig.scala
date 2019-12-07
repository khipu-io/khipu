package khipu.vm

import akka.util.ByteString
import khipu.DataWord
import java.math.BigInteger
import khipu.config.BlockchainConfig

object EvmConfig {

  val MaxCallDepth: Int = 1024

  /** used to artificially limit memory usage by incurring maximum gas cost */
  val MaxMemory: Long = Int.MaxValue
  val MAX_MEMORY = BigInteger.valueOf(Int.MaxValue)

  /**
   * returns the evm config that should be used for given block
   */
  def forBlock(blockNumber: Long, blockchainConfig: BlockchainConfig): EvmConfig = {
    val transitionBlockToConfig = Map(
      blockchainConfig.frontierBlockNumber -> FrontierConfig,
      blockchainConfig.homesteadBlockNumber -> HomesteadConfig,
      blockchainConfig.eip150BlockNumber -> PostEIP150Config,
      blockchainConfig.eip170BlockNumber -> PostEIP170Config,
      blockchainConfig.eip161PatchBlockNumber -> EIP161PatchConfig,
      blockchainConfig.eip161PatchBlockNumber + 1 -> PostEIP161PatchConfig,
      blockchainConfig.byzantiumBlockNumber -> ByzantiumConfig,
      blockchainConfig.petersburgConfigBlockNumber -> PetersburgConfig
    )

    // highest transition block that is less/equal to `blockNumber`
    transitionBlockToConfig
      .filterKeys(_ <= blockNumber)
      .maxBy(_._1)
      ._2
  }

  val FrontierConfig = EvmConfig(
    feeSchedule = FeeSchedule.FrontierFeeSchedule,
    opCodes = OpCodes.FrontierOpCodes,
    exceptionalFailedCodeDeposit = false,
    subGasCapDivisor = None,
    chargeSelfDestructForNewAccount = false,
    maxContractSize = Int.MaxValue,
    eip161 = false,
    eip161Patch = false,
    eip140 = false,
    eip213 = false,
    eip212 = false,
    eip198 = false,
    eip658 = false,
    eip145 = false,
    eip1014 = false,
    eip1052 = false,
    eip1283 = false,
    eip152 = false,
    eip1108 = false,
    eip1344 = false,
    eip1884 = false,
    eip2028 = false,
    eip2200 = false
  )

  val HomesteadConfig = EvmConfig(
    feeSchedule = FeeSchedule.HomesteadFeeSchedule,
    opCodes = OpCodes.HomesteadOpCodes,
    exceptionalFailedCodeDeposit = true,
    subGasCapDivisor = None,
    chargeSelfDestructForNewAccount = false,
    maxContractSize = Int.MaxValue,
    eip161 = false,
    eip161Patch = false,
    eip140 = false,
    eip213 = false,
    eip212 = false,
    eip198 = false,
    eip658 = false,
    eip145 = false,
    eip1014 = false,
    eip1052 = false,
    eip1283 = false,
    eip152 = false,
    eip1108 = false,
    eip1344 = false,
    eip1884 = false,
    eip2028 = false,
    eip2200 = false
  )

  val PostEIP150Config = HomesteadConfig.copy(
    feeSchedule = FeeSchedule.PostEIP150FeeSchedule,
    subGasCapDivisor = Some(64),
    chargeSelfDestructForNewAccount = true
  )

  val PostEIP160Config = PostEIP150Config.copy(
    feeSchedule = FeeSchedule.PostEIP160FeeSchedule
  )

  val PostEIP161Config = PostEIP160Config.copy(
    eip161 = true
  )

  val PostEIP170Config = PostEIP161Config.copy(
    maxContractSize = 0x6000
  )

  val EIP161PatchConfig = PostEIP170Config.copy(
    eip161Patch = true
  )

  val PostEIP161PatchConfig = EIP161PatchConfig.copy(
    eip161Patch = false
  )

  /**
   * EIPs included in the Hard Fork:
   *     100 - Change difficulty adjustment to target mean block time including uncles</li>
   *     140 - REVERT instruction in the Ethereum Virtual Machine</li>
   *     196 - Precompiled contracts for addition and scalar multiplication on the elliptic curve alt_bn128</li>
   *     197 - Precompiled contracts for optimal Ate pairing check on the elliptic curve alt_bn128</li>
   *     198 - Precompiled contract for big int modular exponentiation</li>
   *     211 - New opcodes: RETURNDATASIZE and RETURNDATACOPY</li>
   *     214 - New opcode STATICCALL</li>
   *     658 - Embedding transaction return data in receipts</li>
   */
  val ByzantiumConfig = PostEIP161PatchConfig.copy(
    opCodes = OpCodes.ByzantiumOpCodes,
    eip140 = true,
    eip213 = true,
    eip212 = true,
    eip198 = true,
    eip658 = true
  )

  val ConstantinopleConfig = ByzantiumConfig.copy(
    opCodes = OpCodes.ConstantinopleCodes,
    eip145 = true,
    eip1014 = true,
    eip1052 = true,
    eip1283 = true
  )

  /**
   * A version of Constantinople Hard Fork after removing eip-1283.
   * <p>
   *   Unofficial name 'Petersburg', includes:
   * <ul>
   *     <li>1234 - Constantinople Difficulty Bomb Delay and Block Reward Adjustment (2 ETH)</li>
   *     <li>145  - Bitwise shifting instructions in EVM</li>
   *     <li>1014 - Skinny CREATE2</li>
   *     <li>1052 - EXTCODEHASH opcode</li>
   * </ul>
   */
  val PetersburgConfig = ConstantinopleConfig.copy(
    eip1283 = false
  )

  /**
   *
   * EIPs included in the Hard Fork: https://eips.ethereum.org/EIPS/eip-1679
   * <ul>
   * <li>EIP-152: Add Blake2 compression function F precompile</li>
   * <li>EIP-1108: Reduce alt_bn128 precompile gas costs</li>
   * <li>EIP-1344: Add ChainID opcode</li>
   * <li>EIP-1884: Repricing for trie-size-dependent opcodes</li>
   * <li>EIP-2028: Calldata gas cost reduction</li>
   * <li>EIP-2200: Rebalance net-metered SSTORE gas cost with consideration of SLOAD gas cost change</li>
   * </ul>
   */
  val IstanbulConfig = PetersburgConfig.copy(
    feeSchedule = FeeSchedule.PostEIP1108FeeSchedule,
    eip152 = true,
    eip1108 = true,
    eip1344 = true,
    eip1884 = true,
    eip2028 = true,
    eip2200 = true
  )

}

final case class EvmConfig(
    feeSchedule:                     FeeSchedule,
    opCodes:                         List[OpCode[_]],
    exceptionalFailedCodeDeposit:    Boolean,
    subGasCapDivisor:                Option[Long],
    chargeSelfDestructForNewAccount: Boolean,
    maxContractSize:                 Int,
    eip161:                          Boolean,
    eip161Patch:                     Boolean,
    eip140:                          Boolean,
    eip213:                          Boolean, // replaced eip196
    eip212:                          Boolean, // replaced eip197
    eip198:                          Boolean,
    eip658:                          Boolean,
    eip145:                          Boolean,
    eip1014:                         Boolean,
    eip1052:                         Boolean,
    eip1283:                         Boolean,
    eip152:                          Boolean,
    eip1108:                         Boolean,
    eip1344:                         Boolean,
    eip1884:                         Boolean,
    eip2028:                         Boolean,
    eip2200:                         Boolean
) {
  import EvmConfig._

  private val byteToOpCode = {
    val ops = Array.ofDim[OpCode[_]](256)
    opCodes foreach { op =>
      ops(op.code.toInt & 0xFF) = op
      op.code.hashCode
    }
    ops
  }

  def getOpCode(code: Byte) = {
    Option(byteToOpCode(code.toInt & 0xFF))
  }

  /**
   * Calculate gas cost of memory usage. Incur a blocking gas cost if memory usage exceeds reasonable limits.
   *
   * For each word (32 bytes), there’s a cost associated for the expansion of the memory.
   * The EVM uses a Quadratic-memory gas calculation formula:
   *  (memSizeWords ^ 2) / QuadCoefficient + (MemWords * MemGas)
   * since we’ve already capped the gas counter, the memory fee associated with
   * the expansion could never go beyond 2⁶⁴-1.
   *
   * @param memSize  current memory size in bytes
   * @param offset   memory offset to be written/read
   * @param dataSize size of data to be written/read in bytes
   * @return gas cost
   */
  def calcMemCost(memSize: Long, offset: Long, dataSize: Long): Long = {
    val memNeeded = if (dataSize == 0) 0 else offset + dataSize
    if (memNeeded > MaxMemory) {
      // Use an big value here to cause OOG, but this returned value may be added outside,
      // so should not be too big to avoid the added value exceeding Long.MaxValue, 
      // and become a negative value (long is signed number type)
      Long.MaxValue / 2
    } else if (memNeeded <= memSize) {
      0
    } else {
      c(memNeeded) - c(memSize)
    }
  }

  /** See YP H.1 (222) */
  private def c(m: Long): Long = {
    val a = DataWord.wordsForBytes(m)
    feeSchedule.G_memory * a + a * a / 512
  }

  /**
   * Calculates transaction intrinsic gas. See YP section 6.2
   *
   */
  def calcTransactionIntrinsicGas(txData: ByteString, isContractCreation: Boolean): Long = {
    var txDataZero = 0
    var i = 0
    while (i < txData.length) {
      if (txData(i) == 0) {
        txDataZero += 1
      }
      i += 1
    }

    val txDataNonZero = txData.length - txDataZero

    txDataZero * feeSchedule.G_txdatazero +
      txDataNonZero * feeSchedule.G_txdatanonzero +
      (if (isContractCreation) feeSchedule.G_txcreate else 0) +
      feeSchedule.G_transaction
  }

  /**
   * If the initialization code completes successfully, a final contract-creation cost is paid, the code-deposit cost,
   * proportional to the size of the created contract’s code. See YP equation (96)
   *
   * @param executionResultData Transaction code initialization result
   * @return Calculated gas cost
   */
  def calcCodeDepositCost(executionResultData: ByteString): Long =
    feeSchedule.G_codedeposit * executionResultData.size

  /**
   * a helper method used for gas adjustment in CALL and CREATE opcode, see YP eq. (224)
   */
  def gasCap(g: Long): Long =
    subGasCapDivisor.map(divisor => g - g / divisor).getOrElse(g)
}

object FeeSchedule {
  object FrontierFeeSchedule extends FrontierFeeSchedule
  class FrontierFeeSchedule extends FeeSchedule {
    override val G_zero = 0
    override val G_base = 2
    override val G_verylow = 3
    override val G_low = 5
    override val G_mid = 8
    override val G_high = 10
    override val G_balance = 20
    override val G_sload = 50
    override val G_jumpdest = 1
    override val G_sset = 20000
    override val G_sreset = 5000
    override val G_sreuse = 200
    override val R_sclear = 15000
    override val R_selfdestruct = 24000
    override val G_selfdestruct = 0
    override val G_create = 32000
    override val G_codedeposit = 200
    override val G_call = 40
    override val G_callvalue = 9000
    override val G_callstipend = 2300
    override val G_newaccount = 25000
    override val G_exp = 10
    override val G_expbyte = 10
    override val G_memory = 3
    override val G_txcreate = 0
    override val G_txdatazero = 4
    override val G_txdatanonzero = 68
    override val G_transaction = 21000
    override val G_log = 375
    override val G_logdata = 8
    override val G_logtopic = 375
    override val G_sha3 = 30
    override val G_sha3word = 6
    override val G_copy = 3
    override val G_blockhash = 20
    override val G_extcodesize = 20
    override val G_extcodecopy = 20
    override val G_extcodehash = 400
    override val G_bn128add = 500
    override val G_bn128mul = 40000
    override val G_bn128pairing_base = 100000
    override val G_bn128pairing_pairing = 80000
  }

  object HomesteadFeeSchedule extends HomesteadFeeSchedule
  class HomesteadFeeSchedule extends FrontierFeeSchedule {
    override val G_txcreate = 32000
  }

  object PostEIP150FeeSchedule extends PostEIP150FeeSchedule
  class PostEIP150FeeSchedule extends HomesteadFeeSchedule {
    override val G_sload = 200
    override val G_call = 700
    override val G_balance = 400
    override val G_selfdestruct = 5000
    override val G_extcodesize = 700
    override val G_extcodecopy = 700
  }

  object PostEIP160FeeSchedule extends PostEIP160FeeSchedule
  class PostEIP160FeeSchedule extends PostEIP150FeeSchedule {
    override val G_expbyte = 50
  }

  object PostEIP1108FeeSchedule extends PostEIP1108FeeSchedule
  class PostEIP1108FeeSchedule extends PostEIP160FeeSchedule {
    override val G_bn128add = 150
    override val G_bn128mul = 6000
    override val G_bn128pairing_base = 45000
    override val G_bn128pairing_pairing = 34000
  }
}
trait FeeSchedule {
  def G_zero: Long
  def G_base: Long
  def G_verylow: Long
  def G_low: Long
  def G_mid: Long
  def G_high: Long
  def G_balance: Long
  def G_sload: Long
  def G_jumpdest: Long
  def G_sset: Long
  def G_sreset: Long
  def G_sreuse: Long
  def R_sclear: Long
  def R_selfdestruct: Long
  def G_selfdestruct: Long
  def G_create: Long
  def G_codedeposit: Long
  def G_call: Long
  def G_callvalue: Long
  def G_callstipend: Long
  def G_newaccount: Long
  def G_exp: Long
  def G_expbyte: Long
  def G_memory: Long
  def G_txcreate: Long
  def G_txdatazero: Long
  def G_txdatanonzero: Long
  def G_transaction: Long
  def G_log: Long
  def G_logdata: Long
  def G_logtopic: Long
  def G_sha3: Long
  def G_sha3word: Long
  def G_copy: Long
  def G_blockhash: Long
  def G_extcodesize: Long
  def G_extcodecopy: Long
  def G_extcodehash: Long
  def G_bn128add: Long
  def G_bn128mul: Long
  def G_bn128pairing_base: Long
  def G_bn128pairing_pairing: Long
}
