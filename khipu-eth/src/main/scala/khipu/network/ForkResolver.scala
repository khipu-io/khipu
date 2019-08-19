package khipu.network

import khipu.config.BlockchainConfig
import khipu.config.KhipuConfig
import khipu.domain.BlockHeader

object ForkResolver {
  val isEth = KhipuConfig.chainType match {
    case "eth" => true
    case _     => false
  }

  trait Fork
  case object Etc extends Fork
  case object Eth extends Fork

  final class DAOForkResolver(blockchainConfig: BlockchainConfig) extends ForkResolver {
    val forkBlockNumber = blockchainConfig.daoForkBlockNumber

    def recognizeFork(blockHeader: BlockHeader): Fork = {
      if (blockHeader.hash == blockchainConfig.daoForkBlockHash) Etc else Eth
    }

    def isAccepted(fork: Fork): Boolean = if (isEth) fork == Eth else fork == Etc
  }
}

trait ForkResolver {
  def forkBlockNumber: Long
  def recognizeFork(blockHeader: BlockHeader): ForkResolver.Fork
  def isAccepted(fork: ForkResolver.Fork): Boolean
}
