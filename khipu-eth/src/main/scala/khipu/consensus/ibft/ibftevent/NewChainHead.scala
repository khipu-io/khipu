package khipu.consensus.ibft.ibftevent

import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Objects;

import com.google.common.base.MoreObjects;

/**
 * Event indicating that new chain head has been received
 * @param newChainHeadHeader The header of the current blockchain head
 */
final case class NewChainHead(newChainHeadHeader: BlockHeader) extends IbftEvent {

  override def getType(): IbftEvents.Type = {
    return IbftEvents.Type.NEW_CHAIN_HEAD
  }
}
