package khipu.blockchain.sync

import akka.util.ByteString
import khipu.domain.Blockchain
import khipu.network.p2p.Message
import khipu.network.p2p.MessageSerializable
import khipu.network.p2p.messages.PV62
import khipu.network.p2p.messages.PV63
import khipu.network.rlpx.PeerConfiguration
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * BlockchainHost actor is in charge of replying to the peer's requests for blockchain data, which includes both
 * node and block data.
 *
 * Note blockchain should be read only in this actor
 */
class HostService(blockchainReadOnly: Blockchain, peerConfig: PeerConfiguration)(implicit ec: ExecutionContext) {

  private var _isShuttingDown = false
  private def isShuttingDown = _isShuttingDown
  def shutdown() {
    _isShuttingDown = true
  }

  def ask(message: Message) = Future {
    if (isShuttingDown) {
      None
    } else {
      message match {
        case PV62.GetBlockHeaders(block, maxHeaders, skip, reverse) =>
          //log.info("Got request GetBlockHeaders")

          val blockNumber = block.fold(a => Some(a), b => blockchainReadOnly.getBlockHeaderByHash(b).map(_.number))

          // force Message to MessageSerializable
          val resp: Option[MessageSerializable] = blockNumber match {
            case Some(startBlockNumber) if startBlockNumber >= 0 && maxHeaders >= 0 && skip >= 0 =>
              try {
                val headersCount = maxHeaders min peerConfig.fastSyncHostConfiguration.maxBlocksHeadersPerMessage

                val range = if (reverse) {
                  startBlockNumber to (startBlockNumber - (skip + 1) * headersCount + 1) by -(skip + 1)
                } else {
                  startBlockNumber to (startBlockNumber + (skip + 1) * headersCount - 1) by (skip + 1)
                }

                val blockHeaders = range.flatMap { num =>
                  blockchainReadOnly.getBlockHeaderByNumber(num)
                }

                Some(PV62.BlockHeaders(blockHeaders))
              } catch {
                case ex: Throwable =>
                  //log.error(s"$ex")
                  None
              }

            case _ =>
              //log.warning("got request for block headers with invalid block hash/number: {}", request)
              None
          }

          resp

        case PV62.GetBlockBodies(hashes) =>
          //log.info("Got request GetBlockBodies")

          try {
            val blockBodies = hashes.take(peerConfig.fastSyncHostConfiguration.maxBlocksBodiesPerMessage).flatMap { hash =>
              blockchainReadOnly.getBlockBodyByHash(hash)
            }

            // force Message to MessageSerializable
            val resp: Option[MessageSerializable] = Some(PV62.BlockBodies(blockBodies))

            resp
          } catch {
            case ex: Throwable =>
              //log.error(s"$ex")
              None
          }

        case PV63.GetReceipts(blockHashes) =>
          //log.info("Got request GetReceipts")

          try {
            val receipts = blockHashes.take(peerConfig.fastSyncHostConfiguration.maxReceiptsPerMessage).flatMap { hash =>
              blockchainReadOnly.getReceiptsByHash(hash)
            }

            // force Message to MessageSerializable
            val resp: Option[MessageSerializable] = Some(PV63.Receipts(receipts))

            resp
          } catch {
            case ex: Throwable =>
              //log.error(s"$ex")
              None
          }

        case PV63.GetNodeData(mptElementsHashes) =>
          //log.info("Got request GetNodeData")

          try {
            val hashesRequested = mptElementsHashes.take(peerConfig.fastSyncHostConfiguration.maxMptComponentsPerMessage)

            val nodeData: Seq[ByteString] = hashesRequested.flatMap { hash =>
              // fetch mpt node by hash, if no mpt node was found, fetch evm by hash
              blockchainReadOnly.getMptNodeByHash(hash).map(x => ByteString(x.toBytes)) orElse blockchainReadOnly.getEvmcodeByHash(hash)
            }

            // force Message to MessageSerializable
            val resp: Option[MessageSerializable] = Some(PV63.NodeData(nodeData))

            resp
          } catch {
            case ex: Throwable =>
              //log.error(s"$ex")
              None
          }

        case _ => None
      }
    }
  }
}
