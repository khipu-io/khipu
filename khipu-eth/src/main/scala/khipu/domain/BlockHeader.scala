package khipu.domain

import akka.util.ByteString
import khipu.Hash
import khipu.crypto
import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
import khipu.rlp
import khipu.rlp.RLPList
import khipu.vm.UInt256

object BlockHeader {
  // 0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347
  private val EmptyOmmersHash = crypto.kec256(rlp.EmptyRLPList)

  def getEncodedWithoutNonce(blockHeader: BlockHeader): Array[Byte] = {
    val rlpEncoded = blockHeader.toRLPEncodable match {
      case rlpList: RLPList => RLPList(rlpList.items.dropRight(2): _*)
      case _                => throw new Exception("BlockHeader cannot be encoded without nonce and mixHash")
    }
    rlp.encode(rlpEncoded)
  }
}
final case class BlockHeader(
    parentHash:       Hash, // The SHA3 256-bit hash of the parent block, in its entirety
    ommersHash:       Hash, // The SHA3 256-bit hash of the uncles list portion of this block
    beneficiary:      ByteString, // The 160-bit address to which all fees collected from the successful mining of this block be transferred; formally
    stateRoot:        Hash, // The SHA3 256-bit hash of the root node of the state trie, after all transactions are executed and finalisations applied
    transactionsRoot: Hash, // The SHA3 256-bit hash of the root node of the trie structure populated with each transaction in the transaction list portion, the trie is populate by [key, val] --> [rlp(index), rlp(tx_recipe)] of the block
    receiptsRoot:     Hash, // The SHA3 256-bit hash of the root node of the trie structure populated with each transaction recipe in the transaction recipes list portion, the trie is populate by [key, val] --> [rlp(index), rlp(tx_recipe)] of the block
    logsBloom:        ByteString,
    difficulty:       UInt256, // A scalar value corresponding to the difficulty level of this block. This can be calculated from the previous block’s difficulty level and the timestamp
    number:           Long, // A scalar value equal to the number of ancestor blocks. The genesis block has a number of zero 
    gasLimit:         Long, // A scalar value equal to the current limit of gas expenditure per block
    gasUsed:          Long, // A scalar value equal to the total gas used in transactions in this bloc
    unixTimestamp:    Long, // A scalar value equal to the reasonable output of Unix's time() at this block's inception
    extraData:        ByteString, // An arbitrary byte array containing data relevant to this block. With the exception of the genesis block, this must be 32 bytes or fewer
    mixHash:          Hash,
    nonce:            ByteString // A 256-bit hash which proves that a sufficient amount of computation has been carried out on this block
) {

  /**
   * calculates blockHash for given block header
   * @return - hash that can be used to get block bodies / receipts
   */
  lazy val hash = Hash(crypto.kec256(this.toBytes))
  lazy val hashAsHexString = hash.hexString

  def nonUncles = java.util.Arrays.equals(ommersHash.bytes, BlockHeader.EmptyOmmersHash)
  def hasUncles = !nonUncles

  override def toString: String = {
    s"""BlockHeader {
         |parentHash: ${parentHash.hexString}
         |ommersHash: ${ommersHash.hexString}
         |beneficiary: ${khipu.toHexString(beneficiary)}
         |stateRoot: ${stateRoot.hexString}
         |transactionsRoot: ${transactionsRoot.hexString}
         |receiptsRoot: ${receiptsRoot.hexString}
         |logsBloom: ${khipu.toHexString(logsBloom)}
         |difficulty: $difficulty,
         |number: $number,
         |gasLimit: $gasLimit,
         |gasUsed: $gasUsed,
         |unixTimestamp: $unixTimestamp,
         |extraData: ${khipu.toHexString(extraData)}
         |mixHash: ${mixHash.hexString}
         |nonce: ${khipu.toHexString(nonce)}
         |}""".stripMargin
  }

}

