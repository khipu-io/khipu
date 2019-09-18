package khipu.domain

import akka.util.ByteString
import java.util.Arrays
import khipu.Hash
import khipu.DataWord
import khipu.crypto
import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
import khipu.rlp
import khipu.rlp.RLPList
import org.spongycastle.util.BigIntegers

object BlockHeader {
  // 0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347
  private val EmptyOmmersHash = crypto.kec256(rlp.EmptyRLPList)
}
final case class BlockHeader(
    parentHash:       Hash, // The SHA3 256-bit hash of the parent block, in its entirety
    ommersHash:       Hash, // The SHA3 256-bit hash of the uncles list portion of this block
    beneficiary:      ByteString, // The 160-bit address to which all fees collected from the successful mining of this block be transferred; formally
    stateRoot:        Hash, // The SHA3 256-bit hash of the root node of the state trie, after all transactions are executed and finalisations applied
    transactionsRoot: Hash, // The SHA3 256-bit hash of the root node of the trie structure populated with each transaction in the transaction list portion, the trie is populate by [key, val] --> [rlp(index), rlp(tx_recipe)] of the block
    receiptsRoot:     Hash, // The SHA3 256-bit hash of the root node of the trie structure populated with each transaction recipe in the transaction recipes list portion, the trie is populate by [key, val] --> [rlp(index), rlp(tx_recipe)] of the block
    logsBloom:        ByteString,
    difficulty:       DataWord, // A scalar value corresponding to the difficulty level of this block. This can be calculated from the previous block’s difficulty level and the timestamp
    number:           Long, // A scalar value equal to the number of ancestor blocks. The genesis block has a number of zero 
    gasLimit:         Long, // A scalar value equal to the current limit of gas expenditure per block
    gasUsed:          Long, // A scalar value equal to the total gas used in transactions in this bloc
    unixTimestamp:    Long, // A scalar value equal to the reasonable output of Unix's time() at this block's inception
    extraData:        ByteString, // An arbitrary byte array containing data relevant to this block. With the exception of the genesis block, this must be 32 bytes or fewer
    mixHash:          Hash,
    nonce:            ByteString // A 256-bit hash which proves that a sufficient amount of computation has been carried out on this block
) extends Ordered[BlockHeader] {

  /**
   * calculates blockHash for given block header
   * @return - hash that can be used to get block bodies / receipts
   */
  lazy val hash = Hash(crypto.kec256(this.toBytes))
  lazy val hashAsHexString = hash.hexString

  def nonUncles = Arrays.equals(ommersHash.bytes, BlockHeader.EmptyOmmersHash)
  def hasUncles = !nonUncles

  def getPowBoundary: Array[Byte] = BigIntegers.asUnsignedByteArray(32, DataWord.MODULUS.divide(difficulty.n))

  def getEncodedWithoutNonce: Array[Byte] = {
    val rlpEncoded = this.toRLPEncodable match {
      case rlpList: RLPList => RLPList(rlpList.items.dropRight(2): _*)
      case x                => throw new Exception(s"BlockHeader cannot be encoded: $x")
    }
    rlp.encode(rlpEncoded)
  }

  def calculatePoWValue: Array[Byte] = {
    val nonceReverted = nonce.reverse
    val hashBlockWithoutNonce = crypto.kec256(getEncodedWithoutNonce)
    val seedHash = crypto.kec512(hashBlockWithoutNonce ++ nonceReverted)

    crypto.kec256(seedHash ++ mixHash.bytes)
  }

  def compare(that: BlockHeader) = {
    if (number < that.number) {
      -1
    } else if (number == that.number) {
      0
    } else {
      1
    }
  }

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

