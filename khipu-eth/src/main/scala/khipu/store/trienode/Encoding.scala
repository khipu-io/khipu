package khipu.store.trienode

import khipu.Hash
import khipu.rlp
import khipu.rlp.RLPImplicitConversions.{ toRlpList, _ }
import khipu.rlp.RLPImplicits._
import scala.collection.mutable

object Encoding {

  private[store] def storedNodeFromBytes(encoded: Array[Byte]): StoredNode = rlp.decode(encoded)(storedNodeEncDec)
  private[store] def pruneCandidatesFromBytes(array: Array[Byte]): PruneCandidates = rlp.decode(array)(pruneCandidatesEncDec)
  private[store] def storedNodeToBytes(storedNode: StoredNode): Array[Byte] = storedNodeEncDec.encode(storedNode)
  private[store] def pruneCandiatesToBytes(pruneCandidates: PruneCandidates): Array[Byte] = pruneCandidatesEncDec.encode(pruneCandidates)

  private val storedNodeEncDec = new rlp.RLPDecoder[StoredNode] with rlp.RLPEncoder[StoredNode] {
    override def decode(rlpEncodeable: rlp.RLPEncodeable): StoredNode = rlpEncodeable match {
      case rlp.RLPList(nodeEncoded, references) => StoredNode(nodeEncoded, references)
      case _                                    => throw new RuntimeException("Error when decoding stored node")
    }

    override def encode(obj: StoredNode): rlp.RLPEncodeable = rlp.encode(rlp.RLPList(obj.nodeEncoded, obj.references))
  }

  private val pruneCandidatesEncDec = new rlp.RLPEncoder[PruneCandidates] with rlp.RLPDecoder[PruneCandidates] {
    override def encode(obj: PruneCandidates): rlp.RLPEncodeable = rlp.encode(toRlpList(obj.nodeKeys.toSeq))

    override def decode(rlpEncodeable: rlp.RLPEncodeable): PruneCandidates = rlpEncodeable match {
      case rlp.RLPList(candidates @ _*) => PruneCandidates(candidates.map(b => b: Hash).toSet)
      case _                            => throw new RuntimeException("Error when decoding pruning candidate")
    }
  }
}
