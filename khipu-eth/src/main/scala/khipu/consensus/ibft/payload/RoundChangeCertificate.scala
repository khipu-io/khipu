/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package khipu.consensus.ibft.payload

import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

object RoundChangeCertificate {
  def readFrom(rlpInput: RLPInput ):RoundChangeCertificate =  {
    final List<SignedData<RoundChangePayload>> roundChangePayloads;

    rlpInput.enterList();
    roundChangePayloads = rlpInput.readList(SignedData::readSignedRoundChangePayloadFrom);
    rlpInput.leaveList();

    RoundChangeCertificate(roundChangePayloads);
  }

  class Builder {

    private val roundChangePayloads: List[RoundChange]  = Lists.newArrayList[RoundChange]();


    def appendRoundChangeMessage(msg: RoundChange ) {
      roundChangePayloads.add(msg);
    }

    def buildCertificate(): RoundChangeCertificate =  {
      RoundChangeCertificate(
          roundChangePayloads.stream()
              .map(RoundChange::getSignedPayload)
              .collect(Collectors.toList()));
    }
  }



}
final case class RoundChangeCertificate(roundChangePayloads: List[SignedData[RoundChangePayload]] ) {

  def writeTo(rlpOutput: RLPOutput ) {
    rlpOutput.startList();
    rlpOutput.writeList(roundChangePayloads, SignedData::writeTo);
    rlpOutput.endList();
  }
}
