package khipu.consensus.ibft.messagedata

/** Message codes for iBFT v2 messages */
object IbftV2 {
  val PROPOSAL = 0;
  val PREPARE = 1;
  val COMMIT = 2;
  val ROUND_CHANGE = 3;
  val MESSAGE_SPACE = 4;
}
