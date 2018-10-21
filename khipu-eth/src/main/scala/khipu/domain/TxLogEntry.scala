package khipu.domain

import akka.util.ByteString

final case class TxLogEntry(loggerAddress: Address, logTopics: List[ByteString], data: ByteString) {
  override def toString: String = {
    s"TxLogEntry(loggerAddress: $loggerAddress, logTopics: ${logTopics.map(e => khipu.toHexString(e))}, data: ${khipu.toHexString(data)})"
  }
}
