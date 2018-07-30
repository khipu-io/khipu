package khipu.jsonrpc

import akka.util.ByteString
import khipu.crypto
import khipu.util.Config
import scala.concurrent.Future

object Web3Service {
  final case class Sha3Request(data: ByteString)
  final case class Sha3Response(data: ByteString)

  final case class ClientVersionRequest()
  final case class ClientVersionResponse(value: String)
}

class Web3Service {
  import Web3Service._

  def sha3(req: Sha3Request): ServiceResponse[Sha3Response] = {
    Future.successful(Right(Sha3Response(ByteString(crypto.kec256(req.data)))))
  }

  def clientVersion(req: ClientVersionRequest): ServiceResponse[ClientVersionResponse] = {
    Future.successful(Right(ClientVersionResponse(Config.clientVersion)))
  }

}
