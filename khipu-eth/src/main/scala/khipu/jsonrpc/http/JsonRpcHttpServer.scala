package khipu.jsonrpc.http

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ MalformedRequestContentRejection, RejectionHandler }
import akka.http.scaladsl.server.Directives._
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import khipu.jsonrpc.http.JsonRpcHttpServer.JsonRpcHttpServerConfig
import khipu.jsonrpc.{ JsonRpcController, JsonRpcErrors, JsonRpcRequest, JsonRpcResponse }
import org.json4s.JsonAST.JInt
import org.json4s.{ DefaultFormats, native }
import scala.util.{ Failure, Success }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._

object JsonRpcHttpServer {

  trait JsonRpcHttpServerConfig {
    val enabled: Boolean
    val interface: String
    val port: Int
  }

}
// curl localhost:8546 -H 'Content-Type: application/json' -X POST --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[5000000, true],"id":1}' 
class JsonRpcHttpServer(jsonRpcController: JsonRpcController, config: JsonRpcHttpServerConfig)(implicit system: ActorSystem) extends Json4sSupport {
  private val log = Logging(system, this.getClass)

  implicit val serialization = native.Serialization

  implicit val formats = DefaultFormats

  val corsSettings = CorsSettings.defaultSettings.copy(allowGenericHttpRequests = true)

  implicit def myRejectionHandler: RejectionHandler =
    RejectionHandler.newBuilder().handle {
      case _: MalformedRequestContentRejection =>
        complete((StatusCodes.BadRequest, JsonRpcResponse("2.0", None, Some(JsonRpcErrors.ParseError), JInt(0))))
    }.result()

  val route = cors(corsSettings) {
    (pathEndOrSingleSlash & post) {
      entity(as[JsonRpcRequest]) { request =>
        handleRequest(request)
      } ~ entity(as[Seq[JsonRpcRequest]]) { request =>
        handleBatchRequest(request)
      }
    }
  }

  def run() {

    val bindingResultF = Http(system).bindAndHandle(route, config.interface, config.port)

    bindingResultF onComplete {
      case Success(serverBinding) => log.debug(s"JSON RPC server listening on ${serverBinding.localAddress}")
      case Failure(ex)            => log.error("Cannot start JSON RPC server", ex)
    }
  }

  private def handleRequest(request: JsonRpcRequest) = {
    complete(jsonRpcController.handleRequest(request))
  }

  private def handleBatchRequest(requests: Seq[JsonRpcRequest]) = {
    complete(Future.sequence(requests.map(request => jsonRpcController.handleRequest(request))))
  }

}

